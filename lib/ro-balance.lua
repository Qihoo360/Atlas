--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2009, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ --]]

---
-- slave aware load-balancer
--
-- * take slaves which are too far behind out of the rotation
-- * when we bring back the slave make sure it doesn't get 
--   behind again too early (slow start)
-- * fallback to the master of all slaves are too far behind
-- * check if the backends are really configures as slave
-- * check if the io-thread is running at all
--
-- it is unclear how to 
-- - handle requests sent to a backend with a io-thread off
-- - should we just close the connection to the client
--   and let it retry ?
-- - how to activate the backend again ?
-- - if a slave is lagging, we don't send queries to it
--   how do we figure out it is back again ?

require("proxy.auto-config")

local commands    = require("proxy.commands")

if not proxy.global.config.ro_balance then
	proxy.global.config.ro_balance = { 
		max_seconds_lag = 10,      -- 10 seconds
		max_bytes_lag = 10 * 1024, -- 10k
		check_timeout = 2,
		is_debug = true
	}
end

if not proxy.global.lag then
	proxy.global.lag = { }
end

local config      = proxy.global.config.ro_balance
local backend_lag = proxy.global.lag

---
-- pick a backend server
--
-- we take the lag into account
-- * ignore backends which aren't configured
-- * ignore backends which don't have a io-thread running
-- * ignore backends that are too far behind
function connect_server()
	local fallback_ndx
	local slave_ndx 
	local unknown_slave_ndx 
	local slave_bytes_lag 

	for b_ndx = 1, #proxy.global.backends do
		local backend = proxy.global.backends[b_ndx]

		if backend.state ~= proxy.BACKEND_STATE_DOWN then
			if backend.type == proxy.BACKEND_TYPE_RW then
				-- the fallback
				fallback_ndx = b_ndx
			else
				--- 
				-- connect to the backends first we don't know yet
				if not backend_lag[backend.dst.name] then
					unknown_slave_ndx = b_ndx
				elseif backend_lag[backend.dst.name].state == "running" then
					if not slave_bytes_lag then
						slave_ndx = b_ndx
						slave_bytes_lag = backend_lag[backend.dst.name].slave_bytes_lag
					elseif backend_lag[backend.dst.name].slave_bytes_lag < slave_bytes_lag then
						slave_ndx = b_ndx
						slave_bytes_lag = backend_lag[backend.dst.name].slave_bytes_lag
					end
				end
			end
		end
	end

	proxy.connection.backend_ndx = unknown_slave_ndx or slave_ndx or fallback_ndx

	if config.is_debug then
		print("(connect-server) using backend: " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name)
	end
end

function read_query(packet)
	local cmd      = commands.parse(packet)
	local ret      = proxy.global.config:handle(cmd)
	if ret then return ret end

	--
	-- check the backend periodicly for its lag
	--
	
	-- translate the backend_ndx into its address
	local backend_addr = proxy.global.backends[proxy.connection.backend_ndx].dst.name
	backend_lag[backend_addr] = backend_lag[backend_addr] or {
		state = "unchecked"
	}

	local now = os.time()

	if config.is_debug then
		print("(read_query) we are on backend: ".. backend_addr .. 
			" in state: " .. backend_lag[backend_addr].state)
	end

	if backend_lag[backend_addr].state == "unchecked" or 
	   (backend_lag[backend_addr].state == "running" and 
	    now - backend_lag[backend_addr].check_ts > config.check_timeout) then
		if config.is_debug then
			print("(read-query) unchecked, injecting a SHOW SLAVE STATUS")
		end
		proxy.queries:append(2, string.char(proxy.COM_QUERY) .. "SHOW SLAVE STATUS")
		proxy.queries:append(1, packet)

		return proxy.PROXY_SEND_QUERY
	elseif proxy.global.backends[proxy.connection.backend_ndx].type == proxy.BACKEND_TYPE_RW or  -- master
	       backend_lag[backend_addr].state == "running" then -- good slave
		-- pass through
		return
	else
		-- looks like this is a bad backend
		-- let's get the client to connect to another backend
		--
		-- ... by closing the connection
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "slave-state is " .. backend_lag[backend_addr].state,
			sqlstate = "08S01"
		}

		proxy.queries:reset()

		-- and now we have to tell the proxy to close the connection
		--
		proxy.connection.connection_close = true

		return proxy.PROXY_SEND_RESULT
	end
end

function read_query_result(inj)
	-- pass through the client query
	if inj.id == 1 then
		return 
	end

	---
	-- parse the SHOW SLAVE STATUS
	--
	-- what can happen ?
	-- * no permissions (ERR)
	-- * if not slave, result is empty
	local res = inj.resultset
	local fields = res.fields
	local show_slave_status = {}

	-- turn the resultset into local hash
	for row in res.rows do
		for field_id, field in pairs(row) do
			show_slave_status[fields[field_id].name] = tostring(field)
			if config.is_debug then
				print(("[%d] '%s' = '%s'"):format(field_id, fields[field_id].name, tostring(field)))
			end
		end
	end

	local backend_addr = proxy.global.backends[proxy.connection.backend_ndx].dst.name
	backend_lag[backend_addr].check_ts = os.time()
	backend_lag[backend_addr].state = nil

	if not show_slave_status["Master_Host"] then
		-- this backend is not a slave
		backend_lag[backend_addr].state = "noslave"
		return proxy.PROXY_IGNORE_RESULT
	end

	if show_slave_status["Master_Log_File"] == show_slave_status["Relay_Master_Log_File"] then
		-- ok, we use the same relay-log for reading and writing
		backend_lag[backend_addr].slave_bytes_lag = tonumber(show_slave_status["Exec_Master_Log_Pos"]) - tonumber(show_slave_status["Read_Master_Log_Pos"])
	else
		backend_lag[backend_addr].slave_bytes_lag = nil
	end

	if show_slave_status["Seconds_Behind_Master"] then
		backend_lag[backend_addr].seconds_lag = tonumber(show_slave_status["Seconds_Behind_Master"])
	else
		backend_lag[backend_addr].seconds_lag = nil
	end

	if show_slave_status["Slave_IO_Running"] == "No" then
		backend_lag[backend_addr].state = "noiothread"
	end

	backend_lag[backend_addr].state = backend_lag[backend_addr].state or "running"

	return proxy.PROXY_IGNORE_RESULT
end


