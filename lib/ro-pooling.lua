--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
-- a flexible statement based load balancer with connection pooling
--
-- * build a connection pool of min_idle_connections for each backend and 
--   maintain its size
-- * reusing a server-side connection when it is idling
-- 

--- config
--
-- connection pool
local min_idle_connections = 4
local max_idle_connections = 8

-- debug
local is_debug = false

--- end of config

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_in_transaction = 0

--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server() 
	-- make sure that we connect to each backend at least ones to 
	-- keep the connections to the servers alive
	--
	-- on read_query we can switch the backends again to another backend

	if is_debug then
		print()
		print("[connect_server] ")
	end

	local least_idle_conns_ndx = 0
	local least_idle_conns = 0

	for i = 1, #proxy.global.backends do
		local s = proxy.global.backends[i]
		local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
		local cur_idle = pool.users[""].cur_idle_connections

		if is_debug then
			print("  [".. i .."].connected_clients = " .. s.connected_clients)
			print("  [".. i .."].idling_connections = " .. cur_idle)
			print("  [".. i .."].type = " .. s.type)
			print("  [".. i .."].state = " .. s.state)
		end

		if s.state ~= proxy.BACKEND_STATE_DOWN then
			-- try to connect to each backend once at least
			if cur_idle == 0 then
				proxy.connection.backend_ndx = i
				if is_debug then
					print("  [".. i .."] open new connection")
				end
				return
			end

			-- try to open at least min_idle_connections
			if least_idle_conns_ndx == 0 or
			   ( cur_idle < min_idle_connections and 
			     cur_idle < least_idle_conns ) then
				least_idle_conns_ndx = i
				least_idle_conns = s.idling_connections
			end
		end
	end

	if least_idle_conns_ndx > 0 then
		proxy.connection.backend_ndx = least_idle_conns_ndx
	end

	if proxy.connection.backend_ndx > 0 then 
		local s = proxy.global.backends[proxy.connection.backend_ndx]
		local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
		local cur_idle = pool.users[""].cur_idle_connections

		if cur_idle >= min_idle_connections then
			-- we have 4 idling connections in the pool, that's good enough
			if is_debug then
				print("  using pooled connection from: " .. proxy.connection.backend_ndx)
			end
	
			return proxy.PROXY_IGNORE_RESULT
		end
	end

	if is_debug then
		print("  opening new connection on: " .. proxy.connection.backend_ndx)
	end

	-- open a new connection 
end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )
	if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
		-- auth was fine, disconnect from the server
		proxy.connection.backend_ndx = 0
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
		-- we received either a 
		-- 
		-- * MYSQLD_PACKET_ERR and the auth failed or
		-- * MYSQLD_PACKET_EOF which means a OLD PASSWORD (4.0) was sent
		print("(read_auth_result) ... not ok yet");
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
		-- auth failed
	end
end


--- 
-- read/write splitting
function read_query( packet ) 
	if is_debug then
		print("[read_query]")
		print("  authed backend = " .. proxy.connection.backend_ndx)
		print("  used db = " .. proxy.connection.client.default_db)
	end

	if packet:byte() == proxy.COM_QUIT then
		-- don't send COM_QUIT to the backend. We manage the connection
		-- in all aspects.
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
		}

		return proxy.PROXY_SEND_RESULT
	end

	if proxy.connection.backend_ndx == 0 then
		-- we don't have a backend right now
		-- 
		-- let's pick a master as a good default
		for i = 1, #proxy.global.backends do
			local s = proxy.global.backends[i]
			local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
			local cur_idle = pool.users[proxy.connection.client.username].cur_idle_connections
			
			if cur_idle > 0 and 
			   s.state ~= proxy.BACKEND_STATE_DOWN and 
			   s.type == proxy.BACKEND_TYPE_RW then
				proxy.connection.backend_ndx = i
				break
			end
		end
	end

	if true or proxy.connection.client.default_db and proxy.connection.client.default_db ~= proxy.connection.server.default_db then
		-- sync the client-side default_db with the server-side default_db
		proxy.queries:append(2, string.char(proxy.COM_INIT_DB) .. proxy.connection.client.default_db, { resultset_is_needed = true })
	end
	proxy.queries:append(1, packet)

	return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj ) 
	local res      = assert(inj.resultset)
  	local flags    = res.flags

	if inj.id ~= 1 then
		-- ignore the result of the USE <default_db>
		return proxy.PROXY_IGNORE_RESULT
	end
	is_in_transaction = flags.in_trans

	if not is_in_transaction then
		-- release the backend
		proxy.connection.backend_ndx = 0
	end
end

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
	if is_debug then
		print("[disconnect_client]")
	end

	if proxy.connection.backend_ndx == 0 then
		-- currently we don't have a server backend assigned
		--
		-- pick a server which has too many idling connections and close one
		for i = 1, #proxy.global.backends do
			local s = proxy.global.backends[i]
			local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
			local cur_idle = pool.users[proxy.connection.client.username].cur_idle_connections

			if s.state ~= proxy.BACKEND_STATE_DOWN and
			   cur_idle > max_idle_connections then
				-- try to disconnect a backend
				proxy.connection.backend_ndx = i
				if is_debug then
					print("  [".. i .."] closing connection, idling: " .. cur_idle)
				end
				return
			end
		end
	end
end
