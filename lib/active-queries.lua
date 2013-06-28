--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2008, Oracle and/or its affiliates. All rights reserved.

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

-- proxy.auto-config will pick them up
local commands = require("proxy.commands")
local auto_config = require("proxy.auto-config")

--- init the global scope
if not proxy.global.active_queries then
	proxy.global.active_queries = {}
end

if not proxy.global.max_active_trx then
	proxy.global.max_active_trx = 0
end

-- default config for this script
if not proxy.global.config.active_queries then
	proxy.global.config.active_queries = {
		show_idle_connections = false
	}
end

---
-- track the active queries and dump all queries at each state-change
--

function collect_stats()
	local num_conns = 0
	local active_conns = 0

	for k, v in pairs(proxy.global.active_queries) do
		num_conns = num_conns + 1

		if v.state ~= "idle" then
			active_conns = active_conns + 1
		end
	end

	if active_conns > proxy.global.max_active_trx then
		proxy.global.max_active_trx = active_conns
	end

	return {
		active_conns = active_conns,
		num_conns = num_conns,
		max_active_trx = proxy.global.max_active_trx
	}
end

---
-- dump the state of the current queries
-- 
function print_stats(stats)
	local o = ""

	for k, v in pairs(proxy.global.active_queries) do
		if v.state ~= "idle" or proxy.global.config.active_queries.show_idle_connections then
			local cmd_query = ""
			if v.cmd then
				cmd_query = string.format("(%s) %q", v.cmd.type_name, v.cmd.query or "")
			end
			o = o .."  ["..k.."] (".. v.username .."@".. v.db ..") " .. cmd_query .." (state=" .. v.state .. ")\n"
		end
	end

	-- prepend the data and the stats about the number of connections and trx
	o = os.date("%Y-%m-%d %H:%M:%S") .. "\n" ..
		"  #connections: " .. stats.num_conns .. 
		", #active trx: " .. stats.active_conns .. 
		", max(active trx): ".. stats.max_active_trx .. 
		"\n" .. o

	print(o)
end

--- 
-- enable tracking the packets
function read_query(packet) 
	local cmd = commands.parse(packet)
	local r = auto_config.handle(cmd)
	if r then return r end
	
	proxy.queries:append(1, packet)

	-- add the query to the global scope
	local connection_id = proxy.connection.server.thread_id

	proxy.global.active_queries[connection_id] = { 
		state = "started",
		cmd = cmd,
		db = proxy.connection.client.default_db or "",
		username = proxy.connection.client.username or ""
	}

	print_stats(collect_stats())

	return proxy.PROXY_SEND_QUERY
end

---
-- statement is done, track the change
function read_query_result(inj)
	local connection_id = proxy.connection.server.thread_id

	proxy.global.active_queries[connection_id].state = "idle"
	proxy.global.active_queries[connection_id].cmd = nil
	
	if inj.resultset then
		local res = inj.resultset

		if res.flags.in_trans then
			proxy.global.active_queries[connection_id].state = "in_trans" 
		end
	end

	print_stats(collect_stats())
end

---
-- remove the information about the connection 
-- 
function disconnect_client()
	local connection_id = proxy.connection.server.thread_id
	if connection_id then
		proxy.global.active_queries[connection_id] = nil
	
		print_stats(collect_stats())
	end
end

