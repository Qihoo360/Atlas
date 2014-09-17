--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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

--[[
-- redefine next, used for iterate userdata
rawnext = next
function next(t,k)
    local m = getmetatable(t)
    local n = m and m.__next or rawnext
    return n(t,k)
end
]]

function set_error(errmsg) 
	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = errmsg or "error"
	}
end

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		set_error("[admin] we only handle text-based queries (COM_QUERY)")
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2)

	local rows = { }
	local fields = { }

	if string.find(query:lower(), "^select%s+*%s+from%s+backends$") then
		fields = { 
			{ name = "backend_ndx", 
			  type = proxy.MYSQL_TYPE_LONG },
            --[[{ name = "username",
              type = proxy.MYSQL_TYPE_STRING },
              ]]
			{ name = "address",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "state",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "type",
			  type = proxy.MYSQL_TYPE_STRING },
		--	{ name = "uuid",
		--	  type = proxy.MYSQL_TYPE_STRING },
		--	{ name = "connected_clients", 
		--	  type = proxy.MYSQL_TYPE_LONG },
              --[[
            { name = "cur_idle_connections",
              type = proxy.MYSQL_TYPE_LONG },
              ]]
		}

		for i = 1, #proxy.global.backends do
			local states = {
				"unknown",
				"up",
				"down",
				"offline"
			}
			local types = {
				"unknown",
				"rw",
				"ro"
			}
			local b = proxy.global.backends[i]

            rows[#rows + 1] = {
                i,
                b.dst.name,          -- configured backend address
                    states[b.state + 1], -- the C-id is pushed down starting at 0
                    types[b.type + 1],   -- the C-id is pushed down starting at 0
                    --b.uuid,              -- the MySQL Server's UUID if it is managed
                    --b.connected_clients  -- currently connected clients
            }

            --[[
            for j, username in next, b.pool.users, 0 do
        
                rows[#rows + 1] = {
                    i,
                    username,
                    b.dst.name,          -- configured backend address
                    states[b.state + 1], -- the C-id is pushed down starting at 0
                    types[b.type + 1],   -- the C-id is pushed down starting at 0
                    b.uuid,              -- the MySQL Server's UUID if it is managed
                    b.connected_clients,  -- currently connected clients
                    b.pool.users[username].cur_idle_connections
                }
            end
            ]]
		end
	elseif string.find(query:lower(), "^set%s+%a+%s+%d+$") then
		local state,id = string.match(query:lower(), "^set%s+(%a+)%s+(%d+)$")
		if proxy.global.backends[id] == nil then
			set_error("backend id is not exsit")
			return proxy.PROXY_SEND_RESULT
		end

		if state == "offline" then
			proxy.global.backends[id].state = 3
		elseif state == "online" then
			proxy.global.backends[id].state = 0
		else
			set_error("invalid operation")
			return proxy.PROXY_SEND_RESULT
		end

		fields = { 
			{ name = "backend_ndx", 
			  type = proxy.MYSQL_TYPE_LONG },
			{ name = "address",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "state",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "type",
			  type = proxy.MYSQL_TYPE_STRING },
		--	{ name = "uuid",
		--	  type = proxy.MYSQL_TYPE_STRING },
		--	{ name = "connected_clients", 
		--	  type = proxy.MYSQL_TYPE_LONG },
		}

		local states = {
			"unknown",
			"up",
			"down",
			"offline"
		}
		local types = {
			"unknown",
			"rw",
			"ro"
		}
		local b = proxy.global.backends[id]

		rows[#rows + 1] = {
			id,
			b.dst.name,          -- configured backend address
			states[b.state + 1], -- the C-id is pushed down starting at 0
			types[b.type + 1],   -- the C-id is pushed down starting at 0
		--	b.uuid,              -- the MySQL Server's UUID if it is managed
		--	b.connected_clients  -- currently connected clients
		}
	elseif string.find(query:lower(), "^add%s+master%s+%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?$") then
        	local newserver = string.match(query:lower(), "^add%s+master%s+(.+)$")
        	proxy.global.backends.addmaster = newserver
		if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

		fields = {
			{ name = "status", 
			  type = proxy.MYSQL_TYPE_STRING },
		}
	elseif string.find(query:lower(), "^add%s+slave%s+%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?$") then
        	local newserver = string.match(query:lower(), "^add%s+slave%s+(.+)$")
        	proxy.global.backends.addslave = newserver
		if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

		fields = {
			{ name = "status", 
			  type = proxy.MYSQL_TYPE_STRING },
		}
	elseif string.find(query:lower(), "^remove%s+backend%s+%d+$") then
        	local newserver = tonumber(string.match(query:lower(), "^remove%s+backend%s+(%d+)$"))
		if newserver <= 0 or newserver > #proxy.global.backends then
			set_error("invalid backend_id")
			return proxy.PROXY_SEND_RESULT
		else
			proxy.global.backends.removebackend = newserver - 1
			if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

			fields = {
				{ name = "status", 
				  type = proxy.MYSQL_TYPE_STRING },
			}
		end
	elseif string.find(query:lower(), "^add%s+client%s+%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?$") then
		local client = string.match(query:lower(), "^add%s+client%s+(.+)$")
		proxy.global.backends.addclient = client

		fields = {
			{ name = "status",
			  type = proxy.MYSQL_TYPE_STRING },
		}
	elseif string.find(query:lower(), "^remove%s+client%s+%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?$") then
		local client = string.match(query:lower(), "^remove%s+client%s+(.+)$")
		proxy.global.backends.removeclient = client

		fields = {
			{ name = "status",
			  type = proxy.MYSQL_TYPE_STRING },
		}
	elseif string.find(query:lower(), "^save%s+config+$") then
		proxy.global.backends.saveconfig = 0
		fields = {
			{ name = "status",
			  type = proxy.MYSQL_TYPE_STRING },
		}
	elseif string.find(query:lower(), "^select%s+*%s+from%s+help$") then
		fields = { 
			{ name = "command", 
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "description", 
			  type = proxy.MYSQL_TYPE_STRING },
		}
		rows[#rows + 1] = { "SELECT * FROM help", "shows this help" }
		rows[#rows + 1] = { "SELECT * FROM backends", "lists the backends and their state" }
		rows[#rows + 1] = { "SET OFFLINE $backend_id", "offline backend server, $backend_id is backend_ndx's id" }
		rows[#rows + 1] = { "SET ONLINE $backend_id", "online backend server, ..." }
		rows[#rows + 1] = { "ADD MASTER $backend", "example: \"add master 127.0.0.1:3306\", ..." }
		rows[#rows + 1] = { "ADD SLAVE $backend", "example: \"add slave 127.0.0.1:3306\", ..." }
		rows[#rows + 1] = { "REMOVE BACKEND $backend_id", "example: \"remove backend 1\", ..." }
		rows[#rows + 1] = { "ADD CLIENT $client", "example: \"add client 192.168.1.2\", ..." }
		rows[#rows + 1] = { "REMOVE CLIENT $client", "example: \"remove client 192.168.1.2\", ..." }
		rows[#rows + 1] = { "SAVE CONFIG", "save the backends to config file" }
	else
		set_error("use 'SELECT * FROM help' to see the supported commands")
		return proxy.PROXY_SEND_RESULT
	end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_OK,
		resultset = {
			fields = fields,
			rows = rows
		}
	}
	return proxy.PROXY_SEND_RESULT
end
