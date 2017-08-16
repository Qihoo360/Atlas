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

function set_error(errmsg) 
	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = errmsg or "error"
	}
end

function set_ok(okmsg) 
	proxy.response = {
		type = proxy.MYSQLD_PACKET_OK,
		okmsg = okmsg or "ok"
	}
    
end
    


function string_join(key, value)
    local t = type(value) 
    if t == "string" then 
        local value_len = #value
        local value_get = string.sub(value, 1, value_len)
        return string.format("%s = %s", key, value_get)
    elseif t == "number" then
        return string.format("%s = %d", key, value)
    else 
        return " "
    end
end
function toString(v) 
    local t = type(v)
    if t == "boolean" then
        if v == true then
            return "True" 
        else
            return "False"
        end
    elseif t == "number" then    
        return string.format("%d", v) 
    elseif t == "string" then
        return v
    else 
        return " " 
    end 
end
    


--split the string by seperator 

function Split(szFullString, szSeparator)
    local nFindStartIndex = 1
    local nSplitIndex = 1
    local nSplitArray = {}
    while true do
       local nFindLastIndex = string.find(szFullString, szSeparator, nFindStartIndex)
       if not nFindLastIndex then 
            nSplitArray[nSplitIndex] = string.sub(szFullString, nFindStartIndex, string.len(szFullString))
            break
       end
       nSplitArray[nSplitIndex] = string.sub(szFullString, nFindStartIndex, nFindLastIndex - 1)
       nFindStartIndex = nFindLastIndex + string.len(szSeparator)
       nSplitIndex = nSplitIndex + 1
    end
    return nSplitArray
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
			{ name = "global_id", 
			  type = proxy.MYSQL_TYPE_LONG },
			{ name = "address",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "state",
			  type = proxy.MYSQL_TYPE_STRING },
			{ name = "type",
			  type = proxy.MYSQL_TYPE_STRING },
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
				b.info,          -- configured backend address
				states[b.state + 1], -- the C-id is pushed down starting at 0
				types[b.type + 1],   -- the C-id is pushed down starting at 0
			}
		end
    elseif string.find(query:lower(), "%s*add%s+sql_filter%s+\".+\"$") then -----add sql filter rule
        local rule = tostring(string.match(query:lower(), "%s*add%s+sql_filter%s+\"(.+)\"$"))
        if string.len(rule) == 0 then
			set_error("sql filter must not be empty")
			return proxy.PROXY_SEND_RESULT
        end
        ---to add content
        proxy.global.forbidden_sql.add_sql = rule
        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT
        ----fields = {
        ----    { name = "status", 
        ----    type = proxy.MYSQL_TYPE_STRING },
        ----}
    elseif string.find(query:lower(), "%s*remove%s+sql_filter%s+%d+$") then -----add sql filter rule
        local index = tonumber(string.match(query:lower(), "%s*remove%s+sql_filter%s+(%d+)"))
        ---to add content
        if index <= 0 or index > #proxy.global.forbidden_sql then 
            set_error("invalid sql_filter id")
            return proxy.PROXY_SEND_RESULT
        else 
            proxy.global.forbidden_sql.remove_sql = index - 1;
            set_ok("Query OK, 0 rows affected(0.00 sec)")
            return proxy.PROXY_SEND_RESULT
            --fields = {
            --    { name = "status", 
            --    type = proxy.MYSQL_TYPE_STRING },
            --}
        end 

    elseif string.find(query:lower(), "%s*select%s+*%s+from%s+sql_filters$") then -----add sql filter rule
        fields = {
            { name = "id", 
            type = proxy.MYSQL_TYPE_LONG },
            { name = "forbidden_sql", 
            type = proxy.MYSQL_TYPE_STRING },
        }
        for i = 1, #proxy.global.forbidden_sql do
            rows[#rows + 1] = { 
                i, 
                proxy.global.forbidden_sql[i],  
            }
        end
    elseif string.find(query:lower(), "%s*set%s+%a+%s+%d+$") then
        local state,id = string.match(query:lower(), "%s*set%s+(%a+)%s+(%d+)$")
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
            b.info,          -- configured backend address
            states[b.state + 1], -- the C-id is pushed down starting at 1
            types[b.type + 1],   -- the C-id is pushed down starting at 0
        }
    elseif string.find(query:lower(), "^%s*set%s+%w+%-?%w*%-?%w*%-?%w*%s*%=%s*%w+%s*$") then
        local k, v = string.match(query:lower(), "^%s*set%s+(%w+%-?%w*%-?%w*%-?%w*)%s*%=%s*(%w+)%s*$")
        if k:lower() == "auto-sql-filter" then
            proxy.global.router_paras.autoSqlFilter = v  
        elseif k:lower() == "set-router-rule" then     
            proxy.global.router_paras.setRouterRule = v  
        elseif k:lower() == "sql-safe-update" then     
            proxy.global.router_paras.sqlSafeUpdate = v  
        elseif k:lower() == "max-conn-in-pool" then
            proxy.global.router_paras.maxConn = v  
        end
        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT
        --fields = {
        --    { name = "status", 
        --    type = proxy.MYSQL_TYPE_STRING },
        --}
    elseif string.find(query:lower(), "^add%s+master%s+[%w_-]+::[%w_-]+::%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?$") then
        local newserver = string.match(query:lower(), "^add%s+master%s+(.+)$")
        if newserver == nil then 
            set_error("invalid operation")
            return proxy.PROXY_SEND_RESULT
        end 
        proxy.global.backends.addmaster = newserver
        if proxy.global.config.rwsplit then proxy.global.config.rwsplit.max_weight = -1 end

        ---fields = {
        ---    { name = "status", 
        ---    type = proxy.MYSQL_TYPE_STRING },
        ---}
        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT
    elseif string.find(query:lower(), "^add%s+slave%s+[%w_-]+::[%w_-]+::%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?@?%d?%d?%d?$") then
        local newserver = string.match(query:lower(), "^add%s+slave%s+(.+)$")
        group, slave, addr = string.match(query:lower(), "^add%s+slave%s+([%w_-]+)::([%w_-]+)::(%d%d?%d?%.%d%d?%d?%.%d%d?%d?%.%d%d?%d?:%d%d?%d?%d?%d?@?%d?%d?)$") 

        if string.lower(group) ~= string.lower(proxy.global.config.default_node) then
            if string.match(slave, ".+0$") == nil then  
                set_error("invalid operation. only master group support taged slaves")
                return proxy.PROXY_SEND_RESULT
            end
        end

        proxy.global.backends.addslave = newserver
        if proxy.global.config.rwsplit then 
            proxy.global.config.rwsplit.max_weight = -1 
        end

        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT
        ----fields = {
        ----    { name = "status", 
        ----    type = proxy.MYSQL_TYPE_STRING },
        ----}
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
        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT
    elseif string.find(query:lower(), "^select%s+*%s+from%s+clients$") then
        fields = {
            { name = "client",
            type = proxy.MYSQL_TYPE_STRING },
        }
        for i = 1, #proxy.global.clients do
            rows[#rows + 1] = {
                proxy.global.clients[i]
            }
        end
    elseif string.find(query:lower(), "^add%s+client%s+(.+)$") then
        local client = string.match(query:lower(), "^add%s+client%s+(.+)$")

        if proxy.global.clients(client) == 1 then
            set_error("this client is exist")
            return proxy.PROXY_SEND_RESULT
        end

        proxy.global.backends.addclient = client

        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT

        --fields = {
        --    { name = "status",
        --    type = proxy.MYSQL_TYPE_STRING },
        --}
    elseif string.find(query:lower(), "^remove%s+client%s+(.+)$") then
        local client = string.match(query:lower(), "^remove%s+client%s+(.+)$")

        if proxy.global.clients(client) == 0 then
            set_error("this client is NOT exist")
            return proxy.PROXY_SEND_RESULT
        end

        proxy.global.backends.removeclient = client

        set_ok("Query OK, 0 rows affected(0.00 sec)")
        return proxy.PROXY_SEND_RESULT

        ---fields = {
        ---    { name = "status",
        ---    type = proxy.MYSQL_TYPE_STRING },
        ---}
    elseif string.find(query:lower(), "^select%s+*%s+from%s+pwds$") then
        fields = {
            { name = "username",
            type = proxy.MYSQL_TYPE_STRING },
            { name = "password",
            type = proxy.MYSQL_TYPE_STRING },
        }
        for i = 1, #proxy.global.pwds do
            local user_pwd = proxy.global.pwds[i]
            local pos = string.find(user_pwd, ":")
            rows[#rows + 1] = {
                string.sub(user_pwd, 1, pos-1),
                string.sub(user_pwd, pos+1)
            }
        end
    elseif string.find(query, "^[aA][dD][dD]%s+[pP][wW][dD]%s+(.+):(.+)$") then
        local user, pwd = string.match(query, "^[aA][dD][dD]%s+[pP][wW][dD]%s+(.+):(.+)$")
        local ret = proxy.global.backends(user, pwd, 1)

        if ret == 1 then
            set_error("this user is exist")
            return proxy.PROXY_SEND_RESULT
        end

        if ret == 2 then
            set_error("failed to encrypt")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^[aA][dD][dD]%s+[eE][nN][pP][wW][dD]%s+(.+):(.+)$") then
        local user, pwd = string.match(query, "^[aA][dD][dD]%s+[eE][nN][pP][wW][dD]%s+(.+):(.+)$")
        local ret = proxy.global.backends(user, pwd, 2)

        if ret == 1 then
            set_error("this user is exist")
            return proxy.PROXY_SEND_RESULT
        end

        if ret == 2 then
            set_error("failed to decrypt")
            return proxy.PROXY_SEND_RESULT
        end

        fields = {
            { name = "status",
            type = proxy.MYSQL_TYPE_STRING },
        }
    elseif string.find(query, "^[rR][eE][mM][oO][vV][eE]%s+[pP][wW][dD]%s+(.+)$") then
        local user = string.match(query, "^[rR][eE][mM][oO][vV][eE]%s+[pP][wW][dD]%s+(.+)$")
        local ret = proxy.global.backends(user, nil, 3)

        if ret == 1 then
            set_error("this user is NOT exist")
            return proxy.PROXY_SEND_RESULT
        end

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
    elseif string.find(query:lower(), "^select%s+version+$") then
        fields = {
            { name = "version",
            type = proxy.MYSQL_TYPE_STRING },
        }
        rows[#rows + 1] = { "2.2.3" }
    elseif string.find(query:lower(), "^show%s+status%s*$") then     
        fields = {
            { name = "statistics", 
            type = proxy.MYSQL_TYPE_STRING}, 
            { name = "count", 
            type = proxy.MYSQL_TYPE_LONG},  
        }
        rows[#rows + 1] = {"up_time", proxy.global.statistics.uptime}

        rows[#rows + 1] = {"flush_time", proxy.global.statistics.flushtime}
        rows[#rows + 1] = {"com_select", proxy.global.statistics.comselect}
        rows[#rows + 1] = {"com_insert", proxy.global.statistics.cominsert}
        rows[#rows + 1] = {"com_update", proxy.global.statistics.comupdate}
        rows[#rows + 1] = {"com_delete", proxy.global.statistics.comdelete}
        rows[#rows + 1] = {"com_replace", proxy.global.statistics.comreplace}

        rows[#rows + 1] = {"com_error", proxy.global.statistics.comerror}
        rows[#rows + 1] = {"threadsconnected", proxy.global.statistics.threadconnected}
        rows[#rows + 1] = {"threadsrunning", proxy.global.statistics.threadrunning}

    elseif string.find(query:lower(), "^flush%s+status$") then     
        fields = {
            { name = "status", 
            type = proxy.MYSQL_TYPE_STRING}, 
        }
        proxy.global.statistics.comselect = 0
        proxy.global.statistics.comreplace = 0
        proxy.global.statistics.cominsert = 0
        proxy.global.statistics.comdelete = 0
        proxy.global.statistics.comerror = 0
        proxy.global.statistics.comupdate = 0
        proxy.global.statistics.comreplace = 0


    elseif string.find(query:lower(), "^show%s+variables%s?$") then
        fields = {
            { name = "variable_name",
            type = proxy.MYSQL_TYPE_STRING},
            { name = "group",
            type = proxy.MYSQL_TYPE_STRING},
            { name = "value",
            type = proxy.MYSQL_TYPE_STRING},
            { name = "type",
            type = proxy.MYSQL_TYPE_STRING},
        }
        local log_level = proxy.global.config.loglevel
        rows[#rows + 1] = { "log-level", "NULL", toString(log_level), "Static"}

        local log_path = proxy.global.config.logpath
        rows[#rows + 1] = { "log-path", "NULL",toString(log_path), "Static"}

        local daemon = proxy.global.config.daemon
        rows[#rows + 1] = {"daemon", "NULL", toString(daemon), "Static"}

        local keepalive = proxy.global.config.keepalive  
        rows[#rows + 1] = {"keep-alive", "NULL", toString(keepalive), "Static"}

        local threadsNums = proxy.global.config.eventthreads
        rows[#rows + 1] = {"event-threds", "NULL", toString(threadsNums), "Static"}

        local instance = proxy.global.config.instance
        rows[#rows + 1] = {"instance", "NULL", toString(instance), "Static"}

        local charset = proxy.global.config.charset
        rows[#rows + 1] = {"charset", "NULL", toString(charset), "Static"}
        ---rows[#rows + 1] = { string_join("charset", charset) }

        local sys_log = proxy.global.config.use_syslog
        rows[#rows + 1] = {"sql-log", "NULL", toString(sys_log), "Static"}

        local default_node = proxy.global.config.default_node
        rows[#rows + 1] = {"default-node", "NULL", toString(default_node), "Static"}

        local auto_sql_filter = proxy.global.router_paras.auto_sql_filter 
        rows[#rows + 1] = {"auto-sql-filter", "NULL", toString(auto_sql_filter), "Dynamic"}

        local set_router_rule = proxy.global.router_paras.set_router_rule 
        rows[#rows + 1] = {"set-router-rule", "NULL", toString(set_router_rule), "Dynamic"}

        local sql_safe_update = proxy.global.router_paras.sql_safe_update 
        rows[#rows + 1] = {"sql-safe-update", "NULL", toString(sql_safe_update), "Dynamic"}

        local max_conn = proxy.global.router_paras.max_conn_in_pool 
        rows[#rows + 1] = {"max-conn-in-pool", "NULL", toString(max_conn), "Dynamic"}


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
        rows[#rows + 1] = { "SET ARGS = V", "set up args = value, args like " }
        rows[#rows + 1] = { "SET ONLINE $backend_id", "online backend server, ..." }
        rows[#rows + 1] = { "ADD MASTER $backend", "example: \"add master 127.0.0.1:3306\", ..." }
        rows[#rows + 1] = { "ADD SLAVE $backend", "example: \"add slave 127.0.0.1:3306\", ..." }
        rows[#rows + 1] = { "REMOVE BACKEND $backend_id", "example: \"remove backend 1\", ..." }

        rows[#rows + 1] = { "SELECT * FROM clients", "lists the clients" }
        rows[#rows + 1] = { "ADD CLIENT $client", "example: \"add client 192.168.1.2\", ..." }
        rows[#rows + 1] = { "REMOVE CLIENT $client", "example: \"remove client 192.168.1.2\", ..." }

        rows[#rows + 1] = { "SELECT * FROM pwds", "lists the pwds" }
        rows[#rows + 1] = { "ADD PWD $pwd", "example: \"add pwd user:raw_password\", ..." }
        rows[#rows + 1] = { "ADD ENPWD $pwd", "example: \"add enpwd user:encrypted_password\", ..." }
        rows[#rows + 1] = { "REMOVE PWD $pwd", "example: \"remove pwd user\", ..." }

        rows[#rows + 1] = { "SAVE CONFIG", "save the backends to config file" }
        rows[#rows + 1] = { "SELECT VERSION", "display the version of Atlas" }

        rows[#rows + 1] = { "SHOW VARIABLES", "display configure of Atlas"} 
        rows[#rows + 1] = { "SHOW STATUS", "display statistics of query"} 
        rows[#rows + 1] = { "FLUSH STATUS", "reset the statistics fo query"}

        rows[#rows + 1] = {"ADD SQL_FILTER $sql", "add forbiddened \"sql\""}
        rows[#rows + 1] = {"REMOVE SQL_FILTER $sql_id", "remove forbiddened sql"}
        rows[#rows + 1] = {"SELECT * FROM SQL_FILTERS", "shwo all the forbiddened sqls"}
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
