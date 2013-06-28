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
-- a flexible statement based load balancer with connection pooling
--
-- * build a connection pool of min_idle_connections for each backend and maintain
--   its size
-- * 
-- 
-- 

local commands    = require("proxy.commands")
local tokenizer   = require("proxy.tokenizer")
local lb          = require("proxy.balance")
local auto_config = require("proxy.auto-config")
local parser      = require("proxy.parser")
local log		  = require("proxy.log")
local charset	  = require("proxy.charset")
local split		  = require("proxy.split")
local config_file = string.format("proxy.conf.config_%s", proxy.global.config.instance)
local config	  = require(config_file)
local auth		  = require("proxy.auth")

--- config
--
-- connection pool
local level		= log.level
local write_log	= log.write_log
local write_sql	= log.write_sql
local write_des	= log.write_des
local write_query = log.write_query
local lvs_ip      = config.lvs_ip
local proxy_cnf   = string.format("%s/../conf/%s.cnf", proxy.global.config.logpath, proxy.global.config.instance) 

function update_rwsplit()
	local weight = 0
	for i = 1, #proxy.global.backends do
		if weight < proxy.global.backends[i].weight then
			weight = proxy.global.backends[i].weight
		end
	end

	proxy.global.config.rwsplit = {
		min_idle_connections = config.min_idle_connections,
		max_idle_connections = 500,

		is_debug = config.debug_info,
		is_auth  = config.is_auth,

		max_weight = weight,
		cur_weight = weight,
		next_ndx   = 1,
		ndx_num    = #proxy.global.backends
	}
end

if not proxy.global.config.rwsplit then update_rwsplit() end

---
-- read/write splitting sends all non-transactional SELECTs to the slaves
--
-- is_in_transaction tracks the state of the transactions
local is_in_transaction       = false

-- if this was a SELECT SQL_CALC_FOUND_ROWS ... stay on the same connections
local is_in_select_calc_found_rows = false

local is_in_lock = false

--log.init_log(proxy.global.config.rwsplit.log_level, proxy.global.config.rwsplit.is_rt, proxy.global.config.rwsplit.is_sql)
log.init_log()

-- Global tokens
g_tokens = {}
-- SPLIT SQL FOR Mutil table
merge_res = {
    sub_sql_num = 0,
    sub_sql_exed = 0,
    rows = {},
    sortindex = false,
    sorttype = false,
    limit = 5000
}

--- 
-- get a connection to a backend
--
-- as long as we don't have enough connections in the pool, create new connections
--
function connect_server()
	write_log(level.DEBUG, "ENTER CONNECT_SERVER")

	--for i = 1, #proxy.global.backends do	--global对应chassis_private？
		--print(proxy.global.backends[i].dst.name)
	--end

	--print("---------------------------------")
	local is_debug = proxy.global.config.rwsplit.is_debug
	local is_auth  = proxy.global.config.rwsplit.is_auth
	-- make sure that we connect to each backend at least ones to 
	-- keep the connections to the servers alive
	--
	-- on read_query we can switch the backends again to another backend

	local client_src = proxy.connection.client.src.name 
	local client_dst = proxy.connection.client.dst.name 

	if is_debug then
		print()
		print("[connect_server] " .. client_src)	--connection对应结构network_mysqld_con
		print("[connect_server] " .. client_dst)	--connection对应结构network_mysqld_con
	end

	write_log(level.INFO, "[connect_server] ", client_src)	--connection对应结构network_mysqld_con
	write_log(level.INFO, "[connect_server] ", client_dst)	--connection对应结构network_mysqld_con

	local client_ip = string.sub(client_src, 1, string.find(client_src, ':')-1)

	for i = 1, #lvs_ip do
		if client_ip == lvs_ip[i] then
			io.input(proxy_cnf)
			for line in io.lines() do
				line = line:lower()
				if string.find(line, "online") ~= nil then
					line = string.gsub(line, "%s*online%s*=%s*", "")
					line = string.gsub(line, "%s*", "")
					if line == "false" then
						proxy.response =
						{
							type = proxy.MYSQLD_PACKET_ERR,
							errmsg = "Proxy Warning - Offline Now"
						}
						return proxy.PROXY_SEND_RESULT
					end
				end
			end
			io.close(proxy_cnf)
			io.input(stdin)
			break
		end
	end

	if is_auth and auth.allow_ip(client_ip) == false then
		proxy.response =
		{
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "Proxy Warning - IP Forbidden"
		}
		return proxy.PROXY_SEND_RESULT
	end

	local rw_ndx = 0

	-- init all backends 
	for i = 1, #proxy.global.backends do
		local s        = proxy.global.backends[i]	--s对应结构network_backend_t
		local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling. pool对应结构network_connection_pool
		local cur_idle = pool.users[""].cur_idle_connections	--cur_idle_connections对应什么？

		pool.min_idle_connections = proxy.global.config.rwsplit.min_idle_connections
		pool.max_idle_connections = proxy.global.config.rwsplit.max_idle_connections
		
		if is_debug then
			print("  [".. i .."].connected_clients = " .. s.connected_clients)
			print("  [".. i .."].pool.cur_idle     = " .. cur_idle)
			print("  [".. i .."].pool.max_idle     = " .. pool.max_idle_connections)
			print("  [".. i .."].pool.min_idle     = " .. pool.min_idle_connections)
			print("  [".. i .."].type = " .. s.type)	--type 的类型是enum backend_type_t
			print("  [".. i .."].state = " .. s.state)	--state的类型是enum backend_state_t
		end

		write_log(level.INFO, "  [", i, "].connected_clients = ", s.connected_clients)
		write_log(level.INFO, "  [", i, "].pool.cur_idle     = ", cur_idle)
		write_log(level.INFO, "  [", i, "].pool.max_idle     = ", pool.max_idle_connections)
		write_log(level.INFO, "  [", i, "].pool.min_idle     = ", pool.min_idle_connections)
		write_log(level.INFO, "  [", i, "].type = ", s.type)	--type 的类型是enum backend_type_t
		write_log(level.INFO, "  [", i, "].state = ", s.state)	--state的类型是enum backend_state_t

		-- prefer connections to the master
		if s.type == proxy.BACKEND_TYPE_RW and
		   s.state ~= proxy.BACKEND_STATE_DOWN and
		   s.state ~= proxy.BACKEND_STATE_OFFLINE and
		   cur_idle < pool.min_idle_connections then
			proxy.connection.backend_ndx = i	--connection又对应结构network_mysqld_con_lua_t？
			break
		elseif s.type == proxy.BACKEND_TYPE_RO and
		       s.state ~= proxy.BACKEND_STATE_DOWN and
		       s.state ~= proxy.BACKEND_STATE_OFFLINE and
		       cur_idle < pool.min_idle_connections then
			proxy.connection.backend_ndx = i
			break
		elseif s.type == proxy.BACKEND_TYPE_RW and
		       s.state ~= proxy.BACKEND_STATE_DOWN and
		       s.state ~= proxy.BACKEND_STATE_OFFLINE and
		       rw_ndx == 0 then
			rw_ndx = i
		end
	end

	if proxy.connection.backend_ndx == 0 then	--backend_ndx对应network_mysqld_con_lua_t.backend_ndx，若为0说明上面的判断走的第3条路径
		if is_debug then
			print("  [" .. rw_ndx .. "] taking master as default")	--从master的连接池里取一个连接
		end
		write_log(level.INFO, "  [", rw_ndx, "] taking master as default")	--从master的连接池里取一个连接
		proxy.connection.backend_ndx = rw_ndx
	end

	-- pick a random backend
	--
	-- we someone have to skip DOWN backends

	-- ok, did we got a backend ?

	if proxy.connection.server then	--connection又对应回了结构network_mysqld_con？ 
		if is_debug then
			print("  using pooled connection from: " .. proxy.connection.backend_ndx)	--从master的连接池里取一个连接返回给客户端
		end
		write_log(level.INFO, "  using pooled connection from: ", proxy.connection.backend_ndx)	--从master的连接池里取一个连接返回给客户端
		write_log(level.DEBUG, "LEAVE CONNECT_SERVER")

		-- stay with it
		return proxy.PROXY_IGNORE_RESULT
	end

	if is_debug then
		print("  [" .. proxy.connection.backend_ndx .. "] idle-conns below min-idle")	--创建新连接
	end
	write_log(level.INFO, "  [", proxy.connection.backend_ndx, "] idle-conns below min-idle")	--创建新连接
	write_log(level.DEBUG, "LEAVE CONNECT_SERVER")

	-- open a new connection 
end

--- 
-- put the successfully authed connection into the connection pool
--
-- @param auth the context information for the auth
--
-- auth.packet is the packet
function read_auth_result( auth )
	write_log(level.DEBUG, "ENTER READ_AUTH_RESULT")
	if is_debug then
		print("[read_auth_result] " .. proxy.connection.client.src.name)
	end
	write_log(level.INFO, "[read_auth_result] " .. proxy.connection.client.src.name)

	if auth.packet:byte() == proxy.MYSQLD_PACKET_OK then
		-- auth was fine, disconnect from the server
		proxy.connection.backend_ndx = 0	--auth成功，把连接放回连接池
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_EOF then
		-- we received either a 
		-- 
		-- * MYSQLD_PACKET_ERR and the auth failed or
		-- * MYSQLD_PACKET_EOF which means a OLD PASSWORD (4.0) was sent
		print("(read_auth_result) ... not ok yet");
	elseif auth.packet:byte() == proxy.MYSQLD_PACKET_ERR then
		-- auth failed
	end

	write_log(level.DEBUG, "LEAVE READ_AUTH_RESULT")
end

--- 
-- read/write splitting
function read_query( packet )
	write_log(level.DEBUG, "ENTER READ_QUERY")

	local is_debug = proxy.global.config.rwsplit.is_debug
	local cmd      = commands.parse(packet)
	local c        = proxy.connection.client

	local r = auto_config.handle(cmd)
	if r then
		write_log(level.DEBUG, "LEAVE READ_QUERY")
		return r
	end

	local tokens, attr
	local norm_query

	-- looks like we have to forward this statement to a backend
	if is_debug then
		print("[read_query] " .. proxy.connection.client.src.name)
		print("  current backend   = " .. proxy.connection.backend_ndx)
		print("  client default db = " .. c.default_db)	--default_db对应network_socket.default_db
		print("  client username   = " .. c.username)	--username对应什么？
		if cmd.type == proxy.COM_QUERY then 
			print("  query             = "        .. cmd.query)
		end
	end

	write_log(level.INFO, "[read_query] ", proxy.connection.client.src.name)
	write_log(level.INFO, "  current backend   = ", proxy.connection.backend_ndx)
	write_log(level.INFO, "  client default db = ", c.default_db)	--default_db对应network_socket.default_db
	write_log(level.INFO, "  client username   = ", c.username)	--username对应什么？
	if cmd.type == proxy.COM_QUERY then 
		write_log(level.INFO, "  query             = ", cmd.query)
	--	write_sql(cmd.query)
	end

	if cmd.type == proxy.COM_QUIT then	--quit;或ctrl-D
		-- don't send COM_QUIT to the backend. We manage the connection
		-- in all aspects.
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
		}
	
		if is_debug then
			print("  (QUIT) current backend   = " .. proxy.connection.backend_ndx)
		end

		write_log(level.INFO, "  (QUIT) current backend   = ", proxy.connection.backend_ndx)
		write_log(level.DEBUG, "LEAVE READ_QUERY")

		return proxy.PROXY_SEND_RESULT
	end

	--print("cmd.type = " .. cmd.type)

	if cmd.type == proxy.COM_QUERY then
        local new_sql, status
		tokens, attr = tokenizer.tokenize(cmd.query)
        -- set global
        g_tokens = tokens
        -- sql 解析失败则立即返回给客户端
        new_sql, status = split.sql_parse(tokens, cmd.query)
        if status == -1 then
			proxy.queries:reset()
			write_log(level.DEBUG, "LEAVE READ_QUERY")
			write_query(cmd.query, c.src.name)
	        return proxy.PROXY_SEND_RESULT
        end

		if status == 1 then
			if #new_sql > 1 then	--分表(多句)，打sql.log，有序号
				-- init muti sql
				merge_res.sub_sql_exed = 0
				merge_res.sub_sql_num = 0
				merge_res.rows = {}

				for id = 1, #new_sql do 
					proxy.queries:append(6, string.char(proxy.COM_QUERY) .. new_sql[id], { resultset_is_needed = true })
					merge_res.sub_sql_num = merge_res.sub_sql_num + 1
				end
			elseif #new_sql == 1 then	--分表(1句)，打sql.log，有序号
				proxy.queries:append(7, string.char(proxy.COM_QUERY) .. new_sql[1], { resultset_is_needed = true })
			else						--不分表且sql_type不为4，打sql.log，无序号
				proxy.queries:append(8, packet, { resultset_is_needed = true })
			end
		else	--不分表且sql_type为4，不打sql.log
			proxy.queries:append(1, packet, { resultset_is_needed = true })
        end
	else	--类型不是COM_QUERY，不打sql.log
		proxy.queries:append(1, packet, { resultset_is_needed = true })
	end

	-- read/write splitting 
	--
	-- send all non-transactional SELECTs to a slave
	if not is_in_transaction and not is_in_lock and
	   cmd.type == proxy.COM_QUERY then
		--tokens     = tokens or assert(tokenizer.tokenize(cmd.query))

		local stmt = string.upper(tokenizer.first_stmt_token(tokens).text)	--命令字符串的第一个单词

		if stmt == "SELECT" and tokens[2].text:upper() == "GET_LOCK" then
			is_in_lock = true
		end

		if stmt == "SELECT" or stmt == "SET" or stmt == "SHOW" or stmt == "DESC" then	--TK_**的定义在lib/sql-tokenizer.h里
			is_in_select_calc_found_rows = false	--初始化为false
			local is_insert_id = false

			for i = 1, #tokens do
				-- local token = tokens[i]
				-- SQL_CALC_FOUND_ROWS + FOUND_ROWS() have to be executed 
				-- on the same connection
				-- print("token: " .. token.token_name)
				-- print("  val: " .. token.text)

				local text = tokens[i].text:upper()

				if text == "SQL_CALC_FOUND_ROWS" then
					is_in_select_calc_found_rows = true	--SQL_CALC_FOUND_ROWS指令，将is_in_select_calc_found_rows设为true
				else
					if text == "LAST_INSERT_ID" or text == "@@INSERT_ID" or text == "@@LAST_INSERT_ID" then
						is_insert_id = true
					end
				end

				-- we found the two special token, we can't find more
				if is_insert_id and is_in_select_calc_found_rows then	--and还是or？
					break
				end
			end

			-- if we ask for the last-insert-id we have to ask it on the original 
			-- connection
			if proxy.connection.backend_ndx == 0 then
				if not is_insert_id then
					if attr == 1 or is_in_lock then
						proxy.connection.backend_ndx = lb.idle_failsafe_rw()	--idle_failsafe_rw定义在balance.lua里，返回第一台池不为空的master机器的序号
					else
					--	local backend_ndx = lb.idle_ro()	--idle_ro定义在balance.lua里，返回当前连接的客户端数量最少的slave机器的序号
						if proxy.global.config.rwsplit.max_weight == -1 then update_rwsplit() end
						local backend_ndx = lb.cycle_read_ro()

						if backend_ndx > 0 then
							proxy.connection.backend_ndx = backend_ndx
						end
					end
				else
					proxy.connection.backend_ndx = lb.idle_failsafe_rw()	--idle_failsafe_rw定义在balance.lua里，返回第一台池不为空的master机器的序号
				--	print("   found a SELECT LAST_INSERT_ID(), staying on the same backend")
				end
			end
		end
	end

	if proxy.connection.backend_ndx == 0 and cmd.type == proxy.COM_INIT_DB then
		if proxy.global.config.rwsplit.max_weight == -1 then update_rwsplit() end
		proxy.connection.backend_ndx = lb.cycle_read_ro()
	end

	-- no backend selected yet, pick a master
	if proxy.connection.backend_ndx == 0 then
		-- we don't have a backend right now
		-- 
		-- let's pick a master as a good default
		--
		proxy.connection.backend_ndx = lb.idle_failsafe_rw()	--idle_failsafe_rw定义在balance.lua里，返回第一台池不为空的master机器的序号
	end

	-- by now we should have a backend
	--
	-- in case the master is down, we have to close the client connections
	-- otherwise we can go on
	if proxy.connection.backend_ndx == 0 then	--所有backend状态都是DOWN导致的情况
        --[[
        for i = 1, #proxy.global.backends do	--global对应chassis_private？
            print("backend[".. i .."] :" .. proxy.global.backends[i].dst.name)
        end
        ]]
		write_log(level.DEBUG, "LEAVE READ_QUERY")
		if cmd.type == proxy.COM_QUERY then write_query(cmd.query, c.src.name) end
		return proxy.PROXY_SEND_QUERY		--为什么返回SEND_QUERY而不是ERROR之类？因为connection.server为空
	end

	local s = proxy.connection.server

	-- if client and server db don't match, adjust the server-side 
	--
	-- skip it if we send a INIT_DB anyway
	if cmd.type == proxy.COM_QUERY then
		if c.default_db then
        --	if not s.default_db or c.default_db ~= s.default_db then
            	proxy.queries:prepend(2, string.char(proxy.COM_INIT_DB) .. c.default_db, { resultset_is_needed = true })	--inj.id设为2
        --	end
        end
        charset.modify_charset(tokens, c, s)
	end

	-- send to master
	if is_debug then
		if proxy.connection.backend_ndx > 0 then
			local b = proxy.global.backends[proxy.connection.backend_ndx]
			print("  sending to backend : " .. b.dst.name);
			print("  server src port : " .. proxy.connection.server.src.port)
			print("    is_slave         : " .. tostring(b.type == proxy.BACKEND_TYPE_RO));
			print("    server default db: " .. s.default_db)
			print("    server username  : " .. s.username)
		end
		print("    in_trans        : " .. tostring(is_in_transaction))
		print("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
		print("    COM_QUERY       : " .. tostring(cmd.type == proxy.COM_QUERY))
	end

	if proxy.connection.backend_ndx > 0 then
		local b = proxy.global.backends[proxy.connection.backend_ndx]
		write_log(level.INFO, "  sending to backend : ", b.dst.name);
		write_log(level.INFO, "  server src port : ", proxy.connection.server.src.port)
		write_log(level.INFO, "    is_slave         : ", tostring(b.type == proxy.BACKEND_TYPE_RO));
		write_log(level.INFO, "    server default db: ", s.default_db)
		write_log(level.INFO, "    server username  : ", s.username)
	end
	write_log(level.INFO, "    in_trans        : ", tostring(is_in_transaction))
	write_log(level.INFO, "    in_calc_found   : ", tostring(is_in_select_calc_found_rows))
	write_log(level.INFO, "    COM_QUERY       : ", tostring(cmd.type == proxy.COM_QUERY))
	write_log(level.DEBUG, "LEAVE READ_QUERY")

	return proxy.PROXY_SEND_QUERY
end

---
-- as long as we are in a transaction keep the connection
-- otherwise release it so another client can use it
function read_query_result( inj ) 
	write_log(level.DEBUG, "ENTER READ_QUERY_RESULT")

	local is_debug = proxy.global.config.rwsplit.is_debug
	local res      = assert(inj.resultset)
	local flags    = res.flags

	local src_name = proxy.connection.client.src.name
	local dst_name = proxy.global.backends[proxy.connection.backend_ndx].dst.name

	if inj.id == 1 then
	--	write_des(0, inj)
	elseif inj.id == 8 then
        if res.query_status == proxy.MYSQLD_PACKET_ERR then
		--	write_sql("response failure: " .. inj.query:sub(2))
			write_sql(inj, src_name, dst_name)
		else
			write_des(0, inj, src_name, dst_name)
		end
	elseif inj.id == 7 then
        if res.query_status == proxy.MYSQLD_PACKET_ERR then
            write_sql(inj, src_name, dst_name)
		else
			write_des(1, inj, src_name, dst_name)
		end
	else
		-- ignore the result of the USE <default_db>
		-- the DB might not exist on the backend, what do do ?
		--
		local re = proxy.PROXY_IGNORE_RESULT
		if inj.id == 2 then
			-- the injected INIT_DB failed as the slave doesn't have this DB
			-- or doesn't have permissions to read from it
			if res.query_status == proxy.MYSQLD_PACKET_ERR then
				proxy.queries:reset()
				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't change DB ".. proxy.connection.client.default_db ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}
				re = proxy.PROXY_SEND_RESULT
			end
		elseif inj.id == 3 then
			if res.query_status == proxy.MYSQLD_PACKET_ERR then
				proxy.queries:reset()
				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't change charset_client " .. proxy.connection.client.charset_client ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}
				re = proxy.PROXY_SEND_RESULT
			end
		elseif inj.id == 4 then
			if res.query_status == proxy.MYSQLD_PACKET_ERR then
				proxy.queries:reset()
				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't change charset_results " .. proxy.connection.client.charset_results ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}
				re = proxy.PROXY_SEND_RESULT
			end
		elseif inj.id == 5 then
			if res.query_status == proxy.MYSQLD_PACKET_ERR then
				proxy.queries:reset()
				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "can't change charset_connection " .. proxy.connection.client.charset_connection ..
						" to on slave " .. proxy.global.backends[proxy.connection.backend_ndx].dst.name
				}
				re = proxy.PROXY_SEND_RESULT
			end
        elseif inj.id == 6 then
            -- merge the field-definition
            local fields = {}
            for n = 1, #inj.resultset.fields do
                    fields[#fields + 1] = {
                            type = inj.resultset.fields[n].type,
                            name = inj.resultset.fields[n].name,
                    }
            end

            -- append the rows to the result-set storage
            if res.query_status == proxy.MYSQLD_PACKET_OK then
                -- get attribute
                merge_res.sortindex, merge_res.sorttype = parser.get_sort(g_tokens, fields)
                merge_res.limit = parser.get_limit(g_tokens)
                --[[
                print(merge_res.sortindex)
                print(merge_res.sorttype)
                print(merge_res.limit)
                ]]
                -- merge rows
                merge_res.rows = split.merge_rows(merge_res.rows, inj.resultset.rows, merge_res.sorttype, merge_res.sortindex, merge_res.limit)
            elseif res.query_status == proxy.MYSQLD_PACKET_ERR then
                write_log(level.ERROR, "response failure: " .. inj.query:sub(2))
            	write_sql(inj, src_name, dst_name)
            end

            -- finished one response
            merge_res.sub_sql_exed = merge_res.sub_sql_exed + 1
			write_des(merge_res.sub_sql_exed, inj, src_name, dst_name)

            -- finished all sub response
            if merge_res.sub_sql_exed >= merge_res.sub_sql_num and #fields > 0 then
                -- generate response struct
				proxy.queries:reset()
                proxy.response = {
                        type = proxy.MYSQLD_PACKET_OK,
                        resultset = {
                                rows = merge_res.rows,
                                fields = fields
                        }
                }

                return proxy.PROXY_SEND_RESULT
            elseif merge_res.sub_sql_exed >= merge_res.sub_sql_num and #fields < 1 then
				proxy.queries:reset()
				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = "Proxy Warning - Query failure"
				}
				return proxy.PROXY_SEND_RESULT
            end
		end

		if re == proxy.PROXY_SEND_RESULT and proxy.response.type == proxy.MYSQLD_PACKET_ERR then 
			write_log(level.ERROR, proxy.response.errmsg)
		end

		write_log(level.DEBUG, "LEAVE READ_QUERY_RESULT")

		return re
	end

	if is_in_transaction == false or flags.in_trans == true then
		is_in_transaction = flags.in_trans
	else
		if inj.query:sub(2, 7):upper() == "COMMIT" or inj.query:sub(2, 9):upper() == "ROLLBACK" then
			is_in_transaction = false
		end
	end

	if is_in_lock then
		if string.match(inj.query:sub(2):upper(), "SELECT RELEASE_LOCK(.*)") then
			is_in_lock = false
		end
	end
--[[
	if is_in_transaction and flags.in_trans == false and inj.query:upper() ~= "ROLLBACK" and inj.query:upper() ~= "COMMIT" then
	--	is_in_transaction = false
	else
		is_in_transaction = flags.in_trans
	end
]]
	local have_last_insert_id = (res.insert_id and (res.insert_id > 0))

	if not is_in_transaction and 
	   not is_in_select_calc_found_rows and
	   not have_last_insert_id and
	   not is_in_lock then
		-- release the backend
		proxy.connection.backend_ndx = 0	--将连接放回连接池
	else
		if is_debug then
			print("(read_query_result) staying on the same backend")
			print("    in_trans        : " .. tostring(is_in_transaction))
			print("    in_calc_found   : " .. tostring(is_in_select_calc_found_rows))
			print("    have_insert_id  : " .. tostring(have_last_insert_id))
		end
		write_log(level.INFO, "(read_query_result) staying on the same backend")
		write_log(level.INFO, "    in_trans        : ", tostring(is_in_transaction))
		write_log(level.INFO, "    in_calc_found   : ", tostring(is_in_select_calc_found_rows))
		write_log(level.INFO, "    have_insert_id  : ", tostring(have_last_insert_id))
	end

	write_log(level.DEBUG, "LEAVE READ_QUERY_RESULT")
end

--- 
-- close the connections if we have enough connections in the pool
--
-- @return nil - close connection 
--         IGNORE_RESULT - store connection in the pool
function disconnect_client()
	write_log(level.DEBUG, "ENTER DISCONNECT_CLIENT")
	local is_debug = proxy.global.config.rwsplit.is_debug
	if is_debug then
		print("[disconnect_client] " .. proxy.connection.client.src.name)
	end
	write_log(level.INFO, "[disconnect_client] ", proxy.connection.client.src.name)
	-- make sure we are disconnection from the connection
	-- to move the connection into the pool

	if is_in_transaction or is_in_lock then
		proxy.connection.backend_ndx = -1
	else
		proxy.connection.backend_ndx = 0
	end

	write_log(level.DEBUG, "LEAVE DISCONNECT_CLIENT")
end
