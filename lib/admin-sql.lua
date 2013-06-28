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
-- map SQL commands to the hidden MySQL Protocol commands
--
-- some protocol commands are only available through the mysqladmin tool like
-- * ping
-- * shutdown
-- * debug
-- * statistics
--
-- ... while others are avaible
-- * process info (SHOW PROCESS LIST)
-- * process kill (KILL <id>)
-- 
-- ... and others are ignored
-- * time
-- 
-- that way we can test MySQL Servers more easily with "mysqltest"
--


--- 
-- recognize special SQL commands and turn them into COM_* sequences
--
function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then return end

	if packet:sub(2) == "COMMIT SUICIDE" then
		proxy.queries:append(proxy.COM_SHUTDOWN, string.char(proxy.COM_SHUTDOWN), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "PING" then
		proxy.queries:append(proxy.COM_PING, string.char(proxy.COM_PING), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "STATISTICS" then
		proxy.queries:append(proxy.COM_STATISTICS, string.char(proxy.COM_STATISTICS), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "PROCINFO" then
		proxy.queries:append(proxy.COM_PROCESS_INFO, string.char(proxy.COM_PROCESS_INFO), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "TIME" then
		proxy.queries:append(proxy.COM_TIME, string.char(proxy.COM_TIME), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "DEBUG" then
		proxy.queries:append(proxy.COM_DEBUG, string.char(proxy.COM_DEBUG), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "PROCKILL" then
		proxy.queries:append(proxy.COM_PROCESS_KILL, string.char(proxy.COM_PROCESS_KILL), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "SETOPT" then
		proxy.queries:append(proxy.COM_SET_OPTION, string.char(proxy.COM_SET_OPTION), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "BINLOGDUMP" then
		proxy.queries:append(proxy.COM_BINLOG_DUMP, string.char(proxy.COM_BINLOG_DUMP), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "BINLOGDUMP1" then
		proxy.queries:append(proxy.COM_BINLOG_DUMP, 
			string.char(proxy.COM_BINLOG_DUMP) ..
			"\004\000\000\000" ..
			"\000\000" ..
			"\002\000\000\000" ..
			"\000" .. 
			""
			, { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "REGSLAVE" then
		proxy.queries:append(proxy.COM_REGISTER_SLAVE, string.char(proxy.COM_REGISTER_SLAVE), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "REGSLAVE1" then
		proxy.queries:append(proxy.COM_REGISTER_SLAVE, 
			string.char(proxy.COM_REGISTER_SLAVE) .. 
			"\001\000\000\000" .. -- server-id
			"\000" .. -- report-host
			"\000" .. -- report-user
			"\000" .. -- report-password ?
			"\001\000" .. -- our port
			"\000\000\000\000" .. -- recovery rank
			"\001\000\000\000" .. -- master id ... what ever that is
			""
			, { resultset_is_needed = true }) 
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "PREP" then
		proxy.queries:append(proxy.COM_STMT_PREPARE, string.char(proxy.COM_STMT_PREPARE), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "PREP1" then
		proxy.queries:append(proxy.COM_STMT_PREPARE, string.char(proxy.COM_STMT_PREPARE) .. "SELECT ?", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "EXEC" then
		proxy.queries:append(proxy.COM_STMT_EXECUTE, string.char(proxy.COM_STMT_EXECUTE), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "EXEC1" then
		proxy.queries:append(proxy.COM_STMT_EXECUTE, 
			string.char(proxy.COM_STMT_EXECUTE) .. 
			"\001\000\000\000" .. -- stmt-id
			"\000" .. -- flags
			"\001\000\000\000" .. -- iteration count
			"\000"             .. -- null-bits
			"\001"             .. -- new-parameters
			"\000\254" ..
			"\004" .. "1234" ..
			"", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "DEAL" then
		proxy.queries:append(proxy.COM_STMT_CLOSE, string.char(proxy.COM_STMT_CLOSE), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "DEAL1" then
		proxy.queries:append(proxy.COM_STMT_CLOSE, string.char(proxy.COM_STMT_CLOSE) .. "\001\000\000\000", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "RESET" then
		proxy.queries:append(proxy.COM_STMT_RESET, string.char(proxy.COM_STMT_RESET), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "RESET1" then
		proxy.queries:append(proxy.COM_STMT_RESET, string.char(proxy.COM_STMT_RESET) .. "\001\000\000\000", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "FETCH" then
		proxy.queries:append(proxy.COM_STMT_FETCH, string.char(proxy.COM_STMT_FETCH), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "FETCH1" then
		proxy.queries:append(proxy.COM_STMT_FETCH, string.char(proxy.COM_STMT_FETCH) .. "\001\000\000\000" .. "\128\000\000\000", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "FLIST" then
		proxy.queries:append(proxy.COM_FIELD_LIST, string.char(proxy.COM_FIELD_LIST), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "FLIST1" then
		proxy.queries:append(proxy.COM_FIELD_LIST, string.char(proxy.COM_FIELD_LIST) .. "t1\000id\000\000\000", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "TDUMP" then
		proxy.queries:append(proxy.COM_TABLE_DUMP, string.char(proxy.COM_TABLE_DUMP), { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "TDUMP1" then
		proxy.queries:append(proxy.COM_TABLE_DUMP, string.char(proxy.COM_TABLE_DUMP) .. "\004test\002t1", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	elseif packet:sub(2) == "TDUMP2" then
		proxy.queries:append(proxy.COM_TABLE_DUMP, string.char(proxy.COM_TABLE_DUMP) .. "\004test\002t2", { resultset_is_needed = true })
		return proxy.PROXY_SEND_QUERY
	end
end


---
-- adjust the response to match the needs of COM_QUERY 
-- where neccesary
--
-- * some commands return EOF (COM_SHUTDOWN), 
-- * some are plain-text (COM_STATISTICS)
--
-- in the end the client sent us a COM_QUERY and we have to hide
-- all those specifics
function read_query_result(inj)

	if inj.id == proxy.COM_SHUTDOWN or
	   inj.id == proxy.COM_SET_OPTION or
	   inj.id == proxy.COM_BINLOG_DUMP or
	   inj.id == proxy.COM_STMT_PREPARE or
	   inj.id == proxy.COM_STMT_FETCH or
	   inj.id == proxy.COM_FIELD_LIST or
	   inj.id == proxy.COM_TABLE_DUMP or
	   inj.id == proxy.COM_DEBUG then
		-- translate the EOF packet from the COM_SHUTDOWN into a OK packet
		-- to match the needs of the COM_QUERY we got
		if inj.resultset.raw:byte() ~= 255 then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
			}
			return proxy.PROXY_SEND_RESULT
		end
	elseif inj.id == proxy.COM_PING or
	       inj.id == proxy.COM_TIME or
	       inj.id == proxy.COM_PROCESS_KILL or
	       inj.id == proxy.COM_REGISTER_SLAVE or
	       inj.id == proxy.COM_STMT_EXECUTE or
	       inj.id == proxy.COM_STMT_RESET or
	       inj.id == proxy.COM_PROCESS_INFO then
		-- no change needed
	elseif inj.id == proxy.COM_STATISTICS then
		-- the response a human readable plain-text
		--
		-- just turn it into a proper result-set
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "statisitics" }
				},
				rows = {
					{ inj.resultset.raw }
				}
			}
		}
		return proxy.PROXY_SEND_RESULT

	else
		-- we don't know them yet, just return ERR to the client to
		-- match the needs of COM_QUERY
		print(("got: %q"):format(inj.resultset.raw))
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
		}
		return proxy.PROXY_SEND_RESULT
	end
end

