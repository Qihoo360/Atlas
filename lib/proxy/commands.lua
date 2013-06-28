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

module("proxy.commands", package.seeall)

---
-- map the constants to strings 
-- lua starts at 1
local command_names = {
	"COM_SLEEP",
	"COM_QUIT",
	"COM_INIT_DB",
	"COM_QUERY",
	"COM_FIELD_LIST",
	"COM_CREATE_DB",
	"COM_DROP_DB",
	"COM_REFRESH",
	"COM_SHUTDOWN",
	"COM_STATISTICS",
	"COM_PROCESS_INFO",
	"COM_CONNECT",
	"COM_PROCESS_KILL",
	"COM_DEBUG",
	"COM_PING",
	"COM_TIME",
	"COM_DELAYED_INSERT",
	"COM_CHANGE_USER",
	"COM_BINLOG_DUMP",
	"COM_TABLE_DUMP",
	"COM_CONNECT_OUT",
	"COM_REGISTER_SLAVE",
	"COM_STMT_PREPARE",
	"COM_STMT_EXECUTE",
	"COM_STMT_SEND_LONG_DATA",
	"COM_STMT_CLOSE",
	"COM_STMT_RESET",
	"COM_SET_OPTION",
	"COM_STMT_FETCH",
	"COM_DAEMON"
}

---
-- split a MySQL command packet into its parts
--
-- @param packet a network packet
-- @return a table with .type, .type_name and command specific fields 
function parse(packet)
	local cmd = {}

	cmd.type = packet:byte()
	cmd.type_name = command_names[cmd.type + 1]
	
	if cmd.type == proxy.COM_QUERY then
		cmd.query = packet:sub(2)
	elseif cmd.type == proxy.COM_QUIT or
	       cmd.type == proxy.COM_PING or
	       cmd.type == proxy.COM_SHUTDOWN then
		-- nothing to decode
	elseif cmd.type == proxy.COM_STMT_PREPARE then
		cmd.query = packet:sub(2)
	-- the stmt_handler_id is at the same position for both STMT_EXECUTE and STMT_CLOSE
	elseif cmd.type == proxy.COM_STMT_EXECUTE or cmd.type == proxy.COM_STMT_CLOSE then
		cmd.stmt_handler_id = string.byte(packet, 2) + (string.byte(packet, 3) * 256) + (string.byte(packet, 4) * 256 * 256) + (string.byte(packet, 5) * 256 * 256 * 256)
	elseif cmd.type == proxy.COM_FIELD_LIST then
		cmd.table = packet:sub(2)
	elseif cmd.type == proxy.COM_INIT_DB or
	       cmd.type == proxy.COM_CREATE_DB or
	       cmd.type == proxy.COM_DROP_DB then
		cmd.schema = packet:sub(2)
	elseif cmd.type == proxy.COM_SET_OPTION then
		cmd.option = packet:sub(2)
	else
		print("[debug] (command) unhandled type name:" .. tostring(cmd.type_name) .. " byte:" .. tostring(cmd.type))
	end

	return cmd
end

function pretty_print(cmd)
	if cmd.type == proxy.COM_QUERY or
	   cmd.type == proxy.COM_STMT_PREPARE then
		return ("[%s] %s"):format(cmd.type_name, cmd.query)
	elseif cmd.type == proxy.COM_INIT_DB then
		return ("[%s] %s"):format(cmd.type_name, cmd.schema)
	elseif cmd.type == proxy.COM_QUIT or
	       cmd.type == proxy.COM_PING or
	       cmd.type == proxy.COM_SHUTDOWN then
		return ("[%s]"):format(cmd.type_name)
	elseif cmd.type == proxy.COM_FIELD_LIST then
		-- should have a table-name
		return ("[%s]"):format(cmd.type_name)
	elseif cmd.type == proxy.COM_STMT_EXECUTE then
		return ("[%s] %s"):format(cmd.type_name, cmd.stmt_handler_id)
	end

	return ("[%s] ... no idea"):format(cmd.type_name)
end

