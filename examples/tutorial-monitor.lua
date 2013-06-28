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

   

--]]
local function str2hex(str)
  local raw_len = string.len(str)
  local i = 1
  local o = ""
  while i <= raw_len do
    o = o .. string.format(" %02x", string.byte(str, i))
    i = i + 1
  end

  return o
end

--
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

--- dump the result-set to stdout
--
-- @param inj "packet.injection"
local function dump_query_result( inj )
  local field_count = 1
  local fields = inj.resultset.fields 

  while fields[field_count] do
    local field = fields[field_count]
    print("| | field[" .. field_count .. "] = { type = " .. 
            field.type .. ", name = " .. field.name .. " }" )
    field_count = field_count + 1
  end

  local row_count = 0
  for row in inj.resultset.rows do
    local cols = {}
    local o
    for i = 1, field_count do
      if not o then
        o = ""
      else 
        o = o .. ", "
      end
      if not row[i] then
        o = o .. "(nul)"
      else
        o = o .. row[i]
      end
    end
    print("| | row["..row_count.."] = { " .. o .. " }")
    row_count = row_count + 1
  end
end

local function decode_query_packet( packet )
  -- we don't have the packet header in the 
  packet_len = string.len(packet)
  

  print("| query.len = " .. packet_len)
  print("| query.packet =" .. str2hex(packet))
  -- print("(decode_query) " .. "| packet-id = " .. "(unknown)")
  
  print("| .--- query")
  print("| | command = " .. command_names[string.byte(packet) + 1])
  if string.byte(packet) == proxy.COM_QUERY then
    -- after the COM_QUERY comes the query
    print("| | query = " .. string.format("%q", string.sub(packet, 2)))
    
  elseif string.byte(packet) == proxy.COM_INIT_DB then
    print("| | db = " .. string.format("%q", string.sub(packet, 2)))
    
  elseif string.byte(packet) == proxy.COM_STMT_PREPARE then
    print("| | query = " .. string.format("%q", string.sub(packet, 2)))
    
  elseif string.byte(packet) == proxy.COM_STMT_EXECUTE then
    local stmt_handler_id = string.byte(packet, 2) + (string.byte(packet, 3) * 256) + (string.byte(packet, 4) * 256 * 256) + (string.byte(packet, 5) * 256 * 256 * 256)
    local flags = string.byte(packet, 6)
    local iteration_count = string.byte(packet, 7) + (string.byte(packet, 8) * 256) + (string.byte(packet, 9) * 256 * 256) + (string.byte(packet, 10) * 256 * 256 * 256)

    print("| | stmt-id = " .. stmt_handler_id )
    print("| | flags = " .. string.format("%02x", flags) )
    print("| | iteration_count = " .. iteration_count )
    
    if packet_len > 10 then
      -- if we don't have any place-holders, no for NUL and friends
      local nul_bitmap = string.byte(packet, 11)
      local new_param  = string.byte(packet, 12)

      print("| | nul_bitmap = " .. string.format("%02x", nul_bitmap ))
      print("| | new_param = " .. new_param )
    else
      print("| | (no params)")
    end
    
    print("| | prepared-query = " .. prepared_queries[stmt_handler_id] )
  else
    print("| | packet =" .. str2hex(packet))
  end
  print("| '---")
end


function read_query()

--[[	for i = 1, #proxy.global.backends do
		local s = proxy.global.backends[i]
		local pool     = s.pool -- we don't have a username yet, try to find a connections which is idling
		local cur_idle = pool.users[""].cur_idle_connections

        print("  [".. i .."].connected_clients = " .. s.connected_clients)
        print("  [".. i .."].idling_connections = " .. cur_idle)
        print("  [".. i .."].type = " .. s.type)
        print("  [".. i .."].state = " .. s.state)
    end
--]]

	proxy.queries:append(1, string.char(proxy.COM_QUERY) .. "SELECT NOW()", { resultset_is_needed = true } )
    if proxy.connection then
        print ("inject monitor query into backend # " .. proxy.connection.backend_ndx)
    else
        print ("inject monitor query")
    end
end

function read_query_result ( inj )

	local res      = assert(inj.resultset)
	local packet   = assert(inj.query)
 
	decode_query_packet(packet)

	print "read query result, dumping"
	dump_query_result(inj)
end
