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
--[[

   

--]]

-- 
-- Debug
--

---
-- read_query() gets the client query before it reaches the server
--
-- we want to inject the query be able to dump its content in the 
-- read_query_result() call
--
-- @see read_query_result
function read_query( packet )
  proxy.queries:append(1, packet, {resultset_is_needed = true } )

  return proxy.PROXY_SEND_QUERY
end


--- dump the result-set to stdout
--
-- @param inj "packet.injection"
local function dump_query_result( inj )
  local field_count = 0
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

--- dump the connection data at startup
-- 
local function dump_connection() 
  print(".== connection ")
 
  cur_backend_ndx = assert(proxy.connection["backend_ndx"])
  cur_backend = proxy.global.backends[cur_backend_ndx]
  
  print("| backend[ndx] = "      .. cur_backend_ndx)
  print("| connected_clients = " .. cur_backend["connected_clients"])
  print("| address = "           .. cur_backend["address"])
  print("| server-version = "    .. proxy.connection.server["mysqld_version"])
  print("| default-db = "     .. proxy.connection.server["default_db"])
  print("| thread-id = "      .. proxy.connection.server["thread_id"])
  print("'== ")
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

local prepared_queries = {}

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

---
-- read_query_result() is called when we receive a query result 
-- from the server
--
-- we try to dump everything we know about this query
-- * the query
-- * the exec-time
-- * the result-set
-- * the query-status
--
-- 
function read_query_result( inj )
  -- the query

  -- the result-set
  local res      = assert(inj.resultset)
  local packet   = assert(inj.query)
  local raw_len  = assert(res.raw):len()

  local flags    = res.flags

  dump_connection()
 
  print(".---  mysql query")
  decode_query_packet(packet)
  print("|")
  print("| result.len = " .. raw_len)
  print("| result.packet =" .. str2hex(res.raw))
  print("| result.flags = { in_trans = " .. tostring(flags.in_trans) .. ", " ..
               "auto_commit = " .. tostring(flags.auto_commit) .. ", " ..
               "no_good_index_used = " ..tostring( flags.no_good_index_used ) .. ", " ..
               "no_index_used = " .. tostring(flags.no_index_used) .. " }")
  print("| result.warning_count = " .. res.warning_count)
  if res.affected_rows then
    print("| result.affected_rows = " .. res.affected_rows)
    print("| result.insert_id = " .. res.insert_id)
  end
  if res.query_status then
    print("| result.query_status = " .. res.query_status)
  end

  print("| query_time = " .. inj.query_time .. "us")
  print("| response_time = " .. inj.response_time .. "us")

  if res.query_status == proxy.MYSQLD_PACKET_ERR then
    print("| result.err.code = " .. res.raw:byte(2) + (res.raw:byte(3) * 256))
    print("| result.err.sql_state = " .. string.format("%q", res.raw:sub(5, 9)))
    print("| result.err.msg = " .. string.format("%q", res.raw:sub(10)))
  else
    print("| .--- result-set")
    print("| | command = " .. command_names[string.byte(packet) + 1])
  
    if string.byte(packet) == proxy.COM_STMT_PREPARE then
      assert(string.byte(res.raw, 1) == 0, string.format("packet[0] should be 0, is %02x", string.byte(res.raw, 1)))
      local stmt_handler_id = string.byte(res.raw, 2) + (string.byte(res.raw, 3) * 256) + (string.byte(res.raw, 4) * 256 * 256) + (string.byte(res.raw, 5) * 256 * 256 * 256)
      local num_cols = string.byte(res.raw, 6) + (string.byte(res.raw, 7) * 256)
      local num_params = string.byte(res.raw, 8) + (string.byte(res.raw, 9) * 256)
  
      print("| | stmt-id = " .. stmt_handler_id )
      print("| | num-cols = " .. num_cols )
      print("| | num-params = " .. num_params )
  
      if raw_len >= 12 then
        local num_params = string.byte(res.raw, 11) + (string.byte(res.raw, 12) * 256)
        print("| | (5.0) warning-count = " .. num_params )
      end
  
      -- track the prepared query
  
      prepared_queries[stmt_handler_id] = string.sub(packet, 2)
    elseif string.byte(packet) == proxy.COM_STMT_EXECUTE or 
           string.byte(packet) == proxy.COM_QUERY then
      local num_cols = string.byte(res.raw, 1)
      print("| | num-cols = " .. num_cols)
      if num_cols > 0 and num_cols < 255 then
        dump_query_result(inj)
      end
    else
      print("| | client-packet =" .. str2hex(res.raw))
    end
    print("| '---")
  end
  print("'---")
  -- end
end


