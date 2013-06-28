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


---
--[[
   Uses MySQL-Proxy to create and execute a cross tabulation query
  
   Using this script, you can request commands like
  
     XTAB HELP
     XTAB VERSION
     XTAB table_name row_header col_header operation operation_fld
  
     and get the result in tabular format, from any MySQL client
  
   Written by Giuseppe Maxia, QA Developer, MySQL AB
--]]

assert(proxy.PROXY_VERSION >= 0x00600,
  	"you need at least mysql-proxy 0.6.0 to run this module")

--[[
   If the environmental variable 'DEBUG' is set, then
   the proxy will print diagnostic messages
--]]
local DEBUG = os.getenv('DEBUG') or os.getenv('VERBOSE')  or 0
DEBUG = DEBUG + 0

local xtab_version = '0.1.3'

local tokenizer = require("proxy.tokenizer")

--[[
   error status for the xtab sequence
   if an error happens in a query before the last one,
   all results after it are ignored
--]]
local xtab_error_status = 0
local return_xtab_query = false

local xtab_help_messages = {
    { 'xtab - version ' .. xtab_version .. ' - (C) MySQL AB 2007' },
    { 'Syntax: ' }, 
    { ' - ' },
    { 'XTAB table_name row_header col_header operation operation_fld [summary]' }, 
    { '"table_name" can be a table or a view' },
    { '"row_field" is the field to be used as row header' },
    { '"col_field" is the field whose distinct values will become column headers' },
    { '"operation" is the required operation (COUNT|SUM|AVG|MAX|MIN)' },
    { '"operation_field" is the field to which the operation is applied' },
    { ' - ' },
    { 'If the "summary" option is used, then a "WITH ROLLUP" clause ' },
    { 'is added to the query.' },
    { ' - ' },
    { 'Other commands:' },
    { 'XTAB QUERY - the XTAB query is returned instead of its result' },
    { 'XTAB NOQUERY - the XTAB result is returned (default)' },
    { 'XTAB version - shows current version' },
    { 'XTAB help - shows this help' },
    { 'Created by Giuseppe Maxia' },
}

local allowed_operators = {'count', 'sum', 'avg', 'min', 'max' }

--
-- Result with the syntax help
--
local xtab_help_resultset = { 
    fields = {
        { type = proxy.MYSQL_TYPE_STRING, name = 'XTAB help'},
    },
    rows   = xtab_help_messages       
}

--
-- Result with the XTAB version
--
local xtab_version_resultset = {
    fields = {  
        { type = proxy.MYSQL_TYPE_STRING, name = 'XTAB version'} 
    },
    rows = {
        { xtab_version }
    }
}

-- 
-- Result to comment on XTAB QUERY command
--
local xtab_query_resultset = {
    fields = {  
        { type = proxy.MYSQL_TYPE_STRING, name = 'XTAB query '} 
    },
    rows = {
        { 'Setting XTAB QUERY, the next XTAB command will return ' },
        { 'the query text instead of its result.' },
        { '' },
        { 'Setting XTAB NOQUERY (default), the XTAB command' },
        { 'executes the query and returns its result.' },
        { '' },
    }
}

--
-- result returned on wrong XTAB option
--
local xtab_unknown_resultset = {
    fields = {  
        { type = proxy.MYSQL_TYPE_STRING, name = 'XTAB ERROR'} 
    },
    rows = {
        { 'unknown command. Enter "XTAB HELP" for help' }
    }
}

-- 
-- result returned on wrong operator
--
local xtab_unknown_operator = {
    fields = {  
        { type = proxy.MYSQL_TYPE_STRING, name = 'XTAB ERROR'} 
    },
    rows = {
        { 'unknown operator.' },
        { 'Accepted operators: COUNT, SUM, AVG, MIN, MAX' },
        { 'Enter "XTAB HELP" for help' },
    }
}

--
-- xtab parameters to be passed from read_query ro read_query_result 
--
local xtab_params = {}

--[[
  Injection codes to recognize the various queries
  composing the xtab operation
--]]
local xtab_id_before = 1024
local xtab_id_start  = 2048
local xtab_id_exec   = 4096


--[[ 
   TODO
   add XTAB VERBOSE, to enable debugging from SQL CLI
   handling errors related to missing xtab_params values
--]]

function read_query( packet )
    if packet:byte() ~= proxy.COM_QUERY then
        return
    end

    --[[
       To be on the safe side, we clean the params that may trigger 
       behavior on read_query_result
    --]]
    xtab_params = {}
    xtab_error_status = 0

    local query = packet:sub(2)
    --
    -- simple tokeninzing the query, looking for accepted pattern
    --
    local option, table_name, row_field, col_field , op, op_col , summary
    local query_tokens = tokenizer.tokenize(query)
    local START_TOKEN = 0

    if  ( query_tokens[1]['text']:lower() == 'xtab' )
    then
        START_TOKEN = 1
        option = query_tokens[2]['text']
    elseif ( query_tokens[1]['text']:lower() == 'select' 
          and  
          query_tokens[2]['text']:lower() == 'xtab' ) 
    then 
        START_TOKEN = 2
        option = query_tokens[3]['text']
    else
        return 
    end

    --[[ 
       First, checking for short patterns
       XTAB HELP
       XTAB VERSION
    --]]
    print_debug('received query ' .. query)
    if query_tokens[ START_TOKEN + 2 ] == nil then
        if (option:lower() == 'help') then
            proxy.response.resultset = xtab_help_resultset 
        elseif option:lower() == 'version' then
            proxy.response.resultset = xtab_version_resultset 
        elseif option:lower() == 'query' then
            xtab_query_resultset.rows[7] = { 'Current setting: returns a query' }
            proxy.response.resultset = xtab_query_resultset
            return_xtab_query = true
        elseif option:lower() == 'noquery' then
            xtab_query_resultset.rows[7] = { 'Current setting: returns a result set' }
            proxy.response.resultset = xtab_query_resultset
            return_xtab_query = false
        else
            proxy.response.resultset = xtab_unknown_resultset 
        end
        proxy.response.type = proxy.MYSQLD_PACKET_OK
        return proxy.PROXY_SEND_RESULT
    end

    -- 
    -- parsing the query for a xtab recognized command
    --
    table_name = option
    row_field  = query_tokens[START_TOKEN + 2 ]['text']
    col_field  = query_tokens[START_TOKEN + 3 ]['text']
    op         = query_tokens[START_TOKEN + 4 ]['text']
    op_col     = query_tokens[START_TOKEN + 5 ]['text']
    if (query_tokens[START_TOKEN + 6 ] ) then
        summary    = query_tokens[START_TOKEN + 6 ]['text']
    else
        summary = ''
    end
    if op_col then
        print_debug (string.format("<xtab> <%s> (%s) (%s) [%s] [%s]",
            table_name, row_field, col_field, op, op_col ))
    else
        return nil
    end
    
    --[[
       At this point, at least in all appearance, we are dealing
       with a full XTAB command
       
       Now checking for recognized operators
      ]]
    local recognized_operator = 0
    for i,v in pairs(allowed_operators) do
        if string.lower(op) == v then
            recognized_operator = 1
        end
    end
    
    if recognized_operator == 0 then
        print_debug('unknown operator ' .. op)
        proxy.response.type = proxy.MYSQLD_PACKET_OK
        proxy.response.resultset = xtab_unknown_operator 
        return proxy.PROXY_SEND_RESULT
    end

    --[[
       records the xtab parameters for further usage
       in the read_query_result function
    --]]
    xtab_params['table_name'] = table_name
    xtab_params['row_header'] = row_field
    xtab_params['col_header'] = col_field
    xtab_params['operation']  = op
    xtab_params['op_col']     = op_col
    xtab_params['summary']    = summary:lower() == 'summary'

    print_debug('summary: ' .. tostring(xtab_params['summary']))

    --[[
       Making sure that group_concat is large enough.
       The result of this query will be ignored
    --]]
    proxy.queries:append(xtab_id_before, 
        string.char(proxy.COM_QUERY) .. 
        "set group_concat_max_len = 1024*1024",
	{ resultset_is_needed = true })

    --[[
       If further queries need to be executed before the 
       one that gets the distinct values for columns,
       use an ID larger than xtab_id_before and smaller than
       xtab_id_start
    --]]

    --[[
       Getting all distinct values for the given column.
       This query will be used to create the final xtab query
       in read_query_result()
    --]]
    proxy.queries:append(xtab_id_start, 
        string.char(proxy.COM_QUERY) .. 
        string.format([[
          select group_concat( distinct concat(
            '%s(if( `%s`= ', quote(%s),',`%s`,null)) as `%s_',%s,'`' ) 
             order by `%s` ) from `%s` order by `%s`]],
            op, 
            col_field, 
            col_field, 
            op_col, 
            col_field, 
            col_field,
            col_field, 
            table_name, 
            col_field
        ),
	{ resultset_is_needed = true }
    )
    return proxy.PROXY_SEND_QUERY
end

function read_query_result(inj)
    print_debug ( 'injection id ' ..  inj.id ..  
        ' error status: ' .. xtab_error_status)
    if xtab_error_status > 0 then
        print_debug('ignoring resultset ' .. inj.id .. 
            ' for previous error')
        return proxy.PROXY_IGNORE_RESULT
    end
    local res = assert(inj.resultset)
    -- 
    -- on error, empty the query queue and return the error message
    --
    if res.query_status and (res.query_status < 0 ) then
        xtab_error_status = 1
        print_debug('sending result' .. inj.id .. ' on error ')
        proxy.queries:reset()
        return 
    end 

    --
    -- ignoring the preparatory queries
    --
    if (inj.id >= xtab_id_before) and (inj.id < xtab_id_start) then
        print_debug ('ignoring preparatory query from xtab ' .. inj.id )
        return proxy.PROXY_IGNORE_RESULT
    end 
 
    --
    -- creating the XTAB query
    --
    if (inj.id == xtab_id_start) then
        print_debug ('getting columns resultset from xtab ' .. inj.id )
        local col_query = ''
        
        for row in inj.resultset.rows do
            col_query = col_query .. row[1]
        end
        
        print_debug ('column values : ' .. col_query)
        col_query = col_query:gsub(
            ',' .. xtab_params['operation'], '\n, ' 
                .. xtab_params['operation']) 
        local xtab_query = string.format([[
          SELECT 
            %s ,
            %s 
            , %s(`%s`) AS total  
          FROM %s 
          GROUP BY %s
        ]],
            xtab_params['row_header'], 
            col_query,
            xtab_params['operation'], 
            xtab_params['op_col'], 
            xtab_params['table_name'], 
            xtab_params['row_header']
        )
        if xtab_params['summary'] == true then
            xtab_query = xtab_query .. ' WITH ROLLUP '
        end
        --
        -- if the query was requested, it is returned immediately
        --
        if (return_xtab_query == true) then
            proxy.queries:reset()
            proxy.response.type = proxy.MYSQLD_PACKET_OK
            proxy.response.resultset = {
                fields = {
                    { type = proxy.MYSQL_TYPE_STRING, name = 'XTAB query'}
                },
                rows = {  
                    { xtab_query }
                }
            }
            return proxy.PROXY_SEND_RESULT
        end
        --
        -- The XTAB query is executed
        --
        proxy.queries:append(xtab_id_exec, string.char(proxy.COM_QUERY) .. xtab_query,
            { resultset_is_needed = true })
        print_debug (xtab_query, 2)
        return proxy.PROXY_IGNORE_RESULT
    end 

    -- 
    -- Getting the final xtab result
    --
    if (inj.id == xtab_id_exec) then
        print_debug ('getting final xtab result ' .. inj.id )
        -- 
        -- Replacing the default NULL value provided by WITH ROLLUP
        -- with a more human readable value
        --
        if xtab_params['summary'] == true then
            local updated_rows = {}
            local updated_fields = {}
            for row in inj.resultset.rows do
                if row[1] == nil then
                    row[1] = 'Grand Total'
                end
                table.insert(updated_rows , row)
            end
            local field_count = 1
            local fields = inj.resultset.fields

            while fields[field_count] do
                table.insert(updated_fields, { 
                    type = fields[field_count].type , 
                    name = fields[field_count].name } )
                    field_count = field_count + 1
            end
            proxy.response.resultset = {
                fields = updated_fields,    
                rows = updated_rows
            }
            proxy.response.type = proxy.MYSQLD_PACKET_OK
            return proxy.PROXY_SEND_RESULT
        end
        return
    end 
end

function print_debug (msg, min_level)
    if not min_level then
        min_level = 1
    end
    if DEBUG and (DEBUG >= min_level) then
        print(msg)
    end
end
