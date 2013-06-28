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
	(Not so) simple example of a run-time module loader for MySQL Proxy
	
	Usage: 
	1. load this module in the Proxy.
	
	2. From a client, run the command 
	   PLOAD name_of_script
	   and you can use the features implemented in the new script
	   immediately.
	
	3. Successive calls to PLOAD will load a new one.
	
	4. Previous scripts will still be active, and the loader will
	use all of them in sequence to see if one handles the 
	request

	5. CAVEAT. If your script has a read_query function that 
	*always* returns a non null value (e.g. a logging feature)
	then other modules, although loaded, are not used.
	If you have any such modules, you must load them at the
	end of the sequence.

	6. to remove a module, use
	UNLOAD module_name

	IMPORTANT NOTICE:
	the proxy will try to load the file in the directory where
	the proxy (NOT THE CLIENT) was started.
	To use modules not in such directory, you need to
	specify an absolute path.
--]]

local VERSION = '0.1.3'

local DEBUG = os.getenv('DEBUG') or 0
DEBUG = DEBUG + 0

---
-- print_debug()
-- conditionally print a message
--
function proxy.global.print_debug (msg)
	if DEBUG > 0 then
		print(msg)
	end
end

---
-- Handlers for MySQL Proxy hooks
-- Initially, they are void.
-- If the loaded module has implemented any
-- functions, they are associated with these handlers
--
if proxy.global.loaded_handlers == nil then
	proxy.global.loaded_handlers = {
		rq   = {},
		rqr  = {},
		dc   = {},
		cs   = {},
		rh   = {},
		ra   = {},
		rar  = {},
	}
end

---
-- list of functions loaded from user modules
--
if proxy.global.handled_function == nil then
	proxy.global.handled_functions = {
		rq  = 'read_query',
		rqr = 'read_query_result',
		cs  = 'connect_server',
		rh  = 'read_handshake',
		ra  = 'read_auth',
		rar = 'read_auth_result',
		dc  = 'disconnect_client',
	}
end

if proxy.global.handler_status == nil then
	proxy.global.handler_status = {}
end

local funcs_to_create = {
	cs  = 0,
	ra  = 1,
	rar = 1,
	rh  = 1,
	dc  = 0,
}

---
-- creates the hooks for Proxy handled functions
--
for id, has_param in pairs(funcs_to_create) do
	local parameter = has_param == 1 and 'myarg' or ''
	local fstr = string.format(
[[
	function %s(%s)
	if #proxy.global.loaded_handlers['%s'] then
		for i, h in pairs(proxy.global.loaded_handlers['%s'])
		do
			if h then
				proxy.global.print_debug ( 'handling "%s" using ' .. h.name)
				local result = h.handler(%s)
				if result then
					return result
				end
			end
		end
	end
	-- return
end
]],
	proxy.global.handled_functions[id], 
	parameter,
	id, id, proxy.global.handled_functions[id], parameter
)
	proxy.global.print_debug ('creating function ' .. proxy.global.handled_functions[id])
	assert(loadstring(fstr))()
end

---
-- an error string to describe the error occurring
--
local load_multi_error_str = ''

---
-- set_error()
--
-- sets the error string 
--
-- @param msg the message to be assigned to the error string
--
function set_error(msg)
	load_multi_error_str = msg
	proxy.global.print_debug (msg)
	return nil
end

---
-- file_exists()
--
-- checks if a file exists
--
-- @param fname the file name
--
function file_exists(fname)
	local fh=io.open( fname, 'r')
	if fh then 
		fh:close()
		return true
	else
		return false
	end
end

local module_counter = 0

---
--  get_module()
--  
--  This function scans an existing lua file, and turns it into
--  a closure exporting two function handlers, one for
--  read_query() and one for read_query_result().
--  If the input script does not contain the above functions,
--  get_module fails.
--
--	@param module_name the name of the existing script
--
function get_module(module_name)
	-- 
	-- assumes success
	--
	load_multi_error_str = ''

	--
	-- the module is copied to a temporary file
	-- on a given directory
	--
	module_counter = module_counter + 1
	local new_module = 'tmpmodule' .. module_counter
	local tmp_dir	= '/tmp/'
	local new_filename = tmp_dir .. new_module .. '.lua'
	local source_script = module_name
	if not source_script:match('.lua$') then
		source_script = source_script .. '.lua'
	end
	-- 
	-- if the new module does not exist
	-- an error is returned
	--
	if not file_exists(source_script) then
		set_error('file not found ' .. source_script)
		return
	end
	--
	-- if the module directory is not on the search path,
	-- we need to add it
	--
	if not package.path:match(tmp_dir) then
		package.path = tmp_dir .. '?.lua;' .. package.path
	end
	--
	-- Make sure that the module is not loaded.
	-- If we don't remove it from the list of loaded modules,
	-- subsequent load attempts will silently fail
	--
	package.loaded[new_module] = nil 
	local ofh = io.open(new_filename, 'w')
	--
	-- Writing to the new package, starting with the
	-- header and the wrapper function
	--
	ofh:write( string.format(
				 "module('%s', package.seeall)\n"
			  .. "function make_funcs()\n" , new_module)
	)
	local found_funcs = {}
	--
	-- Copying contents from the original script
	-- to the new module, and checking for the existence
	-- of the handler functions
	--
	for line in io.lines(source_script) do
		ofh:write(line .. "\n")
		for i,v in pairs(proxy.global.handled_functions) do
	        if line:match('^%s*function%s+' ..v .. '%s*%(') then
	            found_funcs[i] = v
	                break
			end
		end
	end
	--
	-- closing the wrapper on the new module
	--
	local return_value = ''
	for i,v in pairs(found_funcs) do
		return_value = return_value .. i .. ' = ' .. v .. ', '
	end
	ofh:write(
		   'return { ' ..  return_value ..  '}\n' .. 
		'end\n'
	)
	ofh:close()
	--
	-- Final check. If the handlers were not found, the load fails
	--
	--
	if (found_one == false ) then
		set_error('script ' .. source_script .. 
			' does not contain a proxy handled function')
		return
	end
	--
	-- All set. The new module is loaded
	-- with a new function that will return the handlers
	--
	local result = require(new_module)
	os.remove(new_filename)
	return result
end

---
-- simple_dataset()
--
-- returns a dataset made of a header and a message
--
-- @param header the column name
-- @param message the contents of the column
function proxy.global.simple_dataset (header, message) 
	proxy.response.type = proxy.MYSQLD_PACKET_OK
	proxy.response.resultset = {
		fields = {{type = proxy.MYSQL_TYPE_STRING, name = header}},
		rows = { { message} }
	}
	return proxy.PROXY_SEND_RESULT
end

---
-- make_regexp_from_command()
--
-- creates a regular expression for fast scanning of the command
-- 
-- @param cmd the command to be converted to regexp
--
function proxy.global.make_regexp_from_command(cmd, options)
	local regexp= '^%s*';
	for ch in cmd:gmatch('(.)') do
		regexp = regexp .. '[' .. ch:upper() .. ch:lower() .. ']' 
	end
	if options and options.capture then
	    regexp = regexp  .. '%s+(%S+)'
	end
	return regexp
end

-- 
-- The default command for loading a new module is PLOAD
-- You may change it through an environment variable
--
local proxy_load_command    = os.getenv('PLOAD')  or 'pload'
local proxy_unload_command  = os.getenv('PUNLOAD')  or 'punload'
local proxy_help_command    = os.getenv('PLOAD_HELP')  or 'pload_help'
local currently_using       = 0

local pload_regexp          = proxy.global.make_regexp_from_command(proxy_load_command, {capture = 1})
local punload_regexp        = proxy.global.make_regexp_from_command(proxy_unload_command,{ capture = 1} )
local pload_help_regexp     = proxy.global.make_regexp_from_command(proxy_help_command,{ capture = nil} )
local pload_help_dataset    = {
	{proxy_load_command   .. ' module_name',   'loads a given module'},
	{proxy_unload_command .. 'module_name',    'unloads and existing module'},
	{proxy_help_command,    'shows this help'},
}

---
-- removes a module from the loaded list
--
function remove_module (module_name)
	local found_module = false
    local to_delete = { loaded = {}, status = {} }
	for i,lmodule in pairs(proxy.global.handler_status) do
		if i == module_name then
			found_module = true
            local counter = 0
			for j,h in pairs(lmodule) do
                -- proxy.global.print_debug('removing '.. module_name .. ' (' .. i ..') ' .. h.id .. ' -> ' .. h.ndx )
                to_delete['loaded'][h.id] = h.ndx
                counter = counter + 1
                to_delete['status'][i] = counter
			end
		end
	end
    for i,v in pairs (to_delete['loaded']) do
        table.remove(proxy.global.loaded_handlers[i], v)
    end
    for i,v in pairs (to_delete['status']) do
        table.remove(proxy.global.handler_status[i], v)
    end
	if found_module == false then
		return proxy.global.simple_dataset(module_name, 'NOT FOUND')
	end
	return proxy.global.simple_dataset(module_name, 'unloaded')
end

---
-- creates a dataset from a list of header names
-- and a list of rows
-- @param header a list of field names
-- @param dataset a list of row contents 
function proxy.global.make_dataset (header, dataset) 
	proxy.response.type = proxy.MYSQLD_PACKET_OK

	proxy.response.resultset = {
		fields = {},
		rows = {}
	}
	for i,v in pairs (header) do
		table.insert(proxy.response.resultset.fields, {type = proxy.MYSQL_TYPE_STRING, name = v})
	end
	for i,v in pairs (dataset) do
		table.insert(proxy.response.resultset.rows, v )
	end
	return proxy.PROXY_SEND_RESULT
end

--
-- This function is called at each query.
-- The request for loading a new script is handled here
--
function read_query (packet)
	currently_using = 0
	if packet:byte() ~= proxy.COM_QUERY then
		return
	end
	local query = packet:sub(2)
	-- Checks if a PLOAD command was issued.
	-- A regular expresion check is faster than 
	-- doing a full tokenization. (Especially on large queries)
	--
	if (query:match(pload_help_regexp)) then
		return proxy.global.make_dataset({'command','description'}, pload_help_dataset)
	end
	local unload_module = query:match(punload_regexp)
	if (unload_module) then
		return remove_module(unload_module)
	end
	local new_module = query:match(pload_regexp)
	if (new_module) then
		--[[
		   If a request for loading is received, then
		   we attempt to load the new module using the
		   get_module() function
		--]]
		local new_tablespace = get_module(new_module)
		if (new_tablespace) then
			local handlers = new_tablespace.make_funcs()
			--
			-- The new module must have at least  handlers for read_query()
			-- or disconnect_client. read_query_result() is optional. 
			-- The loading function returns nil if no handlers were found
			--
	
			proxy.global.print_debug('')
			proxy.global.handler_status[new_module] = {}
			for i,v in pairs( proxy.global.handled_functions) do
                local handler_str = type(handlers[i]) 
				proxy.global.print_debug (i .. ' ' .. handler_str )
			    if handlers[i] then
			        table.insert(proxy.global.loaded_handlers[i] , 
				{ name = new_module, handler = handlers[i]} )
				table.insert(proxy.global.handler_status[new_module], 
				{ func = proxy.global.handled_functions[i], id=i, ndx = #proxy.global.loaded_handlers[i] })
				end
			end
			if handlers['rqr'] and not handlers['rq'] then
			    table.insert(proxy.global.loaded_handlers['rq'] , nil )
			end
			if handlers['rq'] and not handlers['rqr'] then
			    table.insert(proxy.global.loaded_handlers['rqr'] , nil )
			end
			-- 
			-- Returns a confirmation that a new  module was loaded
			--
			return proxy.global.simple_dataset('info', 'module "' .. new_module .. '" loaded' )
		else
			--
			-- The load was not successful.
			-- Inform the user
			--
			return proxy.global.simple_dataset('ERROR', 'load of "' 
				.. new_module .. '" failed (' 
				.. load_multi_error_str .. ')' )
		end
	end
	--
	-- If a handler was installed from a new module, it is called
	-- now. 
	--
	if #proxy.global.loaded_handlers['rq'] then
		for i,rqh in pairs(proxy.global.loaded_handlers['rq'])
		do
			proxy.global.print_debug ( 'handling "read_query" using ' .. i .. ' -> ' .. rqh.name)
			local result = rqh.handler(packet)
			if (result) then
				currently_using = i
				return result
			end
		end
	end
end

--
-- this function is called every time a result set is
-- returned after an injected query
--
function read_query_result(inj)
	-- 
	-- If the dynamically loaded module had an handler for read_query_result()
	-- it is called now
	--
	local rqrh = proxy.global.loaded_handlers['rqr'][currently_using]
	if rqrh then
		proxy.global.print_debug ( 'handling "read_query_result" using ' .. currently_using .. ' -> ' .. rqrh.name)
		local result = rqrh.handler(inj)
		if result then
			return result
		end
	end
end
