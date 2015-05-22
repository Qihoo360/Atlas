--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

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

-- vim:sw=4:noexpandtab 

---
-- a lua baed test-runner for the mysql-proxy
--
-- to stay portable it is written in lua
--

-- we require LFS (LuaFileSystem)
require("lfs")
require("glib2")
require("posix")

---
-- get the directory-name of a path
--
-- @param filename path to create the directory name from
function dirname(filename)
	local dirname = filename

	attr = assert(lfs.attributes(dirname))

	if attr.mode == "directory" then
		return dirname
	end

	dirname = filename:gsub("/[^/]+$", "")
	
	attr = assert(lfs.attributes(dirname))

	assert(attr.mode == "directory", "dirname("..filename..") failed: is ".. attr.mode)

	return dirname
end

---
-- get the file-name of a path
--
-- @param filename path to create the directory name from
function basename(filename)
	name = filename:gsub(".*/", "")
	
	return name
end

-- 
-- a set of user variables which can be overwritten from the environment
--

local testdir = dirname(arg[0])

local USE_POPEN		=	os.getenv('USE_POPEN') or 1

local VERBOSE		= 	os.getenv('TEST_VERBOSE') or 
						os.getenv('VERBOSE') or 
						os.getenv('DEBUG') or 0
VERBOSE = VERBOSE + 0

local FORCE_ON_ERROR 	= os.getenv('FORCE_ON_ERROR') 	or os.getenv('FORCE')
local MYSQL_USER	 	= os.getenv("MYSQL_USER")	 	or "root"
local MYSQL_PASSWORD 	= os.getenv("MYSQL_PASSWORD") 	or ""
local MYSQL_HOST	 	= os.getenv("MYSQL_HOST")	 	or "127.0.0.1"
local MYSQL_PORT	 	= os.getenv("MYSQL_PORT")	 	or "3306"
local MYSQL_DB	   		= os.getenv("MYSQL_DB")	   		or "test"
local MYSQL_TEST_BIN 	= os.getenv("MYSQL_TEST_BIN") 	or "mysqltest"
local MYSQL_CLIENT_BIN 	= os.getenv("MYSQL_CLIENT_BIN") or "mysql"
local TESTS_REGEX 	= os.getenv("TESTS_REGEX")

--
-- Global variables that can be referenced from .options files
--
-- TODO : add HOST variables for MASTER, SLAVE, CHAIN
function get_port_base() 
	local port_base_start = os.getenv("MYSQL_PROXY_START_PORT") or 32768
	local port_base_end   = os.getenv("MYSQL_PROXY_END_PORT") or 65535
	local port_interval   = 64 -- let's take the base port in steps of ...
	local range = port_base_end - port_base_start - port_interval
	math.randomseed(posix.getpid())
	local rand = math.floor(math.random() * (math.ceil(range / port_interval)))
	local port_base = port_base_start + (rand * port_interval)

	print(("... using tcp-port = %d as start port"):format(port_base))

	return port_base
end
local port_base = get_port_base()

PROXY_HOST	   = os.getenv("PROXY_HOST")		or "127.0.0.1"
PROXY_PORT	   = os.getenv("PROXY_PORT")		or tostring(port_base + 0)
PROXY_MASTER_PORT  = os.getenv("PROXY_MASTER_PORT")	or tostring(port_base + 10)
PROXY_SLAVE_PORT   = os.getenv("PROXY_SLAVE_PORT")	or tostring(port_base + 20)
PROXY_CHAIN_PORT   = os.getenv("PROXY_CHAIN_PORT")	or tostring(port_base + 30)
ADMIN_PORT	   = os.getenv("ADMIN_PORT")		or tostring(port_base + 15)
ADMIN_MASTER_PORT  = os.getenv("ADMIN_MASTER_PORT")	or tostring(port_base + 25)
ADMIN_SLAVE_PORT   = os.getenv("ADMIN_SLAVE_PORT")	or tostring(port_base + 35)
ADMIN_CHAIN_PORT   = os.getenv("ADMIN_CHAIN_PORT")	or tostring(port_base + 45)
ADMIN_USER	   = os.getenv("ADMIN_USER")		or "root"
ADMIN_PASSWORD	   = os.getenv("ADMIN_PASSWORD")	or ""
ADMIN_DEFAULT_SCRIPT_FILENAME = os.getenv("ADMIN_DEFAULT_SCRIPT_FILENAME")	or ""
-- local PROXY_TMP_LUASCRIPT = os.getenv("PROXY_TMP_LUASCRIPT") or "/tmp/proxy.tmp.lua"

local srcdir		 = os.getenv("srcdir")		 	or testdir .. "/"
local top_builddir	 = os.getenv("top_builddir")   		or testdir .. "/../"
local builddir	         = os.getenv("builddir")	   	or testdir .. "/" -- same as srcdir by default

local PROXY_TRACE	= os.getenv("PROXY_TRACE")		or ""	-- use it to inject strace or valgrind
local PROXY_PARAMS   = os.getenv("PROXY_PARAMS")   	or ""	-- extra params
local PROXY_BINPATH  = os.getenv("PROXY_BINPATH")  	or top_builddir .. "/src/mysql-proxy"
PROXY_LIBPATH  = os.getenv("PROXY_LIBPATH")  	or top_builddir .. "/plugins/"

local COVERAGE_LCOV  = os.getenv("COVERAGE_LCOV")

--
-- end of user-vars
--

PROXY_PIDFILE		 	= lfs.currentdir() .. "/mysql-proxy-test.pid"
PROXY_MASTER_PIDFILE  	= lfs.currentdir() .. "/mysql-proxy-test-master.pid"
PROXY_SLAVE_PIDFILE   	= lfs.currentdir() .. "/mysql-proxy-test-slave.pid"
PROXY_CHAIN_PIDFILE   	= lfs.currentdir() .. "/mysql-proxy-test-chain.pid"
PROXY_BACKEND_PIDFILE 	= lfs.currentdir() .. "/mysql-proxy-test-backend.pid"
PROXY_TEST_BASEDIR		= lfs.currentdir()

DEFAULT_SCRIPT_FILENAME	 = "/tmp/dummy.lua"
default_proxy_options = {
	["proxy-backend-addresses"] = MYSQL_HOST .. ":" .. MYSQL_PORT,
	["proxy-address"]		   	= PROXY_HOST .. ":" .. PROXY_PORT,
	["pid-file"]				= PROXY_PIDFILE,
	["proxy-lua-script"]		= DEFAULT_SCRIPT_FILENAME,
	["plugin-dir"]			= PROXY_LIBPATH,
	["basedir"]					= PROXY_TEST_BASEDIR,
	}

default_master_options = {
	["proxy-backend-addresses"] = MYSQL_HOST .. ":" .. MYSQL_PORT,
	["proxy-address"]		   	= PROXY_HOST .. ":" .. PROXY_MASTER_PORT,
	["pid-file"]				= PROXY_MASTER_PIDFILE,
	["proxy-lua-script"]		= DEFAULT_SCRIPT_FILENAME,
	["plugin-dir"]			= PROXY_LIBPATH,
	["basedir"]					= PROXY_TEST_BASEDIR,
	}

default_slave_options = {
	["proxy-backend-addresses"] = MYSQL_HOST .. ":" .. MYSQL_PORT,
	["proxy-address"]		   	= PROXY_HOST .. ":" .. PROXY_SLAVE_PORT,
	["pid-file"]				= PROXY_SLAVE_PIDFILE,
	["proxy-lua-script"]		= DEFAULT_SCRIPT_FILENAME,
	["plugin-dir"]			= PROXY_LIBPATH,
	["basedir"]					= PROXY_TEST_BASEDIR,
	}


tests_to_skip = {}
local tests_to_skip_filename = 'tests_to_skip.lua' 
local proxy_list = {}
default_proxy_name = 'default'

local exitcode=0

---
-- print_verbose()
--
-- prints a message if either the DEBUG or VERBOSE variables
-- are set.
--
-- @param msg the message being printed
-- @param min_level the minimum verbosity level for printing the message (default 1)
function print_verbose(msg, min_level)
	min_level = min_level or 1
	if (VERBOSE >= min_level) then
		print (msg)
	end
end
---
-- check if the file exists and is readable 
function file_exists(f)
	return lfs.attributes(f)
end

---
-- create the default option file
--
function make_default_options_file(fname)
	if file_exists(fname) then
		return
	end
	local fd = assert(io.open(fname, "w"))
	fd:write('start_proxy(default_proxy_name, default_proxy_options) \n')
	fd:close();
end

--- 
-- copy a file
--
-- @param dst filename of the destination
-- @param src filename of the source
function file_copy(dst, src)
	-- print_verbose("copying ".. src .. " to " .. dst)
	local src_fd = assert(io.open(src, "rb"))
	local content = src_fd:read("*a")
	src_fd:close();

	local dst_fd = assert(io.open(dst, "wb+"))
	dst_fd:write(content);
	dst_fd:close();
end

---
-- create a empty file
--
-- if the file exists, it will be truncated to 0
--
-- @param dst filename to create and truncate
function file_empty(dst)
	-- print_verbose("emptying " .. dst)
	local dst_fd = assert(io.open(dst, "wb+"))
	dst_fd:close();
end

---
-- turn a option-table into a string 
--
-- the values are encoded and quoted for the shell
--
-- @param tbl a option table
-- @param sep the seperator, defaults to a space
function options_tostring(tbl, sep)
	-- default value for sep 
	sep = sep or " "
	
	assert(type(tbl) == "table")
	assert(type(sep) == "string")

	local s = ""
	for k, v in pairs(tbl) do
		local values
		-- if the value is a table, repeat the option
		if type(v) == "table" then
			values = v
		else
			values = { v }
		end

		for tk, tv in pairs(values) do
			local enc_value = tv:gsub("\\", "\\\\"):gsub("\"", "\\\"")
			s = s .. "--" .. k .. "=\"" .. enc_value .. "\" "
		end
	end
	-- print_verbose(" option: " .. s)

	return s
end

--- turns an option table into a string of environment variables
--
function env_options_tostring(tbl)
	assert(type(tbl) == "table")

	local s = ""
	for k, v in pairs(tbl) do
		local enc_value = v:gsub("\\", "\\\\"):gsub("\"", "\\\"")
		s = s .. k .. "=\"" .. enc_value .. "\" "
	end

	return s
end


function os_execute(cmdline)
	print_verbose("$ " .. cmdline)
	return os.execute(cmdline)
end

---
-- get the PID from the pid file
--
function get_pid(pid_file_name)
	-- the file may exist, but the PID may not be written yet
	local pid
	local rounds = 0

	repeat 
		local fh = assert(io.open(pid_file_name, 'r'))
		pid = fh:read("*n")
		fh:close()
		if not pid then
			glib2.usleep(200 * 1000) -- wait a bit until we get some content
			rounds = rounds + 1

			if rounds > 10 then
				error(("reading PID from existing pid-file '%s' failed after waiting 2 sec"):format(
					pid_file_name))
			end
		end
	until pid

	-- the PID we get here should be a number
	assert(type(pid) == "number")

	return pid
end

function wait_proc_up(pid_file) 
	local rounds = 0

	while not file_exists(pid_file) do
		glib2.usleep(200 * 1000) -- wait until the pid-file is created

		rounds = rounds + 1
		print_verbose(("(wait_proc_up) pid-wait: %d rounds, (%s)"):format(rounds, pid_file))

		if rounds > 1000 then error(("proxy failed to start: no pid-file %s"):format(pid_file)) end
	end

	local pid = get_pid(pid_file)

	rounds = 0
	-- check that the process referenced in the PID-file is still up
	while 0 ~= os.execute("kill -0 ".. pid .."  2> /dev/null") do
		glib2.usleep(200 * 1000) -- wait until the pid-file is created
		rounds = rounds + 1
		print_verbose(("(wait_proc_up) kill-wait: %d rounds, pid=%d (%s)"):format(rounds, pid, pid_file))

		if rounds > 5 then error(("proxy seems to have crashed: pid=%d (%s)"):format(pid, pid_file)) end
	end
end

function proc_is_up(pid)
	return os.execute("kill -0 ".. pid .."  2> /dev/null")
end

function proc_stop(pid)
	return os.execute("kill -TERM ".. pid)
end

function wait_proc_down(pid_file) 
	local rounds = 0
	local pid = get_pid(pid_file)

	-- wait until the proc in the pid file is dead
	-- the shutdown takes at about 500ms
	while 0 == proc_is_up(pid) do
		glib2.usleep(200 * 1000) -- wait until process is gone
		rounds = rounds + 1
		print_verbose(("(wait_proc_down) kill-wait: %d rounds, pid=%d (%s)"):format(rounds, pid, pid_file))
	end
end

function stop_proxy()
	-- shut dowm the proxy
	--
	-- win32 has tasklist and taskkill on the shell
	 
	
	-- shuts down every proxy in the proxy list
	--
	for proxy_name, proxy_options in pairs(proxy_list) do
		local pid
		pid_file = proxy_options['pid-file']
		print_verbose ('stopping proxy ' .. proxy_name)
		
		pid = get_pid(pid_file)

		if proc_is_up(pid) then
			proc_stop(pid)
		else
			print("-- process "..proxy_name.." is already down")
			exitcode = -1
		end
	end

	-- wait until they are all gone
	for proxy_name, proxy_options in pairs(proxy_list) do
		pid_file = proxy_options['pid-file']

		wait_proc_down(pid_file)

		os.remove(pid_file)
	end

	--
	-- empties the proxy list
	--
	proxy_list = { }
end

function only_item ( tbl, item)
	local exists = false
	for i,v in pairs(tbl) do
		if i == item then
			exists = true
		else
			return false
		end
	end
	return exists
end

---
-- before_test()
--
-- Executes a script with a base name like test_name and extension ".options"
--
-- If there is no such file, the default options are used
--
function before_test(basedir, test_name)
	local script_filename = basedir .. "/t/" .. test_name .. ".lua"
	local options_filename = basedir .. "/t/" .. test_name .. ".options"
	local has_option_file = file_exists(options_filename)
	if file_exists( script_filename) then
		if has_option_file then 
			default_proxy_options['proxy-lua-script'] = script_filename
			print_verbose('using lua script directly ' .. script_filename)
			file_empty(DEFAULT_SCRIPT_FILENAME)
		else
			default_proxy_options['proxy-lua-script'] = DEFAULT_SCRIPT_FILENAME
			file_copy(DEFAULT_SCRIPT_FILENAME, script_filename)
			print_verbose('copying lua script to default ' .. script_filename)
		end
	else
		default_proxy_options['proxy-lua-script'] = DEFAULT_SCRIPT_FILENAME
		file_empty(DEFAULT_SCRIPT_FILENAME)
		print_verbose('using empty lua script')
	end
	global_basedir = basedir
	print_verbose ('current_dir ' ..  
					basedir ..  
					' - script: '  ..
					default_proxy_options['proxy-lua-script']  )
	-- 
	-- executes the content of the options file
	--
	if has_option_file then
		print_verbose('# using options file ' .. options_filename)
		stop_proxy()
	else
		-- 
		-- if no option file is found, the default options file is executed
		--
		options_filename = basedir .. "/t/default.options"
		print_verbose('#using default options file' .. options_filename)
		if only_item(proxy_list,'default') then
			print_verbose('reusing existing proxy')
			return
		end
		make_default_options_file(options_filename)
	end
	assert(loadfile(options_filename))()
end

function after_test()
	if only_item(proxy_list, 'default') then
		return
	end
	stop_proxy()
end

function alternative_execute (cmd)
	print_verbose(cmd)

	local fh = io.popen(cmd)
	assert(fh, 'error executing '.. cmd)
	local result = ''
	local line = fh:read()
	while line do
		result = result .. line
		line = fh:read()
	end
	fh:close()
	return result
end

function conditional_execute (cmd)
	if USE_POPEN then
		return alternative_execute(cmd)
	else
		return os_execute(cmd)
	end
end

--- 
-- run a test
--
-- @param testname name of the test
-- @return exit-code of mysql-test
function run_test(filename, basedir)
	if not basedir then basedir = srcdir end

	local testname = assert(filename:match("t/(.+)\.test"))
	if tests_to_skip[testname] then
		print('skip ' .. testname ..' '.. (tests_to_skip[testname] or 'no reason given') )
		return 0, 1
	end
	before_test(basedir, testname)
	if VERBOSE > 1 then		
		os.execute('echo -n "' .. testname  .. ' " ; ' )
	end
	local result = 0
	local ret = conditional_execute(
		env_options_tostring({
			['MYSQL_USER']  = MYSQL_USER,
			['MYSQL_PASSWORD']  = MYSQL_PASSWORD,
			['PROXY_HOST']  = PROXY_HOST,
			['PROXY_PORT']  = PROXY_PORT,
			['PROXY_CHAIN_PORT']  = PROXY_CHAIN_PORT,
			['MASTER_PORT'] = PROXY_MASTER_PORT,
			['SLAVE_PORT'] = PROXY_SLAVE_PORT,
		}) .. ' ' ..
		MYSQL_TEST_BIN .. " " ..
		options_tostring({
			user	 = MYSQL_USER,
			password = MYSQL_PASSWORD,
			database = MYSQL_DB,
			host	 = PROXY_HOST,
			port	 = PROXY_PORT,
			verbose  = (VERBOSE > 0) and "TRUE" or "FALSE", -- pass down the verbose setting 
			["test-file"] = basedir .. "/t/" .. testname .. ".test",
			["result-file"] = basedir .. "/r/" .. testname .. ".result",
			["logdir"] = builddir, -- the .result dir might not be writable
		})
	)
	if USE_POPEN then
		assert(ret == 'ok' or ret =='not ok', 'unexpected result <' .. ret .. '>')
		if (ret == 'ok') then
			result = 0
		elseif ret == 'not ok'then
			result = 1
		end
		print(ret .. ' ' .. testname)  
	else
		result = ret
	end
	after_test()	
	return result, 0
end

---
--sql_execute()
--
-- Executes a SQL query in a given Proxy
-- 
-- If no Proxy is indicated, the query is passed directly to the backend server
--
-- @param query A SQL statement to execute, or a table of SQL statements
-- @param proxy_name the name of the proxy that executes the query
function sql_execute(queries, proxy_name)
	local ret = 0
	assert(type(queries) == 'string' or type(queries) == 'table', 'invalid type for query' )
	if type(queries) == 'string' then
		queries = {queries}
	end
	local query = ''
	for i, q in pairs(queries) do
		query = query .. ';' .. q
	end

	if proxy_name then  
		-- 
		-- a Proxy name is passed. 
		-- The query is executed with the given proxy
		local opts = proxy_list[proxy_name]
		assert(opts,'proxy '.. proxy_name .. ' not active')
		assert(opts['proxy-address'],'address for proxy '.. proxy_name .. ' not found')
		local p_host, p_port = opts['proxy-address']:match('(%S+):(%S+)')
		ret = os_execute( MYSQL_CLIENT_BIN .. ' ' ..
			options_tostring({
				user	 = MYSQL_USER,
				password = MYSQL_PASSWORD,
				database = MYSQL_DB,
				host	 = p_host,
				port	 = p_port,
				execute  = query
			})
		)
        assert(ret == 0, 'error using mysql client ')
	else
		--
		-- No proxy name was passed.
		-- The query is executed in the backend server
		--
		ret = os_execute( MYSQL_CLIENT_BIN .. ' ' ..
			options_tostring({
				user	 = MYSQL_USER,
				password = MYSQL_PASSWORD,
				database = MYSQL_DB,
				host	 = PROXY_HOST,
				port	 = MYSQL_PORT,
				execute  = query
			})
		)
	end
	return ret
end

stop_proxy()

-- the proxy needs the lua-script to exist
-- file_empty(PROXY_TMP_LUASCRIPT)

-- if the pid-file is still pointing to a active process, kill it
--[[
if file_exists(PROXY_PIDFILE) then
	os.execute("kill -TERM `cat ".. PROXY_PIDFILE .." `")
	os.remove(PROXY_PIDFILE)
end
--]]

if COVERAGE_LCOV then
	-- os_execute(COVERAGE_LCOV .. 
	--	" --zerocounters --directory ".. srcdir .. "/../src/" )
end

-- setting the include path
--

-- this is the path containing the global Lua modules
local GLOBAL_LUA_PATH = os.getenv('LUA_LDIR')  or '/usr/share/lua/5.1/?.lua'

-- this is the path containing the Proxy libraries 
local PROXY_LUA_PATH = os.getenv('LUA_PATH')  or '/usr/local/share/?.lua'

-- This is the path with specific libraries for the test suite
local PRIVATE_LUA_PATH = arg[1]  .. '/t/?.lua'  

-- This is the path with additional libraries that the user needs
local LUA_USER_PATH = os.getenv('LUA_USER_PATH')  or '../lib/?.lua'

-- Building the final include path
local INCLUDE_PATH = 
		LUA_USER_PATH	 .. ';' ..
		PRIVATE_LUA_PATH  .. ';' ..
		GLOBAL_LUA_PATH   .. ';' .. 
		PROXY_LUA_PATH 

---
-- start_proxy()
--
-- starts an instance of MySQL Proxy
--
-- @param proxy_name internal name of the proxy instance, for retrieval
-- @param proxy_options the options to start the Proxy
function start_proxy(proxy_name, proxy_options)
	-- start the proxy
	assert(type(proxy_options) == 'table')
	if not file_exists(proxy_options['proxy-lua-script']) then
		proxy_options['proxy-lua-script'] = 
			global_basedir .. 
			'/t/' ..  proxy_options['proxy-lua-script'] 
	end
	print_verbose("starting " .. proxy_name .. " with " .. options_tostring(proxy_options))

	-- remove the old pid-file if it exists
	os.remove(proxy_options['pid-file'])
	-- if we are supposed to listen on a UNIX socket, remove it first, because we don't clean it up on exit!
	-- TODO: fix the code, instead of hacking around here! Bug#38415
	if proxy_options['proxy-address'] == '/tmp/mysql-proxy-test.sock' then
		os.remove(proxy_options['proxy-address'])
	end
	-- os.execute("head " .. proxy_options['proxy-lua-script'])
	assert(os.execute( 'LUA_PATH="' .. INCLUDE_PATH  .. '"  ' ..
		PROXY_TRACE .. " " .. PROXY_BINPATH .. " " ..
		options_tostring( proxy_options) .. " &"
	))

	-- wait until the proxy is up
	wait_proc_up(proxy_options['pid-file'])

	proxy_list[proxy_name] = proxy_options 
end

---
-- simulate_replication()
--
-- creates a fake master/slave by having two proxies
-- pointing at the same backend
--
-- you can alter those backends by changing 
-- the starting parameters
--
-- @param master_options options for master
-- @param slave_options options for slave
function simulate_replication(master_options, slave_options)
	if not master_options then
		master_options = default_master_options
	end
	if not master_options['pid-file'] then
		master_options['pid-file'] = PROXY_MASTER_PIDFILE
	end
	if not slave_options then
		slave_options = default_slave_options
	end
	if not slave_options['pid-file'] then
		slave_options['pid-file'] = PROXY_SLAVE_PIDFILE
	end
	start_proxy('master', master_options)
	start_proxy('slave', slave_options)
end

---
-- chain_proxy()
--
-- starts two proxy instances, with the first one is pointing to the 
-- default backend server, and the second one (with default ports)
-- is pointing at the first proxy
--
--   client -> proxy -> backend_proxy -> [ mysql-backend ]
--
-- usually we use it to mock a server with a lua script (...-mock.lua) and
-- the script under test in the proxy (...-test.lua)
--
-- in case you want to start several backend_proxies, just provide a array
-- as first param
--
-- @param backend_lua_script
-- @param second_lua_script 
-- @param use_replication uses a master proxy as backend 
function chain_proxy (backend_lua_scripts, second_lua_script, use_replication)
	local backends = { }

	if type(backend_lua_scripts) == "table" then
		backends = backend_lua_scripts
	else
		backends = { backend_lua_scripts }
	end

	local backend_addresses = { }

	for i, backend_lua_script in ipairs(backends) do
		backend_addresses[i] = PROXY_HOST .. ":" .. (PROXY_CHAIN_PORT + i - 1)

		backend_proxy_options = {
			["proxy-backend-addresses"] = MYSQL_HOST .. ":" .. MYSQL_PORT,
			["proxy-address"]		   = backend_addresses[i],
			["pid-file"]				= PROXY_CHAIN_PIDFILE .. i,
			["proxy-lua-script"]		= backend_lua_script or DEFAULT_SCRIPT_FILENAME,
			["plugin-dir"]			= PROXY_LIBPATH,
			["basedir"]					= PROXY_TEST_BASEDIR,
			["log-level"]			= (VERBOSE == 4) and "debug" or "critical",
		}
		-- 
		-- if replication was not started, then it is started here
		--
		if use_replication and (use_replication == true) then
			if (proxy_list['master'] == nil) then
				simulate_replication()
			end
			backend_proxy_options["proxy-backend-addresses"] = PROXY_HOST .. ':' .. PROXY_MASTER_PORT
		end
		start_proxy('backend_proxy' .. i, backend_proxy_options) 
	end

	second_proxy_options = {
			["proxy-backend-addresses"] = backend_addresses ,
			["proxy-address"]		   	= PROXY_HOST .. ":" .. PROXY_PORT,
			["pid-file"]				= PROXY_PIDFILE,
			["proxy-lua-script"]		= second_lua_script or DEFAULT_SCRIPT_FILENAME,
			["plugin-dir"]			= PROXY_LIBPATH,
			["basedir"]					= PROXY_TEST_BASEDIR,
			["log-level"]			= (VERBOSE == 3) and "debug" or "critical",
	}
	start_proxy('second_proxy',second_proxy_options) 
end

local num_tests	 	= 0
local num_passes	= 0
local num_skipped   = 0
local num_fails	 	= 0
local all_ok		= true
local failed_test   = {}

file_empty(DEFAULT_SCRIPT_FILENAME)

--
-- if we have a argument, exectute the named test
-- otherwise execute all tests we can find
if #arg then
	for i, a in ipairs(arg) do
		local stat = assert(lfs.attributes(a))

		if file_exists(a .. '/' .. tests_to_skip_filename) then
			assert(loadfile(a .. '/' .. tests_to_skip_filename))()
		end

		-- if it is a directory, execute all of them
		if stat.mode == "directory" then
			for file in lfs.dir(a .. "/t/") do
				if not TESTS_REGEX or file:match(TESTS_REGEX) then
					local testname = file:match("(.+\.test)$")
			
					if testname then
						print_verbose("# >> " .. testname .. " started")
			
						num_tests = num_tests + 1
						local r, skipped  = run_test("t/" .. testname, a)
						if (r == 0) then
							num_passes = num_passes + 1 - skipped
						else
							num_fails = num_fails + 1
							all_ok = false
							table.insert(failed_test, testname)
						end
						num_skipped = num_skipped + skipped

						print_verbose("# << (exitcode = " .. r .. ")" )
			
						if r ~= 0 and exitcode == 0 then
							exitcode = r
						end
					end
					if all_ok == false and (not FORCE_ON_ERROR) then
						break
					end
				end
			end
		else 
			-- otherwise just this one test
			--
			-- FIXME: base/ is hard-coded for now
			exitcode, skipped = run_test(a, "base/")
			num_skipped = num_skipped + skipped
		end
	end
else
	for file in lfs.dir(srcdir .. "/t/") do
		local testname = file:match("(.+\.test)$")

		if testname then
			print_verbose("# >> " .. testname .. " started")

			num_tests = num_tests + 1
			local r, skipped = run_test("t/" .. testname)
			num_skipped = num_skipped + skipped
			
			print_verbose("# << (exitcode = " .. r .. ")" )
			if (r == 0) then
				num_passes = num_passes + 1 - skipped
			else
				all_ok = false
				num_fails = num_fails + 1
				table.insert(failed_test, testname)
			end
			if r ~= 0 and exitcode == 0 then
				exitcode = r
			end
			if all_ok == false and (not FORCE_ON_ERROR) then
				break
			end
		end
	end
end

if all_ok ==false then
	print ("*** ERRORS OCCURRED - The following tests failed")
	for i,v in pairs(failed_test) do
		print(v )
	end
end

--
-- prints test suite statistics
print_verbose (string.format('tests: %d - skipped: %d (%4.1f%%) - passed: %d (%4.1f%%) - failed: %d (%4.1f%%)',
			num_tests,
			num_skipped,
			num_skipped / num_tests * 100,
			num_passes,
			num_passes / (num_tests - num_skipped) * 100,
			num_fails,
			num_fails / (num_tests  - num_skipped) * 100))

--
-- stops any remaining active proxy
--
stop_proxy()

if COVERAGE_LCOV then
	os_execute(COVERAGE_LCOV .. 
		" --quiet " ..
		" --capture --directory ".. srcdir .. "/../src/" ..
		" > " .. srcdir .. "/../tests.coverage.info" )

	os_execute("genhtml " .. 
		"--show-details " ..
		"--output-directory " .. srcdir .. "/../coverage/ " ..
		"--keep-descriptions " ..
		"--frames " ..
		srcdir .. "/../tests.coverage.info")
end


if exitcode == 0 then
	os.exit(0)
else
	print_verbose("mysql-test exit-code: " .. exitcode)
	os.exit(-1)
end

