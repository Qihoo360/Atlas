module("proxy.log", package.seeall)

local config_file = string.format("proxy.conf.config_%s", proxy.global.config.instance)
local config	  = require(config_file)
level =
{
	DEBUG = 1,
	INFO  = 2,
	WARN  = 3,
	ERROR = 4,
	FATAL = 5,
	OFF   = 6
}
local tag = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

--默认值
local log_level = level.OFF
local is_rt     = false 
local is_sql    = false

local f_log  = nil
local f_sql  = nil
local last_time

function init_log()
	--从config.lua里读取日志设置
	for i = 1, 6 do
		if config.log_level:upper() == tag[i] then
			log_level = i
			break 
		end
	end
	is_rt  = config.real_time
	is_sql = config.sql_log

	--创建lua.log和sql.log文件
	if log_level < level.OFF and f_log == nil then
		local lua_log_file = string.format("%s/lua_%s.log", proxy.global.config.logpath, proxy.global.config.instance)
    	f_log = io.open(lua_log_file, "a+")
    	if f_log == nil then log_level = level.OFF end 
	end
	if is_sql and f_sql == nil then
		local sql_log_file = string.format("%s/sql_%s.log", proxy.global.config.logpath, proxy.global.config.instance)
    	f_sql = io.open(sql_log_file, "a+")
    	if f_sql == nil then is_sql = false end 
	end
end

function write_log(cur_level, ...)
	if cur_level >= log_level then
		f_log:write("[", os.date("%b/%d/%Y"), "] [", tag[cur_level], "] ", unpack(arg))
		f_log:write("\n")
		if is_rt then f_log:flush() end 
	end
end

function write_query(query, client)
	if is_sql then
		local pos = string.find(client, ":")
		client_ip = string.sub(client, 1, pos-1)
		f_sql:write("\n[", os.date("%b/%d/%Y %X"), "] C:", client_ip, " - ERR - \"", query, "\"")
		if is_rt then f_sql:flush() end
	end
end

function write_sql(inj, client, server)
	if is_sql then
		local pos = string.find(client, ":")
		client_ip = string.sub(client, 1, pos-1)
		pos = string.find(server, ":")
		server_ip = string.sub(server, 1, pos-1)
		f_sql:write("\n[", os.date("%b/%d/%Y %X"), "] C:", client_ip, " S:", server_ip, " ERR - \"", inj.query:sub(2), "\"")
		if is_rt then f_sql:flush() end
	end
end

function write_des(index, inj, client, server)
	if is_sql then
		local pos = string.find(client, ":")
		client_ip = string.sub(client, 1, pos-1)
		pos = string.find(server, ":")
		server_ip = string.sub(server, 1, pos-1)

		local current_time = inj.response_time/1000
		if index > 1 then
			f_sql:write(string.format("\n- C:%s S:%s OK %s \"%s\"", client_ip, server_ip, current_time-last_time, inj.query:sub(2)))
			last_time = current_time
		else
			f_sql:write(string.format("\n[%s] C:%s S:%s OK %s \"%s\"", os.date("%b/%d/%Y %X"), client_ip, server_ip, current_time, inj.query:sub(2)))
			last_time = current_time
		end
		if is_rt then f_sql:flush() end 
	end
end
