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
-- automatic configure of modules
--
-- SHOW CONFIG

module("proxy.auto-config", package.seeall)

local l = require("lpeg")

---
--  
local function parse_value(fld, token)
	local t = type(fld)
	local errmsg

	if t == "boolean" then
		if token[1] == "boolean" then
			return token[2] == "true" and true and false
		else
			return nil, "(auto-config) expected a boolean, got " .. token[1]
		end
	elseif t == "number" then
		if token[1] == "number" then
			return tonumber(token[2])
		else
			return nil, "(auto-config) expected a number, got " .. token[1]
		end
	elseif t == "string" then
		if token[1] == "string" then
			return tostring(token[2])
		else
			return nil, "(auto-config) expected a string, got " .. token[1]
		end
	else
		return nil, "(auto-config) type: " .. t .. " isn't handled yet" 
	end
	
	return nil, "(auto-config) should not be reached"
end

---
-- transform a table into loadable string
local function tbl2str(tbl, indent)
	local s = ""
	indent = indent or "" -- set a default

	for k, v in pairs(tbl) do
		s = s .. indent .. ("[%q] --[[%s]]-- = "):format(k, type(k))
		if type(v) == "table" then
			s = s .. "{\n" .. tbl2str(v, indent .. "  ") .. indent .. "}"
		elseif type(v) == "string" then
			s = s .. ("%q"):format(v)
		else
			s = s .. tostring(v)
		end
		s = s .. ",\n"
	end

	return s
end


---
-- turn a string into a case-insensitive lpeg-pattern
--
local function lpeg_ci_str(s)
	local p

	for i = 1, #s do
		local c = s:sub(i, i)

		local lp = l.S(c:upper() .. c:lower() )

		if p then
			p = p * lp
		else
			p = lp
		end
	end

	return p
end


local WS     = l.S(" \t\n")

local PROXY  = lpeg_ci_str("PROXY") * WS^1
local SHOW   = lpeg_ci_str("SHOW") * WS^1
local SET    = lpeg_ci_str("SET") * WS^1
local GLOBAL = lpeg_ci_str("GLOBAL") * WS^1
local CONFIG = lpeg_ci_str("CONFIG")
local SAVE   = lpeg_ci_str("SAVE") * WS^1
local LOAD   = lpeg_ci_str("LOAD") * WS^1
local INTO   = lpeg_ci_str("INTO") * WS^1
local FROM   = lpeg_ci_str("FROM") * WS^1
local DOT    = l.P(".")
local EQ     = WS^0 * l.P("=") * WS^0
local literal = l.R("az", "AZ") ^ 1
local string_quoted  = l.P("\"") * ( 1 - l.P("\"") )^0 * l.P("\"") -- /".*"/
local digit  = l.R("09")       -- [0-9]
local number = (l.P("-") + "") * digit^1         -- [0-9]+
local bool   = l.P("true") + l.P("false")

local l_proxy = l.Ct(PROXY * 
	((SHOW / "SHOW" * CONFIG) +
	 (SET  / "SET"  * GLOBAL * l.C(literal) * DOT * l.C(literal) * EQ * 
	 	l.Ct( l.Cc("string") * l.C(string_quoted) + 
		      l.Cc("number") * l.C(number) + 
		      l.Cc("boolean") * l.C(bool) )) +
	 (SAVE / "SAVE" * CONFIG * WS^1 * INTO * l.C(string_quoted)) +
	 (LOAD / "LOAD" * CONFIG * WS^1 * FROM * l.C(string_quoted))) * -1)

function handle(tbl, cmd)
	---
	-- support old, deprecated API:
	--
	--   auto_config.handle(cmd)
	--
	-- and map it to
	--
	--   proxy.global.config:handle(cmd)
	if cmd == nil and tbl.type and type(tbl.type) == "number" then
		cmd = tbl
		tbl = proxy.global.config
	end
	
	-- handle script-options first
	if cmd.type ~= proxy.COM_QUERY then return nil end

	-- don't try to tokenize log SQL queries
	if #cmd.query > 128 then return nil end

	local tokens = l_proxy:match(cmd.query)

	if not tokens then 
		return nil
	end
	
	-- print(tbl2str(tokens))
	
	if tokens[1] == "SET" then
		if not tbl[tokens[2]] then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_ERR,
				errmsg = "module not known"
			}
		elseif not tbl[tokens[2]][tokens[3]] then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_ERR,
				errmsg = "option not know"
			}
		else
			-- do the assignment
			local val, errmsg = parse_value(tbl[tokens[2]][tokens[3]], tokens[4])

			if not val then
				proxy.response = {
					type = proxy.MYSQLD_PACKET_ERR,
					errmsg = errmsg
				}
			else
				tbl[tokens[2]][tokens[3]] = val
				
				proxy.response = {
					type = proxy.MYSQLD_PACKET_OK,
					affected_rows = 1
				}

			end
		end
	elseif tokens[1] == "SHOW" then
		local rows = { }

		for mod, options in pairs(tbl) do
			for option, val in pairs(options) do
				rows[#rows + 1] = { mod, option, tostring(val), type(val) }
			end
		end

		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "module", type = proxy.MYSQL_TYPE_STRING },
					{ name = "option", type = proxy.MYSQL_TYPE_STRING },
					{ name = "value", type = proxy.MYSQL_TYPE_STRING },
					{ name = "type", type = proxy.MYSQL_TYPE_STRING },
				},
				rows = rows
			}
		}
	elseif tokens[1] == "SAVE" then
		-- save the config into this filename
		local filename = tokens[2]

		local ret, errmsg =  tbl:save(filename)
		
		if ret then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
				affected_rows = 0
			}
		else
			proxy.response = {
				type = proxy.MYSQLD_PACKET_ERR,
				errmsg = errmsg
			}
		end

	elseif tokens[1] == "LOAD" then
		local filename = tokens[2]

		local ret, errmsg =  tbl:load(filename)

		if ret then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
				affected_rows = 0
			}
		else
			proxy.response = {
				type = proxy.MYSQLD_PACKET_ERR,
				errmsg = errmsg
			}
		end
	else
		assert(false)
	end

	return proxy.PROXY_SEND_RESULT
end

function save(tbl, filename)
	local content = "return {\n" .. tbl2str(tbl, "  ") .. "}"

	local file, errmsg = io.open(filename, "w")

	if not file then
		return false, errmsg
	end

	file:write(content)

	return true
end

function load(tbl, filename)
	local func, errmsg = loadfile(filename)

	if not func then
		return false, errmsg
	end

	local v = func()

	for mod, options in pairs(v) do
		if tbl[mod] then
			-- weave the loaded options in
			for option, value in pairs(options) do
				tbl[mod][option] = value
			end
		else
			tbl[mod] = options
		end
	end

	return true
end

local mt = getmetatable(proxy.global.config) or {}
mt.__index = { 
	handle = handle,
	load = load,
	save = save
}
setmetatable(proxy.global.config, mt)

