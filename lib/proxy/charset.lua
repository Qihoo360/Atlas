module("proxy.charset", package.seeall)

local config_file = string.format("proxy.conf.config_%s", proxy.global.config.instance)
local config	  = require(config_file)
local log = require("proxy.log")
local level = log.level
local write_log = log.write_log

function modify_charset(tokens, c, s)
	write_log(level.DEBUG, "ENTER MODIFY_CHARSET")

	local is_set_client     = false
	local is_set_results    = false
	local is_set_connection = false

	if tokens and tokens[1].text:upper() == "SET" then
		if #tokens >=3 then
			local token2 = tokens[2].text:upper()
			if token2 == "NAMES" then
				is_set_client     = true
				is_set_results    = true
				is_set_connection = true
			elseif #tokens == 4 and tokens[3].text == "=" then
				if token2 == "CHARACTER_SET_CLIENT" then
					is_set_client = true
				elseif token2 == "CHARACTER_SET_RESULTS" then
					is_set_results = true
				elseif token2 == "CHARACTER_SET_CONNECTION" then
					is_set_connection = true
				end
			end
		end
	end

	local id = 2
	local default_charset = "LATIN1"

	if not is_set_client and c.charset_client ~= s.charset_client then
		id = id + 1
		if c.charset_client == "" then
      			proxy.queries:prepend(id, string.char(proxy.COM_QUERY) .. "SET CHARACTER_SET_CLIENT=" .. default_charset, { resultset_is_needed = true })
				write_log(level.INFO, "change s.charset_client to ", default_charset)
		else
      			proxy.queries:prepend(id, string.char(proxy.COM_QUERY) .. "SET CHARACTER_SET_CLIENT=" .. c.charset_client, { resultset_is_needed = true })
				write_log(level.INFO, "change s.charset_client to ", c.charset_client)
		end
	end

	if not is_set_results and c.charset_results ~= s.charset_results then
		id = id + 1
		if c.charset_results == "" then
      			proxy.queries:prepend(id, string.char(proxy.COM_QUERY) .. "SET CHARACTER_SET_RESULTS=" .. default_charset, { resultset_is_needed = true })
				write_log(level.INFO, "change s.charset_results to ", default_charset)
		else
      			proxy.queries:prepend(id, string.char(proxy.COM_QUERY) .. "SET CHARACTER_SET_RESULTS=" .. c.charset_results, { resultset_is_needed = true })
				write_log(level.INFO, "change s.charset_results to ", c.charset_results)
		end
	end

	if not is_set_connection and c.charset_connection ~= s.charset_connection then
		id = id + 1
		if c.charset_connection == "" then
      			proxy.queries:prepend(id, string.char(proxy.COM_QUERY) .. "SET CHARACTER_SET_CONNECTION=" .. default_charset, { resultset_is_needed = true })
				write_log(level.INFO, "change s.charset_connection to ", default_charset)
		else
      			proxy.queries:prepend(id, string.char(proxy.COM_QUERY) .. "SET CHARACTER_SET_CONNECTION=" .. c.charset_connection, { resultset_is_needed = true })
				write_log(level.INFO, "change s.charset_connection to ", c.charset_connection)
		end
	end

	write_log(level.DEBUG, "LEAVE MODIFY_CHARSET")
end 
