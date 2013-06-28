module("proxy.filter", package.seeall)

local config_file = string.format("proxy.conf.config_%s", proxy.global.config.instance)
local config	  = require(config_file)
local whitelist = config.whitelist
local blacklist = config.blacklist

local log = require("proxy.log")
local level = log.level
local write_log = log.write_log

function is_whitelist(tokens)
	write_log(level.DEBUG, "ENTER IS_WHITELIST")
	local re = false

	local first_token
	if tokens[1].token_name ~= "TK_COMMENT" then
		first_token = tokens[1].text:upper()
	else
		first_token = tokens[2].text:upper()
	end

	for i = 1, #whitelist do
		local wtokens = whitelist[i]
		for j = 1, #wtokens do
			if first_token == wtokens[j] then
				write_log(level.DEBUG, "LEAVE IS_WHITELIST")
				return i
			end
		end
	end

	write_log(level.DEBUG, "LEAVE IS_WHITELIST")
	return re
end

function is_blacklist(tokens)
	write_log(level.DEBUG, "ENTER IS_BLACKLIST")
	local re = false

	local first_token = tokens[1].text:upper()
	for i = 1, #blacklist do
		local b = blacklist[i]
		local first     = b.FIRST
		local first_not = b.FIRST_NOT

		local meet_first = false
		local meet_any   = true
		local meet_all   = true

		if first then
			if first == first_token then meet_first = true end
		elseif first_not and first_not ~= first_token then
			meet_first = true
		end

		if meet_first then
			local any = b.ANY
			if any then
				meet_any = false
				for j = 1, #tokens do
					local token = tokens[j].text:upper()
					if any == token then
						meet_any = true
						break
					end
				end
			end

			local all_not = b.ALL_NOT
			if all_not then
				for j = 1, #tokens do
					local token = tokens[j].text:upper()
					if all_not == token then
						meet_all = false
						break
					end
				end
			end
		end

		if meet_first and meet_any and meet_all then
			re = true
			break
		end
	end

	write_log(level.DEBUG, "LEAVE IS_BLACKLIST")
	return re
end
