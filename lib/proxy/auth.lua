module("proxy.auth", package.seeall)

local config_file = string.format("proxy.conf.config_%s", proxy.global.config.instance)
local auth_ip = require(config_file).auth
local lvs_ip  = require(config_file).lvs_ip
local seg_ip  = require(config_file).seg_ip

function allow_ip(ip)
	for i = 1, #auth_ip do
		if ip == auth_ip[i].ip then
			return true
		end
	end
	for i = 1, #lvs_ip do
		if ip == lvs_ip[i] then
			return true
		end
	end
	for i = 1, #seg_ip do
		if ip:find(seg_ip[i]) == 1 then
			return true
		end 
	end
	return false
end
