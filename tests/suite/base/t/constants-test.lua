--[[ $%BEGINLICENSE%$
 Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.

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

local chassis = assert(require("chassis"))
-- MYSQL_PROXY_VERSION is something like 0.8.0
local v = assert(os.getenv("MYSQL_PROXY_VERSION"))

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	---
	-- extract the version string by hand and build it ourself
	--
	-- 0.8.0 -> 0x00 0x08 0x00
	--
	-- if a assert()ion fails the proxy will just forward the query to the backend
	-- we will return "failed"
	local s_maj, s_min, s_patch = v:match("^(%d+)\.(%d+)\.(%d+)")
	assert(s_maj)
	assert(s_min)
	assert(s_patch)

	assert(tostring(tonumber(s_maj)) == s_maj)
	assert(tostring(tonumber(s_min)) == s_min)
	assert(tostring(tonumber(s_patch)) == s_patch)

	local maj = tonumber(s_maj)
	local min = tonumber(s_min)
	local patch = tonumber(s_patch)

	assert(maj >= 0, ("%s -> major = %s should be > 0"):format(v, s_maj))
	assert(min > 0)
	assert(min < 100)
	assert(patch >= 0)
	assert(patch < 100)

	local proxy_version = maj * 65536 + min * 256 + patch

	assert(proxy.PROXY_VERSION == proxy_version)
	assert(_VERSION == "Lua 5.1", _VERSION)

	proxy.response = {
		type = proxy.MYSQLD_PACKET_OK,
		resultset = {
			fields = {
				{ name = "Result", type = proxy.MYSQL_TYPE_STRING },
			},
			rows = { { "passed" }  }
		}
	}

	return proxy.PROXY_SEND_RESULT
end
