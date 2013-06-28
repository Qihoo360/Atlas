--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.

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
local proto = require("mysql.proto")

function connect_server()
	-- emulate a server
	proxy.response = {
		type = proxy.MYSQLD_PACKET_RAW,
		packets = {
			proto.to_challenge_packet({})
		}
	}
	return proxy.PROXY_SEND_RESULT
end

function read_query(packet)
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	local query = packet:sub(2)

	if query == "select 'first' as info" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "info" },
				},
				rows = {
					{ "first" }
				}
			}
		}
	
		return proxy.PROXY_SEND_RESULT
	elseif query == "select 'second' as info" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "info" },
				},
				rows = {
					{ "second" }
				}
			}
		}
	
		return proxy.PROXY_SEND_RESULT
	elseif query == "select 'third' as info" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "info" },
				},
				rows = {
					{ "third" }
				}
			}
		}
	
		return proxy.PROXY_SEND_RESULT
	elseif query == "select 1000" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "1000" },
				},
				rows = {
					{ "1000" }
				}
			}
		}
	
		return proxy.PROXY_SEND_RESULT
	elseif query == "select sleep(0.2)" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "sleep(0.2)" },
				},
				rows = {
					{ "0" }
				}
			}
		}
	
		return proxy.PROXY_SEND_RESULT

	end

	proxy.response = {
		type = proxy.MYSQLD_PACKET_ERR,
		errmsg = ''..query..''
	}
	
	return proxy.PROXY_SEND_RESULT
end




