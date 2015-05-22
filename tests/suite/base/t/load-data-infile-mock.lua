--[[ $%BEGINLICENSE%$
 Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

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
			proto.to_challenge_packet({
				capabilities = 128 +  -- CLIENT_LOCAL_FILES
						512 + -- CLIENT_PROTOCOL_41
						32768 -- CLIENT_SECURE_CONNECTION
			})
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
	if query == "load data local infile 'testfile' into table foo.bar" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_RAW,
			packets = {
				-- a NUL to indicate the LOAD DATA INFILE LOCAL filename to we want to get
				"\251" .. 
				"Makefile" -- the file that 'mysqltest' will find. mysqltest's pwd is ${builddir}/tests/suite/
			}
		}
	elseif query == "SELECT 1" then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = {
					{ name = "1", type = proxy.MYSQL_TYPE_STRING },
				},
				rows = {
					{ 1 },
				},
			},
		}
	else
		proxy.response = {
			type = proxy.MYSQLD_PACKET_ERR,
			errmsg = "(resultset-mock) >" .. query .. "<"
		}
	end
	return proxy.PROXY_SEND_RESULT
end

