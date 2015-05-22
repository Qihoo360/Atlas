--[[ $%BEGINLICENSE%$
 Copyright (c) 2008, 2009, Oracle and/or its affiliates. All rights reserved.

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
local proto = assert(require("mysql.proto"))
local password = assert(require("mysql.password"))
require("proxy.test")
---
-- err packet

local err_packet = proto.to_err_packet({ errmsg = "123" })
assert(type(err_packet) == "string")
assert(#err_packet == 12)

local tbl = proto.from_err_packet(err_packet)
assert(tbl.errmsg == "123")
assert(tbl.errcode == 0)
assert(tbl.sqlstate == "07000")

---
-- ok packet

local ok_packet = proto.to_ok_packet({ server_status = 2 })
assert(type(ok_packet) == "string")
assert(#ok_packet == 7)
local tbl = proto.from_ok_packet(ok_packet)
assert(tbl.server_status == 2)
assert(tbl.insert_id == 0)
assert(tbl.affected_rows == 0)
assert(tbl.warnings == 0)

-- should fail
assert(false == pcall(
	-- wrong type
	function () 
		proto.from_ok_packet(nil)
	end
))

assert(false == pcall(
	-- packet is too short
	function () 
		proto.from_ok_packet("")
	end
))

-- should decode nicely
assert(true == pcall(
	function () 
		proto.from_ok_packet("\000\000\000\002\000\000\000")
	end
))

---
-- eof packet

local eof_packet = proto.to_eof_packet({ server_status = 2 })
assert(type(eof_packet) == "string")
assert(#eof_packet == 5)
local tbl = proto.from_eof_packet(eof_packet)
assert(tbl.server_status == 2)
assert(tbl.warnings == 0)

-- should fail
assert(false == pcall(
	-- wrong type
	function () 
		proto.from_eof_packet(nil)
	end
))

assert(false == pcall(
	-- packet is too short
	function () 
		proto.from_eof_packet("")
	end
))

-- should decode nicely
assert(true == pcall(
	function () 
		proto.from_eof_packet("\254\002\000\000\000")
	end
))

---
-- challenge packet

-- should fail
assert(false == pcall(
	-- wrong type
	function () 
		proto.from_challenge_packet(nil)
	end
))

assert(false == pcall(
	-- packet is too short
	function () 
		proto.from_challenge_packet("")
	end
))

-- should decode nicely
assert(true == pcall(
	function () 
		proto.from_challenge_packet(
			"\010"..
			"5.0.24\000"..
			"\000\000\000\000".. 
			"01234567" ..
			"\000" ..
			"\000\000" ..
			"\000" ..
			"\000\000" ..
			("\000"):rep(13) ..
			("."):rep(12) ..
			"\000"
			)
	end
))

local challenge_packet = proto.to_challenge_packet({ server_status = 2, server_version = 50034 })
assert(type(challenge_packet) == "string")
assert(#challenge_packet == 53, ("expected 53, got %d"):format(#challenge_packet))
local tbl = proto.from_challenge_packet(challenge_packet)
assert(type(tbl) == "table")
assert(tbl.server_status == 2, ("expected 2, got %d"):format(tbl.server_status))
assert(tbl.server_version == 50034, ("expected 50034, got %d"):format(tbl.server_version))

---
-- response packet

-- should fail
assert(false == pcall(
	-- wrong type
	function () 
		proto.from_response_packet(nil)
	end
))

assert(false == pcall(
	-- packet is too short
	function () 
		proto.from_response_packet("")
	end
))

-- should decode nicely
assert(true == pcall(
	function () 
		proto.from_response_packet(
			"\000\000\000\000".. 
			"\000\000\000\000".. 
			"\000" ..
			("\000"):rep(23) ..
			"01234567" ..  "\000" ..
			"\020"..("."):rep(20) ..
			"\000" ..
			"foobar" .. "\000"

			)
	end
))

local response_packet = proto.to_response_packet({ username = "foobar", database = "db" })
assert(type(response_packet) == "string")
assert(#response_packet == 43, ("expected 43, got %d"):format(#response_packet))
local tbl = proto.from_response_packet(response_packet)
assert(type(tbl) == "table")
assert(tbl.username == "foobar", ("expected 'foobar', got %s"):format(tostring(tbl.username)))
assert(tbl.database == "db", ("expected 'db', got %s"):format(tostring(tbl.database)))

-- test 5.1 master.info format
local masterinfofile = proto.from_masterinfo_string(
"15\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\nca-cert.pem\n"
.. "/usr/local/mysql/ssl/ca/\nclient-cert.pem\nssl_cipher\nclient-key.pem\n0\n")
assert( masterinfofile["master_lines"] == 15)
assert( masterinfofile["master_host"] == "127.0.0.1")
assert( masterinfofile["master_log_pos"] == 2143897)
assert( masterinfofile["master_user"] == "root")
assert( masterinfofile["master_connect_retry"] == 60)
assert( masterinfofile["master_log_file"] == "hostname-bin.000024")
assert( masterinfofile["master_port"] == 3306)
assert( masterinfofile["master_password"] == "123")
assert( masterinfofile["master_ssl"] == 0) 
assert( masterinfofile["master_ssl_ca"] == "ca-cert.pem")
assert( masterinfofile["master_ssl_capath"] == "/usr/local/mysql/ssl/ca/")
assert( masterinfofile["master_ssl_cert"] == "client-cert.pem")
assert( masterinfofile["master_ssl_cipher"] == "ssl_cipher")
assert( masterinfofile["master_ssl_key"] == "client-key.pem")
assert( masterinfofile["master_ssl_verify_server_cert"] == 0)

-- test 4.1 and 5.0 master.info format
local masterinfofile = proto.from_masterinfo_string(
"14\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\nca-cert.pem\n"
.. "/usr/local/mysql/ssl/ca/\nclient-cert.pem\nssl_cipher\nclient-key.pem\n")

assert( masterinfofile["master_lines"] == 14)
assert( masterinfofile["master_host"] == "127.0.0.1")
assert( masterinfofile["master_log_pos"] == 2143897)
assert( masterinfofile["master_user"] == "root")
assert( masterinfofile["master_connect_retry"] == 60)
assert( masterinfofile["master_log_file"] == "hostname-bin.000024")
assert( masterinfofile["master_port"] == 3306)
assert( masterinfofile["master_password"] == "123")
assert( masterinfofile["master_ssl"] == 0)
assert( masterinfofile["master_ssl_ca"] == "ca-cert.pem")
assert( masterinfofile["master_ssl_capath"] == "/usr/local/mysql/ssl/ca/")
assert( masterinfofile["master_ssl_cert"] == "client-cert.pem")
assert( masterinfofile["master_ssl_cipher"] == "ssl_cipher")
assert( masterinfofile["master_ssl_key"] == "client-key.pem")
assert( masterinfofile["master_ssl_verify_server_cert"] == nil)

-- test proto.to_masterinfo_string()

local masterinfofile = proto.from_masterinfo_string(
"15\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\nca-cert.pem\n"
.. "/usr/local/mysql/ssl/ca/\nclient-cert.pem\nssl_cipher\nclient-key.pem\n0\n")

assert( proto.to_masterinfo_string(masterinfofile) == 
"15\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\nca-cert.pem\n"
.. "/usr/local/mysql/ssl/ca/\nclient-cert.pem\nssl_cipher\nclient-key.pem\n0\n")

local masterinfofile = proto.from_masterinfo_string(  
"14\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\nca-cert.pem\n"
.. "/usr/local/mysql/ssl/ca/\nclient-cert.pem\nssl_cipher\nclient-key.pem\n")

assert( proto.to_masterinfo_string(masterinfofile) ==
"14\nhostname-bin.000024\n2143897\n127.0.0.1\nroot\n123\n3306\n60\n0\nca-cert.pem\n"
.. "/usr/local/mysql/ssl/ca/\nclient-cert.pem\nssl_cipher\nclient-key.pem\n")

-- test the password functions
local challenge  = "01234567890123456789"
local cleartext  = "123"
local hashed     = password.hash(cleartext)
local dbl_hashed = password.hash(hashed)
local response   = password.scramble(challenge, hashed)
assert(password.unscramble(challenge, response, dbl_hashed) == hashed)

assert(password.check(challenge, response, dbl_hashed))

-- check that a forged response fails the password check
local challenge  = "01234567890123456789"
local cleartext  = "123"
local hashed     = password.hash(cleartext)
local dbl_hashed = password.hash(hashed)
local response   = "09876543210987654321"
assert(password.unscramble(challenge, response, dbl_hashed) ~= hashed)

assert(false == password.check(challenge, response, dbl_hashed))

---
-- prepared stmt decoders
--

-- EXECUTE packet, no params
local packet = "\023\001\000\000\000\000\001\000\000\000"
local execute = proto.from_stmt_execute_packet(packet, 0)
assert(execute)
assertEquals(execute.stmt_id, 1)
assertEquals(execute.flags, 0)
assertEquals(execute.iteration_count, 1)
assertEquals(execute.new_params_bound, false)

-- EXECUTE packet with 14 params
local packet = "\023" ..
	"\001\000\000\000" ..
	"\000" ..
	"\001\000\000\000" ..
	"\003\000" ..
	"\001" ..
	"\254\000\006\000\254\000\008\000\008\128\003\000\002\000\001\000\005\000\004\000\010\000\012\000\007\000\011\000" ..
	"\003\102\111\111" ..
	"\001\000\000\000\000\000\000\000" ..
	"\001\000\000\000\000\000\000\000" ..
	"\001\000\000\000" ..
	"\001\000" ..
	"\001" ..
	"\102\102\102\102\102\102\036\064" ..
	"\000\000\036\065" ..
	"\004\218\007\010\017" ..
	"\011\218\007\010\017\019\027\030\001\000\000\000" ..
	"\011\218\007\010\017\019\027\030\001\000\000\000" ..
	"\012\001\120\000\000\000\019\027\030\001\000\000\000"

local execute = proto.from_stmt_execute_packet(packet, 14)
assert(execute)
assertEquals(execute.stmt_id, 1)
assertEquals(execute.flags, 0)
assertEquals(execute.iteration_count, 1)
assertEquals(execute.new_params_bound, true)

local params = execute.params
assert(params)
assertEquals(#params, 14)

local expected_params = {
	{ type = 254, value = nil },
	{ type = 6, value = nil },
	{ type = 254, value = "foo" },
	{ type = 8, value = 1 },
	{ type = 8, value = 1 },
	{ type = 3, value = 1 },
	{ type = 2, value = 1 },
	{ type = 1, value = 1 },
	{ type = 5, value = 10.2 }, --[[ double ]]--
	{ type = 4, value = 10.25 }, --[[ float ]]--
	{ type = 10, value = "2010-10-17" }, --[[ date ]]--
	{ type = 12, value = "2010-10-17 19:27:30.000000001" }, --[[ datetime ]]--
	{ type = 7, value = "2010-10-17 19:27:30.000000001" }, --[[ timestamp ]]--
	{ type = 11, value = "-120 19:27:30.000000001" }, --[[ time ]]--
}

for ndx, expected_param in ipairs(expected_params) do
	local param = params[ndx]
	assert(param)
	assertEquals(param.type, expected_param.type)
	assertEquals(param.value, expected_param.value)
end

