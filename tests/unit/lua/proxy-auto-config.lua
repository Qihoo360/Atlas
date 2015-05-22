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
-- test the proxy.auto-config module
--
-- 

local tests      = require("proxy.test")
local command    = require("proxy.commands")
tests.ProxyBaseTest.setDefaultScope()

-- file under test
local autoconfig = require("proxy.auto-config")

TestScript = tests.ProxyBaseTest:new()

function TestScript:setUp()
	self:setDefaultScope()

	proxy.global.config.test = { }
end

function TestScript:testUnknownOption()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "PROXY SET GLOBAL unknown.unknown = 1")

	local r = autoconfig.handle(proxy.global.config, cmd)

	assertEquals(r, proxy.PROXY_SEND_RESULT)
	assertEquals(proxy.response.type, proxy.MYSQLD_PACKET_ERR)
	assert(proxy.response.errmsg > "")
end

function TestScript:testKnownModule()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "PROXY SET GLOBAL test.unknown = 1")

	local r = autoconfig.handle(proxy.global.config, cmd)

	assertEquals(proxy.response.type, proxy.MYSQLD_PACKET_ERR)
	assert(proxy.response.errmsg > "")
end

function TestScript:testShowConfig()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "PROXY SHOW CONFIG")

	local r = autoconfig.handle(proxy.global.config, cmd)

	assertEquals(r, proxy.PROXY_SEND_RESULT)
end


function TestScript:testKnownModule()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "PROXY SET GLOBAL test.option = 1")

	proxy.global.config.test.option = 0

	local r = autoconfig.handle(proxy.global.config, cmd)

	assertEquals(r, proxy.PROXY_SEND_RESULT)
	assertEquals(proxy.response.type, proxy.MYSQLD_PACKET_OK)
	assertEquals(proxy.global.config.test.option, 1)
end

function TestScript:testKnownModuleWrongType()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "PROXY SET GLOBAL test.option = \"foo\"")

	proxy.global.config.test.option = 0

	local r = autoconfig.handle(proxy.global.config, cmd)

	assertEquals(r, proxy.PROXY_SEND_RESULT)
	assertEquals(proxy.response.type, proxy.MYSQLD_PACKET_ERR)
	assert(proxy.response.errmsg > "")
end


---
-- the test suite runner

local suite = tests.Suite:new({ result = tests.Result:new()})

suite:run()
suite.result:print()
os.exit(suite:exit_code())
