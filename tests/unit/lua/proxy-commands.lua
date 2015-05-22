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
-- test the proxy.commands module
--
-- 
-- file under test
local cmds  = require("proxy.commands")
local tests = require("proxy.test")

TestScript = tests.ProxyBaseTest:new()

tests.ProxyBaseTest:setDefaultScope()

function TestScript:testParseCOM_QUERY()
	local cmd = cmds.parse(string.char(proxy.COM_QUERY) .. "SELECT 1")

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_QUERY)
	assertEquals(cmd.query, "SELECT 1")
end

function TestScript:testParseCOM_STMT_PREPARE()
	local cmd = cmds.parse(string.char(proxy.COM_STMT_PREPARE) .. "SELECT 1")

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_STMT_PREPARE)
	assertEquals(cmd.query, "SELECT 1")
end


function TestScript:testParseCOM_INIT_DB()
	local cmd = cmds.parse(string.char(proxy.COM_INIT_DB) .. "schema")

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_INIT_DB)
	assertEquals(cmd.schema, "schema")
end

function TestScript:testParseCOM_FIELD_LIST()
	local cmd = cmds.parse(string.char(proxy.COM_FIELD_LIST) .. "table")

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_FIELD_LIST)
	assertEquals(cmd.table, "table")
end

function TestScript:testParseCOM_QUIT()
	local cmd = cmds.parse(string.char(proxy.COM_QUIT))

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_QUIT)
end

function TestScript:testParseCOM_SHUTDOWN()
	local cmd = cmds.parse(string.char(proxy.COM_SHUTDOWN))

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_SHUTDOWN)
end

function TestScript:testParseCOM_PING()
	local cmd = cmds.parse(string.char(proxy.COM_PING))

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_PING)
end

function TestScript:testParseCOM_CREATE_DB()
	local cmd = cmds.parse(string.char(proxy.COM_CREATE_DB) .. "schema")

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_CREATE_DB)
	assertEquals(cmd.schema, "schema")
end

function TestScript:testParseCOM_DROP_DB()
	local cmd = cmds.parse(string.char(proxy.COM_DROP_DB) .. "schema")

	assertNotEquals(cmd, nil)
	assertEquals(cmd.type, proxy.COM_DROP_DB)
	assertEquals(cmd.schema, "schema")
end

---
-- the test suite runner

local suite = tests.Suite:new({ result = tests.Result:new()})

suite:run()
suite.result:print()
return suite:exit_code()
