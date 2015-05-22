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
local tests = require("proxy.test")

---
-- unit-test the unit-test

--- 
-- a mock for a broken compile test
TestUnknown = tests.BaseTest:new()
function TestUnknown:testUnknownFunction()
	unknownfunction()
end

TestFailed = tests.BaseTest:new()
function TestFailed:testFailedAssert()
	assertEquals(1, 2)
end

TestPassed = tests.BaseTest:new()
function TestPassed:testPassedAssert()
	assertEquals(1, 1)
end


TestUnit = tests.BaseTest:new()
function TestUnit:testUnknownFunction()
	local unknown = tests.Suite:new({
		result = tests.Result:new()
	})

	unknown:run({ "TestUnknown" })

	assertEquals(unknown.result.passed, 0)
	assertEquals(unknown.result.failed, 1)
end

function TestUnit:testFailedAssert()
	local failed = tests.Suite:new({
		result = tests.Result:new()
	})

	failed:run({ "TestFailed" })

	assertEquals(failed.result.passed, 0)
	assertEquals(failed.result.failed, 1)
end

function TestUnit:testPassedAssert()
	local passed = tests.Suite:new({
		result = tests.Result:new()
	})

	passed:run({ "TestPassed" })

	assertEquals(passed.result.passed, 1)
	assertEquals(passed.result.failed, 0)
end

function TestUnit:testAssertEquals()
	assertEquals(1, 1)
	assertEquals(0, 0)
	assertEquals(nil, nil)
end

function TestUnit:testAssertNotEquals()
	assertNotEquals(1, 0)
	assertNotEquals(0, 1)
	assertNotEquals(nil, 0)
end

TestProxyUnit = tests.ProxyBaseTest:new()
function TestProxyUnit:testDefaultScope()
	assertEquals(_G.proxy.global, nil)
	self:setDefaultScope()
	assertNotEquals(_G.proxy.global, nil)
end

function TestProxyUnit:tearDown() 
	_G.proxy.global = nil
end


local suite = tests.Suite:new({
	result = tests.Result:new()
})

suite:run({"TestUnit", "TestProxyUnit"})
suite.result:print()
return suite:exit_code()
