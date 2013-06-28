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
local tokenizer = require("proxy.tokenizer")

TestScript = tests.ProxyBaseTest:new()

function TestScript:setUp()
	proxy.global.config.test = { }
end

function TestScript:testNormalize()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "SET GLOBAL unknown.unknown = 1")

	local tokens = tokenizer.tokenize(cmd.query);
	local norm_query = tokenizer.normalize(tokens)

	assertEquals(norm_query, "SET `GLOBAL` `unknown` . `unknown` = ? ")
end

---
-- test if we can access the fields step-by-step and out-of-range
function TestScript:testFields()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "FOO BAR")

	local tokens = tokenizer.tokenize(cmd.query);

	assertEquals(tokens[0], nil) -- out of range
	assertEquals(tokens[1].text, "FOO")
	assertEquals(tokens[2].text, "BAR")
	assertEquals(tokens[3], nil) -- out of range
end

---
-- test if empty -- comments are detected
function TestScript:testComments()
	local cmd = command.parse(string.char(proxy.COM_QUERY) .. "--\nSELECT 1\n")

	local tokens = tokenizer.tokenize(cmd.query)
	
	assertEquals(tokens[1].token_name, "TK_COMMENT")
	assertEquals(tokens[1].text, "")
	assertEquals(tokens[2].token_name, "TK_SQL_SELECT")
	assertEquals(tokens[2].text, "SELECT")
	assertEquals(tokens[3].token_name, "TK_INTEGER")
	assertEquals(tokens[3].text, "1")
end

---
-- test if function names are detected
function TestScript:testFunctions()
	local tokens = tokenizer.tokenize("CALL foo.bar(foo)")
	
	assertEquals(tokens[1].token_name, "TK_SQL_CALL")
	assertEquals(tokens[1].text, "CALL")
	assertEquals(tokens[2].token_name, "TK_LITERAL")
	assertEquals(tokens[2].text, "foo")
	assertEquals(tokens[3].token_name, "TK_DOT")
	assertEquals(tokens[3].text, ".")
	assertEquals(tokens[4].token_name, "TK_FUNCTION")
	assertEquals(tokens[4].text, "bar")
end


---
-- test if empty -- comments are detected
function TestScript:testUnset()
	local tokens = tokenizer.tokenize("SELECT 1")
	
	assertEquals(tokens[1].token_name, "TK_SQL_SELECT")
	assertEquals(tokens[1].text, "SELECT")
	assertEquals(tokens[2].token_name, "TK_INTEGER")
	assertEquals(tokens[2].text, "1")

	tokens[1] = nil

	assertEquals(tokens[1], nil)
end



---
-- the test suite runner

local suite = tests.Suite:new({ result = tests.Result:new()})

suite:run()
suite.result:print()
return suite:exit_code()
