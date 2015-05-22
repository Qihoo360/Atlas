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
-- a first implementation of the a script test-suite
--
-- it is inspired by all the different unit-testsuites our there and
-- provides 
-- * a set of assert functions
-- * a fake proxy implementation
--
-- @todo the local script scope isn't managed yet

local tests = require("proxy.test")
tests.ProxyBaseTest:setDefaultScope()

-- file under test
require("active-queries")

---
-- overwrite the scripts dump_global_state
-- to get rid of the printout
--
function print_stats(stats)
end

function mock_tokenize(query)
	-- parsing a query
	if query == "SELECT 1" then
		return {
			{ token_name = "TK_SQL_SELECT", text = "SELECT" },
			{ token_name = "TK_NUMBER",     text = "1" }
		}
	else 
		error("(mock_tokenize) for "..query)
	end
end

TestScript = tests.ProxyBaseTest:new({ 
	active_qs = proxy.global.active_queries
})

function TestScript:setUp()
	-- reset the query queue
	proxy.queries:reset()

	self:setDefaultScope()

	proxy.tokenize = mock_tokenize

	proxy.global.max_active_trx = 0
	proxy.global.active_queries = { }

	proxy.connection.server.thread_id = 1
end

function TestScript:testInit()
	assertEquals(type(self.active_qs), "table")
end

function TestScript:testCleanStats()
	local stats = collect_stats()

	assertEquals(stats.max_active_trx, 0)
	assertEquals(stats.num_conns, 0)
end

function TestScript:testQueryQueuing()
	-- send a query in
	assertNotEquals(read_query(string.char(proxy.COM_QUERY) .. "SELECT 1"), nil)
	
	local stats = collect_stats()
	assertEquals(stats.max_active_trx, 1)
	assertEquals(stats.num_conns, 1)
	assertEquals(stats.active_conns, 1)
	
	-- and here is the result
	assertEquals(proxy.queries:len(), 1)
end	

function TestScript:testQueryTracking()
	inj = { 
		id = 1, 
		query = string.char(proxy.COM_QUERY) .. "SELECT 1"
	}

	-- setup the query queue
	assertNotEquals(read_query(inj.query), nil)

	local stats = collect_stats()
	assertEquals(stats.num_conns, 1)
	assertEquals(stats.active_conns, 1)
	
	-- check if the stats are updated
	assertEquals(read_query_result(inj), nil)

	local stats = collect_stats()
	assertEquals(stats.num_conns, 1)
	assertEquals(stats.active_conns, 0)
	assertEquals(stats.max_active_trx, 1)
end
	
function TestScript:testDisconnect()
	inj = { 
		id = 1, 
		query = string.char(proxy.COM_QUERY) .. "SELECT 1"
	}

	-- exec some queries
	assertNotEquals(read_query(inj.query), nil)
	assertEquals(read_query_result(inj), nil)

	local stats = collect_stats()
	assertEquals(stats.num_conns, 1)

	-- the disconnect should set the num_conns to 0 again
	assertEquals(disconnect_client(), nil)
	
	local stats = collect_stats()
	assertEquals(stats.num_conns, 0)
	assertEquals(stats.active_conns, 0)
	assertEquals(stats.max_active_trx, 1)
end

---
-- the test suite runner

local suite = tests.Suite:new({ result = tests.Result:new()})

suite:run()
suite.result:print()
return suite:exit_code()
