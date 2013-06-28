---
-- Bug #35729
--
-- a value from the 1st column randomly becomes nil even if we send back
-- a value from the mock server
--
-- it only happens for the 2nd resultset we receive

---
-- duplicate the test-query
function read_query( packet )
	if packet:byte() == proxy.COM_QUERY then
		proxy.queries:append(1, packet, { resultset_is_needed = true } )
		return proxy.PROXY_SEND_QUERY
	end
end

---
-- check that the 2nd resultset is NULL-free
--
-- the underlying result is fine and contains the right data, just the Lua 
-- side gets the wrong values
--
function read_query_result(inj)
	local fields = inj.resultset.fields
	collectgarbage("collect") -- trigger a full GC

	assert(type(fields) == "userdata")
	assert(type(fields[1]) == "userdata") -- if the GC removed the underlying c-struct, fields[1] will be nil 

	proxy.response = {
		type = proxy.MYSQLD_PACKET_OK,
		resultset = {
			fields = { },
			rows = { }
		}
	}

	proxy.response.resultset.fields[1] = {
		name = fields[1].name, type = fields[1].type
	}
	proxy.response.resultset.fields[2] = {
		name = fields[2].name, type = fields[2].type
	}
	for row in inj.resultset.rows do
		collectgarbage("collect") -- trigger a full GC

		---
		-- if something goes wrong 'row' will reference a free()ed old resultset now
		-- leading to nil here
		proxy.response.resultset.rows[#proxy.response.resultset.rows + 1] = { row[1], row[2] }
	end

	return proxy.PROXY_SEND_RESULT
end
