---
-- forward the query AS IS and add a query to the queue,
-- but don't mark it as "SEND_QUERY"
--
-- the query in the queue will never be touched, as the default behaviour
-- is to just forward the result back to the client without buffering 
-- it
function read_query( packet )
	if packet:byte() == proxy.COM_QUERY then
		proxy.queries:append(1, packet, { resultset_is_needed = true } )
	end

	-- forward the incoming query AS IS
end

---
-- try access the resultset 
-- 
function read_query_result(inj)
	local res = assert(inj.resultset)

	if res.query_status == proxy.MYSQLD_PACKET_ERR then
		print(("received error-code: %d"):format(
			res.raw:byte(2)+(res.raw:byte(3)*256)
		))
	end
end
