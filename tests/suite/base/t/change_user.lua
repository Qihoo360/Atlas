require("posix")

function read_query (packet)
	-- ack the packets
	if packet:byte() ~= proxy.COM_QUERY then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK
		}
		return proxy.PROXY_SEND_RESULT
	end

	local pw = posix.getpwuid( posix.getuid() )
	local user
	if pw then
		user = pw['name']
	else
		user = "nil"
	end

	proxy.response.type = proxy.MYSQLD_PACKET_OK
	proxy.response.resultset = {
		fields = {
			{ type = proxy.MYSQL_TYPE_STRING, name = "user", },
		},
		rows = { { user }  }
	}
	return proxy.PROXY_SEND_RESULT
end
