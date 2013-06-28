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

local tokenizer    = require("proxy.tokenizer")
local parser       = require("proxy.parser")
local commands     = require("proxy.commands")
local auto_config  = require("proxy.auto-config")

-- init global counters

if not proxy.global.config.histogram then
	proxy.global.config.histogram = {
		collect_queries = true,
		collect_tables = true
	}
end

-- init query counters
if not proxy.global.norm_queries then
	proxy.global.norm_queries = { }
end

-- init table-usage
if not proxy.global.tables then
	proxy.global.tables = { }
end

function read_query(packet)
	local cmd = commands.parse(packet)

	local r = auto_config.handle(cmd)
	if r then return r end

	if cmd.type == proxy.COM_QUERY then
		local tokens     = assert(tokenizer.tokenize(cmd.query))
		local norm_query = tokenizer.normalize(tokens)

		-- print("normalized query: " .. norm_query)

		if norm_query == "SELECT * FROM `histogram` . `queries` " then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
				resultset = {
					fields = { 
						{ type = proxy.MYSQL_TYPE_STRING,
						  name = "query" },
						{ type = proxy.MYSQL_TYPE_LONG,
						  name = "count" },
						{ type = proxy.MYSQL_TYPE_LONG,
						  name = "max_query_time" },
						{ type = proxy.MYSQL_TYPE_FLOAT,
						  name = "avg_query_time" },
					}
				}
			}

			local rows = {}
			if proxy.global.norm_queries then
				for k, v in pairs(proxy.global.norm_queries) do
					rows[#rows + 1] = { 
						k, 
						v.count,
						v.max_query_time,
						v.avg_query_time,
					}
				end
			end
			
			proxy.response.resultset.rows = rows

			return proxy.PROXY_SEND_RESULT
		elseif norm_query == "DELETE FROM `histogram` . `queries` " then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
			}

			proxy.global.norm_queries = {}
			return proxy.PROXY_SEND_RESULT
		elseif norm_query == "SELECT * FROM `histogram` . `tables` " then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
				resultset = {
					fields = { 
						{ type = proxy.MYSQL_TYPE_STRING,
						  name = "table" },
						{ type = proxy.MYSQL_TYPE_LONG,
						  name = "reads" },
						{ type = proxy.MYSQL_TYPE_LONG,
						  name = "writes" },
					  }
				}
			}

			local rows = {}
			if proxy.global.tables then
				for k, v in pairs(proxy.global.tables) do
					rows[#rows + 1] = { 
						k, 
						v.reads,
						v.writes,
					}
				end
			end
			
			proxy.response.resultset.rows = rows

			return proxy.PROXY_SEND_RESULT
		elseif norm_query == "DELETE FROM `histogram` . `tables` " then
			proxy.response = {
				type = proxy.MYSQLD_PACKET_OK,
			}

			proxy.global.tables = {}
			return proxy.PROXY_SEND_RESULT
		end

		if proxy.global.config.histogram.collect_queries or
		   proxy.global.config.histogram.collect_tables then
			proxy.queries:append(1, packet)

			return proxy.PROXY_SEND_QUERY
		end
	end
end

function read_query_result(inj) 
	local cmd = commands.parse(inj.query)

	if cmd.type == proxy.COM_QUERY then
		local tokens     = assert(tokenizer.tokenize(cmd.query))
		local norm_query = tokenizer.normalize(tokens)

		if proxy.global.config.histogram.collect_queries then
			if not proxy.global.norm_queries[norm_query] then
				proxy.global.norm_queries[norm_query] = {
					count = 0,
					max_query_time = 0,
					avg_query_time = 0
				}
			end
	
			-- set new max if necessary
			if inj.query_time > proxy.global.norm_queries[norm_query].max_query_time then
				proxy.global.norm_queries[norm_query].max_query_time = inj.query_time
			end
	
			-- build rolling average
			proxy.global.norm_queries[norm_query].avg_query_time = 
				((proxy.global.norm_queries[norm_query].avg_query_time * proxy.global.norm_queries[norm_query].count) +
					inj.query_time) / (proxy.global.norm_queries[norm_query].count + 1)
			
			proxy.global.norm_queries[norm_query].count = proxy.global.norm_queries[norm_query].count + 1
		end
	
		if proxy.global.config.histogram.collect_tables then
			-- extract the tables from the queries
			tables = parser.get_tables(tokens)
	
			for table, qtype in pairs(tables) do
				if not proxy.global.tables[table] then
					proxy.global.tables[table] = {
						reads = 0,
						writes = 0
					}
				end
	
				if qtype == "read" then
					proxy.global.tables[table].reads = proxy.global.tables[table].reads + 1
				else
					proxy.global.tables[table].writes = proxy.global.tables[table].writes + 1
				end
			end
		end
	end
end
