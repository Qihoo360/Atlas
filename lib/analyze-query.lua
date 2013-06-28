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

local commands     = require("proxy.commands")
local tokenizer    = require("proxy.tokenizer")
local auto_config  = require("proxy.auto-config")

---
-- configuration
--
-- SET GLOBAL analyze_query.auto_filter = 0
if not proxy.global.config.analyze_query then
	proxy.global.config.analyze_query = {
		analyze_queries  = true,   -- track all queries
		auto_explain     = false,  -- execute a EXPLAIN on SELECT queries after we logged
		auto_processlist = false,  -- execute a SHOW FULL PROCESSLIST after we logged
		min_queries      = 1000,   -- start logging after evaluating 1000 queries
		min_query_time   = 1000,   -- don't log queries less then 1ms
		query_cutoff     = 160,    -- only show the first 160 chars of the query
		log_slower_queries = false -- log the query if it is slower than the queries of the same kind
	}
end

--- dump the result-set to stdout
--
-- @param inj "packet.injection"
function resultset_to_string( res )
	local s = ""
	local fields = res.fields

	if not fields then return "" end

	local field_count = #fields

	local row_count = 0
	for row in res.rows do
		local o
		local cols = {}

		for i = 1, field_count do
			if not o then
				o = ""
			else 
				o = o .. ", "
			end

			if not row[i] then
				o = o .. "NULL"
			-- TODO check if the indexes are in range 
			elseif fields[i].type == proxy.MYSQL_TYPE_STRING or  
				fields[i].type == proxy.MYSQL_TYPE_VAR_STRING or 
				fields[i].type == proxy.MYSQL_TYPE_LONG_BLOB or 
				fields[i].type == proxy.MYSQL_TYPE_MEDIUM_BLOB then
				o = o .. string.format("%q", row[i])
			else
				-- print("  [".. i .."] field-type: " .. fields[i].type)
				o = o .. row[i]
			end
		end
		s = s .. ("  ["..row_count.."]{ " .. o .. " }\n")
		row_count = row_count + 1
	end

	return s
end


function math.rolling_avg_init()
	return { count = 0, value = 0, sum_value = 0 }
end

function math.rolling_stddev_init()
	return { count = 0, value = 0, sum_x = 0, sum_x_sqr = 0 }
end

function math.rolling_avg(val, tbl) 
	tbl.count = tbl.count + 1
	tbl.sum_value = tbl.sum_value + val

	tbl.value = tbl.sum_value / tbl.count

	return tbl.value
end


function math.rolling_stddev(val, tbl)
	tbl.sum_x     = tbl.sum_x + val
	tbl.sum_x_sqr = tbl.sum_x_sqr + (val * val)
	tbl.count     = tbl.count + 1

	tbl.value = math.sqrt((tbl.count * tbl.sum_x_sqr - (tbl.sum_x * tbl.sum_x)) / (tbl.count * (tbl.count - 1)))

	return tbl.value
end

---
-- init query counters
-- 
-- the normalized queries are 
if not proxy.global.queries then
	proxy.global.queries = { }
end

if not proxy.global.baseline then
	proxy.global.baseline = {
		avg = math.rolling_avg_init(),
		stddev = math.rolling_stddev_init()
	}
end


function read_query(packet) 
	local cmd = commands.parse(packet)

	local r = auto_config.handle(cmd)
	if r then return r end

	-- analyzing queries is disabled, just pass them on
	if not proxy.global.config.analyze_query.analyze_queries then
		return
	end

	-- we only handle normal queries
	if cmd.type ~= proxy.COM_QUERY then
		return
	end

	tokens = tokenizer.tokenize(cmd.query)
	norm_query = tokenizer.normalize(tokens)

	-- create a id for this query
	query_id   = ("%s.%s.%s"):format(
		proxy.connection.backend_ndx, 
		proxy.connection.client.default_db ~= "" and 
			proxy.connection.client.default_db or 
			"(null)", 
		norm_query)

	-- handle the internal data
	if norm_query == "SELECT * FROM `histogram` . `queries` " then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
			resultset = {
				fields = { 
					{ type = proxy.MYSQL_TYPE_STRING,
					  name = "query" },
					{ type = proxy.MYSQL_TYPE_LONG,
					  name = "COUNT(*)" },
					{ type = proxy.MYSQL_TYPE_DOUBLE,
					  name = "AVG(qtime)" },
					{ type = proxy.MYSQL_TYPE_DOUBLE,
					  name = "STDDEV(qtime)" },
				}
			}
		}

		local rows = {}
		if proxy.global.queries then
			local cutoff = proxy.global.config.analyze_query.query_cutoff

			for k, v in pairs(proxy.global.queries) do
				local q = v.query

				if cutoff and cutoff < #k then
					q = k:sub(1, cutoff) .. "..."
				end

				rows[#rows + 1] = { 
					q,
					v.avg.count,
					string.format("%.2f", v.avg.value),
					v.stddev.count > 1 and string.format("%.2f", v.stddev.value) or nil
				}
			end
		end
		
		proxy.response.resultset.rows = rows

		return proxy.PROXY_SEND_RESULT
	elseif norm_query == "DELETE FROM `histogram` . `queries` " then
		proxy.response = {
			type = proxy.MYSQLD_PACKET_OK,
		}
		proxy.global.queries = {}
		return proxy.PROXY_SEND_RESULT
	end

	-- we are connection-global
	baseline = nil
	log_query = false

	---
	-- do we want to analyse this query ?
	--
	--
	local queries = proxy.global.queries

	if not queries[query_id] then
		queries[query_id] = {
			db     = proxy.connection.client.default_db,
			query  = norm_query,
			avg    = math.rolling_avg_init(),
			stddev = math.rolling_stddev_init()
		}
	end


	if queries[query_id].do_analyze then
		-- wrap the query in SHOW SESSION STATUS
		--
		-- note: not used yet
		proxy.queries:append(2, string.char(proxy.COM_QUERY) .. "SHOW SESSION STATUS", { resultset_is_needed = true })
		proxy.queries:append(1, packet, { resultset_is_needed = true }) -- FIXME: this should work without it being set (inj.resultset.query_status)
		proxy.queries:append(3, string.char(proxy.COM_QUERY) .. "SHOW SESSION STATUS", { resultset_is_needed = true })
	else
		proxy.queries:append(1, packet, { resultset_is_needed = true }) -- FIXME: this one too
	end

	return proxy.PROXY_SEND_QUERY
end

---
-- normalize a timestamp into a string
--
-- @param ts time in microseconds
-- @return a string with a useful suffix
function normalize_time(ts)
	local suffix = "us"
	if ts > 10000 then
		ts = ts / 1000
		suffix = "ms"
	end
	
	if ts > 10000 then
		ts = ts / 1000
		suffix = "s"
	end

	return string.format("%.2f", ts) .. " " .. suffix
end

function read_query_result(inj)
	local res = assert(inj.resultset)
	local config = proxy.global.config.analyze_query

	if inj.id == 1 then
		log_query = false
		if not res.query_status or res.query_status == proxy.MYSQLD_PACKET_ERR then
			-- the query failed, let's clean the queue
			--
			proxy.queries:reset()
		end

		-- get the statistics values for the query
		local bl = proxy.global.baseline
		local avg_query_time    = math.rolling_avg(   inj.query_time, bl.avg)
		local stddev_query_time = math.rolling_stddev(inj.query_time, bl.stddev)

		local st = proxy.global.queries[query_id]
		local q_avg_query_time    = math.rolling_avg(   inj.query_time, st.avg)
		local q_stddev_query_time = math.rolling_stddev(inj.query_time, st.stddev)

		o = "# " .. os.date("%Y-%m-%d %H:%M:%S") .. 
			" [".. proxy.connection.server.thread_id .. 
			"] user: " .. proxy.connection.client.username .. 
			", db: " .. proxy.connection.client.default_db .. "\n" ..
			"  Query:                     " .. string.format("%q", inj.query:sub(2)) .. "\n" ..
			"  Norm_Query:                " .. string.format("%q", norm_query) .. "\n" ..
			"  query_time:                " .. normalize_time(inj.query_time) .. "\n" ..
			"  global(avg_query_time):    " .. normalize_time(avg_query_time) .. "\n" ..
			"  global(stddev_query_time): " .. normalize_time(stddev_query_time) .. "\n" ..
			"  global(count):             " .. bl.avg.count .. "\n" ..
			"  query(avg_query_time):     " .. normalize_time(q_avg_query_time) .. "\n" ..
			"  query(stddev_query_time):  " .. normalize_time(q_stddev_query_time) .. "\n" ..
			"  query(count):              " .. st.avg.count .. "\n" 


		-- we need a min-query-time to filter out
		if inj.query_time >= config.min_query_time then
			-- this query is slower than 95% of the average
			if bl.avg.count > config.min_queries and
			   inj.query_time > avg_query_time + 5 * stddev_query_time then
				o = o .. "  (qtime > global-threshold)\n"
				log_query = true
			end
	
			-- this query was slower than 95% of its kind
			if config.log_slower_queries and
			   st.avg.count > config.min_queries and 
			   inj.query_time > q_avg_query_time + 5 * q_stddev_query_time then
				o = o .. "  (qtime > query-threshold)\n"
				log_query = true
			end
		end

		if log_query and config.auto_processlist then
			proxy.queries:append(4, string.char(proxy.COM_QUERY) .. "SHOW FULL PROCESSLIST",
				{ resultset_is_needed = true })
		end
	
		if log_query and config.auto_explain then
			if tokens[1] and tokens[1].token_name == "TK_SQL_SELECT" then
				proxy.queries:append(5, string.char(proxy.COM_QUERY) .. "EXPLAIN " .. inj.query:sub(2),
					{ resultset_is_needed = true })
			end
		end

	elseif inj.id == 2 then
		-- the first SHOW SESSION STATUS
		baseline = {}
		
		for row in res.rows do
			-- 1 is the key, 2 is the value
			baseline[row[1]] = row[2]
		end
	elseif inj.id == 3 then
		local delta_counters = { }
		
		for row in res.rows do
			if baseline[row[1]] then
				local num1 = tonumber(baseline[row[1]])
				local num2 = tonumber(row[2])

				if row[1] == "Com_show_status" then
					num2 = num2 - 1
				elseif row[1] == "Questions" then
					num2 = num2 - 1
				elseif row[1] == "Last_query_cost" then
					num1 = 0
				end
				
				if num1 and num2 and num2 - num1 > 0 then
					delta_counters[row[1]] = (num2 - num1)
				end
			end
		end
		baseline = nil
	
		--- filter data
		-- 
		-- we want to see queries which 
		-- - trigger a tmp-disk-tables
		-- - are slower than the average
		--

		if delta_counters["Created_tmp_disk_tables"] then
			log_query = true
		end

		if log_query then
			for k, v in pairs(delta_counters) do
				o = o .. ".. " .. row[1] .. " = " .. (num2 - num1) .. "\n"
			end
		end
	elseif inj.id == 4 then
		-- dump the full result-set to stdout
		o = o .. "SHOW FULL PROCESSLIST\n"
		o = o .. resultset_to_string(inj.resultset)
	elseif inj.id == 5 then
		-- dump the full result-set to stdout
		o = o .. "EXPLAIN <query>\n"
		o = o .. resultset_to_string(inj.resultset)
	end

	if log_query and proxy.queries:len() == 0 then
		print(o)
		o = nil
		log_query = false
	end

	-- drop all resultsets but the original one
	if inj.id ~= 1 then return proxy.PROXY_IGNORE_RESULT end
end
