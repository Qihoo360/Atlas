module("proxy.split", package.seeall)

--local table  = require("proxy.table")
local crc    = require("proxy.crc32")
local log    = require("proxy.log")
local filter = require("proxy.filter") 
local config_file = string.format("proxy.conf.config_%s", proxy.global.config.instance)
local config	  = require(config_file)
local level = log.level
local write_log = log.write_log

--[[
-- Find Table Name's Index Number From Tokens
--  @param tokens       - ARRAY
--  @param break_token  - STRING    do break when foreach
--  @param start_token  - INT       start number when foreach
--
]]
function __findtable_form_tokens(tokens, break_token, start_token)
	write_log(level.DEBUG, "ENTER __FINDTABLE_FORM_TOKENS")
   
	local index = {}
	index.d = 0
	index.t = 0

    for j = start_token + 1, #tokens-1 do
        if tokens[j].token_name == break_token then break end

		if tokens[j].token_name == "TK_LITERAL" then
			if tokens[j+1].text ~= "." then	--适应table
				index.t = j
				break
			else	--适应db.table
				index.d = j
				index.t = j+2
			end
		end
    end 

	write_log(level.DEBUG, "LEAVE __FINDTABLE_FORM_TOKENS")

	return index
end

function __findtable_form_insert(tokens)
	write_log(level.DEBUG, "ENTER __FINDTABLE_FORM_INSERT")

	local index = {}
	index.d = 0
	index.t = 0

	for j = 2, #tokens-1 do
		local text = string.upper(tokens[j].text)
		if text == "VALUES" or text == "VALUE" then break end 

		if tokens[j].token_name == "TK_LITERAL" and tokens[j+1].text == "." then -- 适应 db.table ...
			index.d = j
			index.t = j+2
			break
		elseif (tokens[j].token_name == "TK_LITERAL" or tokens[j].token_name == "TK_FUNCTION") then
			local next_text = string.upper(tokens[j+1].text)
			if next_text == "VALUES" or next_text == "VALUE" or next_text == "(" or next_text == "SET" then -- 适应 table
				index.t = j
				break
			end
		end
	end 

	write_log(level.DEBUG, "LEAVE __FINDTABLE_FORM_INSERT")

	return index
end

function get_table_index(tokens, sql_type)
	write_log(level.DEBUG, "ENTER GET_TABLE_INDEX")

	local dt_index = {}
	dt_index.d = 0
	dt_index.t = 0

	if sql_type == 1 then   -- SELECT
        -- find 'from' index
        for i = 2, #tokens-1 do
            if tokens[i].token_name == "TK_SQL_FROM" then
                dt_index = __findtable_form_tokens(tokens, 'TK_WHERE', i)
            end
        end
    elseif sql_type == 2 then   --UPDATE语句
        dt_index = __findtable_form_tokens(tokens, 'TK_SQL_SET', 1)
    elseif sql_type == 3 then   --INSERT语句，没有同时INSERT多个表的情况
        dt_index = __findtable_form_insert(tokens)
    end 

	if dt_index.d == 0 then
		write_log(level.INFO, "db not found")
	else
		write_log(level.INFO, "db = ", tokens[dt_index.d].text)
	end

	if dt_index.t == 0 then
		write_log(level.INFO, "table not found")
	else
		write_log(level.INFO, "table = ", tokens[dt_index.t].text)
	end

	write_log(level.DEBUG, "LEAVE GET_TABLE_INDEX")

	return dt_index
end

function get_colum_index(tokens, g_table_arr, sql_type, start)	--查找名为property的字段，找到时返回该token的index值，否则返回0
	write_log(level.DEBUG, "ENTER GET_COLUMN_INDEX")

	local index = {}
	local m = 1

	if sql_type == 1 then	--SELECT或DELETE语句
        for i = start, #tokens-3 do
            if tokens[i].token_name == "TK_SQL_WHERE" then
                for j = i+1, #tokens-2 do
                    if tokens[j].token_name == "TK_LITERAL" and tokens[j].text == g_table_arr.property then 
						if tokens[j+1].text == "=" then	--字段名后面是等号，说明是单值
							if tokens[j-1].text ~= "." or tokens[j-2].text == g_table_arr.name then
								index[m] = j + 2
								break
							end
						elseif tokens[j+1].text:upper() == "IN" and tokens[j+2].text == "(" then	--字段名后面是IN和左括号，说明是多值
							local k = j + 2
							while tokens[k].text~= ")" do 
								index[m] = k + 1
								k = k + 2
								m = m + 1
							end
						end 
                    end 
                end 
            end 
        end 

    elseif sql_type == 2 then	--UPDATE语句
        for i = start, #tokens-3 do
            if tokens[i].token_name == "TK_SQL_WHERE" then
                for j = i+1, #tokens-2 do
                    if tokens[j].token_name == "TK_LITERAL" and tokens[j].text == g_table_arr.property and tokens[j+1].text == "=" then
                        if tokens[j-1].text ~= "." or tokens[j-2].text == g_table_arr.name then
							index[m] = j + 2
							break
                        end 
                    end 
                end 
            end 
        end 

    elseif sql_type == 3 then	--INSERT或REPLACE语句
		local start_text = string.upper(tokens[start].text)

		if start_text == "SET" then	--"set 属性 = 值"的形式
			for i = start+1, #tokens-2 do
				if tokens[i].text == g_table_arr.property and tokens[i].token_name == "TK_LITERAL" then
					index[m] = i + 2
					break
				end 
			end
		else
			local k = 2 
			if start_text == "(" then --带有字段名的情况
				local found = nil 
				for j = start+1, #tokens-3 do
					if tokens[j].text == ")" then break end	--找到右括号则跳出循环
					if tokens[j].text == g_table_arr.property and tokens[j].token_name == "TK_LITERAL" then
						if tokens[j-1].text ~= "." or tokens[j-2].text == g_table_arr.name then
							found = j 
							break
						end 
					end 
				end 
				k = found - start + 1 
			end 

			for i = start, #tokens-3 do
				local text = string.upper(tokens[i].text)
				if (text == "VALUES" or text == "VALUE") and tokens[i+1].text == "(" and string.match(tokens[i+k].text, "^%d+$") then
					index[m] = i + k
					break
				end 
			end
		end
    end 

	if #index == 0 then
		write_log(level.INFO, "column not found")
	else
		for i = 1, #index do
			write_log(level.INFO, "column = ", tokens[index[i]].text)
		end
	end

	write_log(level.DEBUG, "LEAVE GET_COLUMN_INDEX")
	return index
end

function combine_sql(tokens, table_index, colum_index, g_table_arr)
	write_log(level.DEBUG, "ENTER COMBINE_SQL")

	local partition = g_table_arr.partition
	local sqls = {}

	if #colum_index == 1 then
		local sql = ""
		if tokens[1].token_name ~= "TK_COMMENT" then sql = tokens[1].text end
		for i = 2, #tokens do
			if tokens[i].text ~= "(" then
				sql = sql .. ' '
			end

			if i == table_index then
				sql = sql .. tokens[i].text
				local column_value = tokens[colum_index[1]].text	--字段值
				if string.match(column_value, "^%d+$") then -- 不能使用TK_INTER,数字可能加'', splitid 只支持整形
					sql = sql .. "_" .. (column_value % partition)
				else
					local key = crc.hash(column_value)
					if key > 2147483647 then
						key = key - 4294967296
					end
					sql = sql .. "_" .. (math.abs(key) % partition)
				end
			elseif tokens[i].token_name == "TK_STRING" then
				sql = sql .. "'" .. tokens[i].text .. "'"
			elseif tokens[i].token_name ~= "TK_COMMENT" then
				sql = sql .. tokens[i].text
			end
		end

		sqls[1] = sql
		write_log(level.INFO, "SQL = ", sql)
	else
		local mt = {}
		for i = 1, partition do	--声明二维数组
			mt[i] = {}
		end

		for i = 1, #colum_index do
			local column_value = tokens[colum_index[i]].text	--字段值

			local mod = nil	--模值
			if string.match(column_value, "^%d+$") then	-- 不能使用TK_INTER,数字可能加'', splitid 只支持整形
				mod = column_value % partition + 1
				local n = #mt[mod] + 1
				mt[mod][n] = column_value
			else	--字段值为字符串的情况
				local key = crc.hash(column_value)
				if key > 2147483647 then
					key = key - 4294967296
				end
				mod = math.abs(key) % partition + 1
				local n = #mt[mod] + 1
				mt[mod][n] = "'" .. column_value .. "'"
			end
		end

		local property_index   = colum_index[1] - 3	--字段名的索引
		local start_skip_index = property_index + 1	--IN的索引
		local end_skip_index   = property_index + (#colum_index + 1) * 2	--右括号的索引

		local j = 1
		for m = 1, partition do	--有几张子表就生成几个SQL语句
			if #mt[m] > 0 then
				local tmp = nil 
				tmp = " IN(" .. mt[m][1]
				for k = 2, #mt[m] do
					tmp = tmp .. "," .. mt[m][k]
				end
				tmp = tmp .. ")"

				local sql = ""
				if tokens[1].token_name ~= "TK_COMMENT" then sql = tokens[1].text end
				for i = 2, #tokens do
					if i < start_skip_index or i > end_skip_index then	--跳过原始SQL中IN到右括号的部分
						if tokens[i].text ~= "(" then
							sql = sql .. ' '
						end

						if i == table_index then
							sql = sql .. tokens[i].text .. "_" .. m-1
						elseif i == property_index then
							sql = sql .. tokens[i].text .. tmp
						elseif tokens[i].token_name ~= "TK_COMMENT" then
							sql = sql .. tokens[i].text
						end
					end
				end

				sqls[j] = sql
				j = j + 1
				write_log(level.INFO, "SQL = ", sql)
			end
		end
	end

	write_log(level.DEBUG, "LEAVE COMBINE_SQL")
	return sqls
end

--[[
--  SQL分析器
--      判断是否命中分表配置
--      进行一些危险SQL过滤
--
--]]
function sql_parse(tokens, query)
	write_log(level.DEBUG, "ENTER SQL_PARSE")

    local re = {} 
--	local config = table.config
	local table = config.table
    local dt_index = {}
	local split_colum_index = {}
--	re[1] = query
	split_colum_index[1] = 0

    --for k,v in pairs(config) do print(k,v.property) end
	--[[
    for i = 1, #tokens do
        print(string.format("%s\t%s", tokens[i].token_name, tokens[i].text))
    end
	]]
	local sql_type = filter.is_whitelist(tokens)
	if sql_type == false then
        proxy.response = {
            type = proxy.MYSQLD_PACKET_ERR,
            errmsg = "Proxy Warning - Syntax Forbidden(NOT in Whitelist)"
        }
		write_log(level.WARN, query, ": Syntax Forbidden(NOT in Whitelist)")
		write_log(level.DEBUG, "LEAVE SQL_PARSE")
        return re, -1
	end
    if filter.is_blacklist(tokens) then
        proxy.response = {
            type = proxy.MYSQLD_PACKET_ERR,
            errmsg = "Proxy Warning - Syntax Forbidden(Blacklist)"
        }
		write_log(level.WARN, query, ": Syntax Forbidden(Blacklist)")
		write_log(level.DEBUG, "LEAVE SQL_PARSE")
        return re, -1
    end

	if sql_type == 4 then
		return re, 0
	end

    --[[ 
    --  SQL Parse for Split
    --]]
    -- global array - table_index's attribute
    g_table_arr = {}

    -- init g_table_arr and find tablename's index
    dt_index = get_table_index(tokens, sql_type)
	local d = dt_index.d
	local t = dt_index.t

	if t == 0 then	--无table，则不需add也不需split
		write_log(level.DEBUG, "LEAVE SQL_PARSE");
		re[1] = query
		return re, 1
	end

	local is_split = false
	local dbtable
	if d == 0 then	--table形式
		dbtable = proxy.connection.client.default_db .. "." .. tokens[t].text
	else		--db.table形式
		dbtable = tokens[d].text..'.'.. tokens[t].text
	end
	if table[dbtable] then
		g_table_arr = table[dbtable]
		is_split = true
	end	

	--print("TABLENAME_INDEX = " .. tablename_index);
    --print("-------------debug---------")
    -- 命中分表配置
    if is_split then
        --print("Found table for split")
        split_colum_index = get_colum_index(tokens, g_table_arr, sql_type, t + 1)
		if #split_colum_index > 0 then
            --[[
            print("Found colum for split")
            print("property: " .. g_table_arr.property)
            print("partition: " .. g_table_arr.partition)
            print("table index: " .. t)
            print("colum index: " .. split_colum_index)
            ]]
        else
            proxy.response = {
                type = proxy.MYSQLD_PACKET_ERR,
                errmsg = "Proxy Warning - Syntax Error(SQL Parse)"
            }
			write_log(level.WARN, query, ": Syntax Error(SQL Parse)")
			write_log(level.DEBUG, "LEAVE SQL_PARSE")
            return re, -1
        end
    end

	if is_split then
		re = combine_sql(tokens, t, split_colum_index, g_table_arr)
	else
		re[1] = query
	end 
	--print("-------------debug---------")
    g_table_arr = {}    -- delete

	write_log(level.DEBUG, "LEAVE SQL_PARSE")

    return re, 1
end

function merge_rows(rows_left, rows_right_fun, sorttype, sortindex, limit)
    -- init
    local rows_right = {}
    local rows_merge = {}
    limit = tonumber(limit)

    local i = 1
    for row in rows_right_fun do
        rows_right[i] = row
        i = i + 1
    end

    -- return when one of array is nil
    if (rows_left == nil or #rows_left < 1) and rows_right ~= nil then
        --print('return rows_right')
        return rows_right
    end

    if (rows_right == nil or #rows_right < 1) and rows_left ~= nil then
        --print('return rows_left')
        return rows_left
    end

    local left_num = #rows_left
    local right_num = #rows_right
    local i = 1
    local j = 1
    local k = 1

    -- merge sort
    if sorttype == false then
        rows_merge = table_merge(rows_left, rows_right, limit)
    else
        if sorttype == "TK_SQL_ASC" then
            while i < left_num+1 and j < right_num+1 do
                if limit > -1 and k > limit then
                    break
                end

                if rows_left[i][sortindex] and rows_right[j][sortindex] and
                        tonumber(rows_left[i][sortindex]) < tonumber(rows_right[j][sortindex]) then
                    rows_merge[k] = rows_left[i]
                    --print(rows_merge[k][sortindex]..' '..rows_left[i][sortindex]..' '..rows_right[j][sortindex])
                    i = i + 1
                else
                    rows_merge[k] = rows_right[j]
                    --print(rows_merge[k][sortindex]..' '..rows_left[i][sortindex]..' '..rows_right[j][sortindex])
                    j = j + 1
                end
                k = k + 1
            end
        elseif sorttype == "TK_SQL_DESC" then
            while i < left_num+1 and j < right_num+1 do
                if limit > -1 and k > limit then
                    break
                end

                if tonumber(rows_left[i][sortindex]) > tonumber(rows_right[j][sortindex]) then
                    rows_merge[k] = rows_left[i]
                    i = i + 1
                else
                    rows_merge[k] = rows_right[j]
                    j = j + 1
                end
                k = k + 1
            end
        end

        while i < left_num+1 do
            if limit > -1 and k > limit then
                break
            end
            rows_merge[k] = rows_left[i]
            k = k + 1
            i = i + 1
        end

        while j < right_num+1 do
            if limit > -1 and k > limit then
                break
            end
            rows_merge[k] = rows_right[j]
            k = k + 1
            j = j + 1
        end
    end

    return rows_merge
end

function table_merge(t1, t2, limit)
    limit = tonumber(limit)
    if not t1 and not t2 then
        return false
    end

    if #t1 >= limit then
        return t1
    end

    local i = #t1
    for j = 1, #t2 do
        if i >= limit then
            break
        end
        t1[#t1+1] = t2[j]
        i = i + 1
    end

    return t1
end
