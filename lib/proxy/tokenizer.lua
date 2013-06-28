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


require("mysql.tokenizer")

module("proxy.tokenizer", package.seeall)

---
-- normalize a query
--
-- * remove comments
-- * quote literals
-- * turn constants into ?
-- * turn tokens into uppercase
--
-- @param tokens a array of tokens
-- @return normalized SQL query
-- 
-- @see tokenize
function normalize(tokens)
	-- we use a string-stack here and join them at the end
	-- see http://www.lua.org/pil/11.6.html for more
	--
	local stack = {}
	-- literals that are SQL commands if they appear at the start
	-- (all uppercase)
	local literal_keywords = {
		["COMMIT"] = { },
		["ROLLBACK"] = { },
		["BEGIN"] = { },
		["START"] = { "TRANSACTION" },
	}

	for i = 1, #tokens do
		local token = tokens[i]

		-- normalize the query
		if token["token_name"] == "TK_COMMENT" then
		elseif token["token_name"] == "TK_COMMENT_MYSQL" then
			-- a /*!... */ comment
			--
			-- we can't look into the comment as we don't know which server-version
			-- we will talk to, pass it on verbatimly
			table.insert(stack, "/*!" ..token.text .. "*/ ")
		elseif token["token_name"] == "TK_LITERAL" then
			if token.text:sub(1, 1) == "@" then
				-- append session variables as is
				table.insert(stack, token.text .. " ")
			elseif #stack == 0 then -- nothing is on the stack yet
				local u_text = token.text:upper()

				if literal_keywords[u_text] then
					table.insert(stack, u_text .. " ")
				else
					table.insert(stack, "`" .. token.text .. "` ")
				end
			elseif #stack == 1 then
				local u_text = token.text:upper()

				local starting_keyword = stack[1]:sub(1, -2)

				if literal_keywords[starting_keyword] and
				   literal_keywords[starting_keyword][1] == u_text then
					table.insert(stack, u_text .. " ")
				else
					table.insert(stack, "`" .. token.text .. "` ")
				end
			else
				table.insert(stack, "`" .. token.text .. "` ")
			end
		elseif token["token_name"] == "TK_STRING" or
		       token["token_name"] == "TK_INTEGER" or
		       token["token_name"] == "TK_FLOAT" then
			table.insert(stack, "? ")
		elseif token["token_name"] == "TK_FUNCTION" then
			table.insert(stack,  token.text:upper())
		else
			table.insert(stack,  token.text:upper() .. " ")
		end
	end

	return table.concat(stack)
end

---
-- call the included tokenizer
--
-- this function is only a wrapper and exists mostly
-- for constancy and documentation reasons
function tokenize(packet)
	local tokens = tokenizer.tokenize(packet)
	local attr   = 0
	if tokens[1].token_name == "TK_COMMENT" or tokens[1].token_name == "TK_COMMENT_MYSQL" then
		if string.match(tokens[1].text:upper(), "^%s*MASTER%s*$") then
			attr = 1	--1代表强制读master
		end
	end
	return tokens, attr
end

---
-- return the first command token
--
-- * strips the leading comments
function first_stmt_token(tokens)
	for i = 1, #tokens do
		local token = tokens[i]
		-- normalize the query
		if token["token_name"] == "TK_COMMENT" then
		elseif token["token_name"] == "TK_LITERAL" then
			-- commit and rollback at LITERALS
			return token
		else
			-- TK_SQL_* are normal tokens
			return token
		end
	end

	return nil
end

---
--[[

   returns an array of simple token values
   without id and name, and stripping all comments
   
   @param tokens an array of tokens, as produced by the tokenize() function
   @param quote_strings : if set, the string tokens will be quoted
   @see tokenize
--]]
function bare_tokens (tokens, quote_strings)
    local simple_tokens = {}
	for i = 1, #tokens do
		local token = tokens[i]
        if (token['token_name'] == 'TK_STRING') and quote_strings then
            table.insert(simple_tokens, string.format('%q', token['text'] ))
        elseif (token['token_name'] ~= 'TK_COMMENT') then
            table.insert(simple_tokens, token['text'])
        end
    end
    return simple_tokens
end

---
--[[
    
   Returns a text query from an array of tokens, stripping off comments
  
   @param tokens an array of tokens, as produced by the tokenize() function
   @param start_item ignores tokens before this one
   @param end_item ignores token after this one
   @see tokenize
--]]
function tokens_to_query ( tokens , start_item, end_item )
    if not start_item then
        start_item = 1
    end
    if not end_item then
        end_item = #tokens
    end
    local counter  = 0
    local new_query = ''
	for i = 1, #tokens do
		local token = tokens[i]
        counter = counter + 1
        if (counter >= start_item and counter <= end_item ) then
            if (token['token_name'] == 'TK_STRING') then
                new_query = new_query .. string.format('%q', token['text'] )
            elseif token['token_name'] ~= 'TK_COMMENT' then
                new_query = new_query .. token['text'] 
            end
            if (token['token_name'] ~= 'TK_FUNCTION')
               and 
               (token['token_name'] ~= 'TK_COMMENT') 
            then
                new_query = new_query .. ' '
            end
        end
    end
    return new_query
end

---
--[[
   returns an array of tokens, stripping off all comments

   @param tokens an array of tokens, as produced by the tokenize() function
   @see tokenize, simple_tokens
--]]
function tokens_without_comments (tokens)
    local new_tokens = {}
	for i = 1, #tokens do
		local token = tokens[i]
        if (token['token_name'] ~= 'TK_COMMENT' and token['token_name'] ~= 'TK_COMMENT_MYSQL') then
            table.insert(new_tokens, token)
        end
    end
    return new_tokens
end

