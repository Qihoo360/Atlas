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
if proxy.global.lm_results == nil then
    proxy.global.lm_results = {}
end

function read_query (packet)
    proxy.global.lm_results['read_query2'] = proxy.global.lm_results['read_query2'] or 0
    proxy.global.lm_results['read_query2'] = proxy.global.lm_results['read_query2'] + 1
	if packet:byte() ~= proxy.COM_QUERY then
		return
	end
	local query = packet:sub(2)
    if query:match('select pload status') then
        local header = { 'function', 'hits' } 
        local rows = {}
        for func, hits in pairs(proxy.global.lm_results) do
            table.insert(rows, { func, hits } )
        end
        return proxy.global.make_dataset(header,rows)
    end
end

