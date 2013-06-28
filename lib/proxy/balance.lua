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


module("proxy.balance", package.seeall)

function idle_failsafe_rw()
	local backend_ndx = 0

	for i = 1, #proxy.global.backends do
		local s = proxy.global.backends[i]
		local conns = s.pool.users[proxy.connection.client.username]
		
		if conns.cur_idle_connections > 0 and 
		   s.state ~= proxy.BACKEND_STATE_DOWN and 
		   s.state ~= proxy.BACKEND_STATE_OFFLINE and 
		   s.type == proxy.BACKEND_TYPE_RW then
			backend_ndx = i
			break
		end
	end

	return backend_ndx
end

function idle_ro() 
	local max_conns = -1
	local max_conns_ndx = 0

	for i = 1, #proxy.global.backends do
		local s = proxy.global.backends[i]
		local conns = s.pool.users[proxy.connection.client.username]

		-- pick a slave which has some idling connections
		if s.type == proxy.BACKEND_TYPE_RO and 
		   s.state ~= proxy.BACKEND_STATE_DOWN and 
		   s.state ~= proxy.BACKEND_STATE_OFFLINE and 
		   conns.cur_idle_connections > 0 then
			if max_conns == -1 or 
			   s.connected_clients < max_conns then
				max_conns = s.connected_clients
				max_conns_ndx = i
			end
		end
	end

	return max_conns_ndx
end

function cycle_read_ro()
	local backends   = proxy.global.backends
	local rwsplit    = proxy.global.config.rwsplit
	local max_weight = rwsplit.max_weight
	local cur_weight = rwsplit.cur_weight
	local next_ndx   = rwsplit.next_ndx
	local ndx_num    = rwsplit.ndx_num

	local ndx = 0
	for i = 1, ndx_num do	--每个query最多轮询ndx_num次
		local s = backends[next_ndx]

		if s.type == proxy.BACKEND_TYPE_RO and s.weight >= cur_weight and s.state ~= proxy.BACKEND_STATE_DOWN and s.state ~= proxy.BACKEND_STATE_OFFLINE and s.pool.users[proxy.connection.client.username].cur_idle_connections > 0 then ndx = next_ndx end

		if next_ndx == ndx_num then	--轮询完了最后一个ndx，权值减一
			cur_weight = cur_weight - 1
			if cur_weight == 0 then cur_weight = max_weight end
			next_ndx = 1
		else				--否则指向下个ndx
			next_ndx = next_ndx + 1
		end

		if ndx ~= 0 then break end
	end

	rwsplit.cur_weight = cur_weight
	rwsplit.next_ndx = next_ndx

	return ndx
end
