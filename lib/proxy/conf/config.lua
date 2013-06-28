local source = debug.getinfo(1, "S").source
local pos1 = 0 
local pos2 = 0 
while pos2 ~= nil do
        pos1 = pos2
        pos2 = string.find(source, "/", pos2+1)
end
pos2 = string.find(source, "%.lua", pos1+1)
local config_module = "proxy.conf." .. string.sub(source, pos1+1, pos2-1)
module(config_module, package.seeall)

--权限控制
is_auth = false

auth = {}

auth[1] =
{
    ip = "127.0.0.1",
}

auth[2] =
{
    ip = "192.168.33.192",
}

auth[3] =
{
    ip = "192.168.0.172",
}

seg_ip = {}

seg_ip[1] = "192.168.33"

--SQL过滤
blacklist = {}
blacklist[1] =
{
	FIRST     = "DELETE",
	FIRST_NOT = nil,
	ANY       = nil,
	ALL_NOT   = "WHERE"
}

whitelist = {}
whitelist[1] = {"SELECT", "DELETE"}
whitelist[2] = {"UPDATE"}
whitelist[3] = {"INSERT", "REPLACE"}
whitelist[4] = {"SHOW", "SET", "START", "COMMIT", "ROLLBACK", "BEGIN", "DESC"}

--日志设置
debug_info = false
log_level  = "info"
sql_log    = true
real_time  = true

--分表设置
table = {}
table["qtb_message.tb_messageinfo"] =
{ 
	name      = "tb_messageinfo",
	property  = "topic_id",
	partition = 600,
}
table["qtb_topic.tb_topicinfo"] =
{ 
	name      = "tb_topicinfo",
	property  = "board_id",
	partition = 100,
}

--Master是否可读
read_master = true

--LVS的IP
lvs_ip = {}

--连接池内的最小空闲连接数
min_idle_connections = 256
