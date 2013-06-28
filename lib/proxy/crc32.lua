-- Copyright (c) 2011, Qihoo 360 - Chancey
-- this is a slightly modified version used inside the interkomm project

require("crc32.string")

module('proxy.crc32', package.seeall)

function hash(string)
    return crc32.crc32(string)
end
