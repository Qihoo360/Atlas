# $%BEGINLICENSE%$
# Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; version 2 of the
# License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
# 02110-1301  USA
# $%ENDLICENSE%$

FIND_PATH(JSON_C_INCLUDE_DIRS 
    NAMES "json.h"
    PATHS "/usr/local/include/json-c"
    "/usr/include/json-c"
    "/usr/include")
FIND_LIBRARY(JSON_C_LIB_PATH
    NAME "json-c"
    PATHS "/usr/local/lib"
          "/usr/lib"
          "/usr/lib32")

FIND_PATH(LEMON_INCLUDE_DIRS 
    NAMES "sqliteInt.h"
    PATHS "/usr/local/include/lemon")
FIND_LIBRARY(LEMON_LIB_PATH
    NAME "lemon_parser"
    PATHS "/usr/local/lib")

INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}) # for config.h
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

INCLUDE_DIRECTORIES(${GLIB_INCLUDE_DIRS})
LINK_DIRECTORIES(${GLIB_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${MYSQL_INCLUDE_DIRS})
LINK_DIRECTORIES(${MYSQL_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${LUA_INCLUDE_DIRS})
LINK_DIRECTORIES(${LUA_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${JSON_C_INCLUDE_DIRS})
LINK_DIRECTORIES(${JSON_C_LIB_PATH})


INCLUDE_DIRECTORIES(${LEMON_INCLUDE_DIRS})
LINK_DIRECTORIES(${LEMON_LIB_PATH})

INCLUDE_DIRECTORIES(${EVENT_INCLUDE_DIRS})
LINK_DIRECTORIES(${EVENT_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${REDIS_INCLUDE_DIRS})
LINK_DIRECTORIES(${REDIS_LIB_PATH})


STRING(REPLACE "." "" SHARED_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
ADD_DEFINITIONS(-DHAVE_LUA_H)
ADD_DEFINITIONS(-DHAVE_ULONG)
ADD_DEFINITIONS(-DHAVE_SYS_RESOURCE_H)
ADD_DEFINITIONS(-DSHARED_LIBRARY_SUFFIX="${SHARED_LIBRARY_SUFFIX}")

SET(chassis_sources 
	lua-load-factory.c
	lua-scope.c
	chassis-plugin.c
	chassis-event-thread.c
	chassis-log.c
	chassis-mainloop.c
	chassis-shutdown-hooks.c
	chassis-keyfile.c
	chassis-path.c
	chassis-filemode.c
	chassis-limits.c
	chassis-stats.c
	chassis-frontend.c
	chassis-options.c
	chassis-unix-daemon.c
	chassis-win32-service.c
    chassis-sharding.c 
)

SET(timing_sources
	chassis-timings.c
	chassis-gtimeval.c
	my_rdtsc.c
)

SET(glibext_sources
	glib-ext.c
	glib-ext-ref.c
)

SET(proxy_sources 
	network-mysqld.c 
	network-mysqld-lua.c 
	network-mysqld-proto.c 
	network_mysqld_type.c 
	network_mysqld_proto_binary.c 
	network-mysqld-binlog.c 
	network-mysqld-packet.c 
	network-mysqld-masterinfo.c 
	network-conn-pool.c  
	network-conn-pool-lua.c  
	network-queue.c
	network-socket.c
	network-socket-lua.c
	network-address.c
	network-address-lua.c
	network-injection.c
	network-injection-lua.c
	network-backend.c
	network-backend-lua.c
	lua-env.c
)

ADD_LIBRARY(mysql-chassis SHARED ${chassis_sources})
ADD_LIBRARY(mysql-chassis-proxy SHARED ${proxy_sources})
ADD_LIBRARY(mysql-chassis-glibext SHARED ${glibext_sources})
ADD_LIBRARY(mysql-chassis-timing SHARED ${timing_sources})
ADD_EXECUTABLE(mysql-proxy mysql-proxy-cli.c)

## for windows we need the winsock lib
SET(WINSOCK_LIBRARIES)
IF(WIN32)
	SET(WINSOCK_LIBRARIES ws2_32)
ENDIF()

TARGET_LINK_LIBRARIES(mysql-chassis-glibext
	${GLIB_LIBRARIES} 
	${GMODULE_LIBRARIES} 
	${GTHREAD_LIBRARIES} 
)

TARGET_LINK_LIBRARIES(mysql-chassis-timing
	${GLIB_LIBRARIES} 
	${GMODULE_LIBRARIES} 
	${GTHREAD_LIBRARIES} 
	mysql-chassis-glibext
)

FIND_LIBRARY(LIBEVENT_LIB NAMES event PATHS /usr/local/lib)
MESSAGE(STATUS "libcurl lib path: ${LIBEVENT_LIB}") 
TARGET_LINK_LIBRARIES(mysql-chassis 
	${GLIB_LIBRARIES} 
	${GMODULE_LIBRARIES} 
	${GTHREAD_LIBRARIES} 
	${LUA_LIBRARIES}
    ${LIBEVENT_LIB}
	${WINSOCK_LIBRARIES}
    ${LEMON_LIB_PATH}
	mysql-chassis-timing
	mysql-chassis-glibext
    crypto
)

TARGET_LINK_LIBRARIES(mysql-chassis-proxy
	mysql-chassis 
	mysql-chassis-glibext
	mysql-chassis-timing
)

TARGET_LINK_LIBRARIES(mysql-proxy 
	${GLIB_LIBRARIES} 
	${GMODULE_LIBRARIES} 
	${GTHREAD_LIBRARIES} 
	${LUA_LIBRARIES}
	${EVENT_LIBRARIES}
    ${MYSQL_LIBRARY}
	mysql
	mysql-chassis
	mysql-chassis-proxy
	mysql-chassis-glibext
	mysql-chassis-timing
    atlas-util
)

