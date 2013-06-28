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

INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}) # for config.h
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

INCLUDE_DIRECTORIES(${GLIB_INCLUDE_DIRS})
LINK_DIRECTORIES(${GLIB_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${MYSQL_INCLUDE_DIRS})
LINK_DIRECTORIES(${MYSQL_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${LUA_INCLUDE_DIRS})
LINK_DIRECTORIES(${LUA_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${EVENT_INCLUDE_DIRS})
LINK_DIRECTORIES(${EVENT_LIBRARY_DIRS})

STRING(REPLACE "." "" SHARED_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
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

TARGET_LINK_LIBRARIES(mysql-chassis 
	${GLIB_LIBRARIES} 
	${GMODULE_LIBRARIES} 
	${GTHREAD_LIBRARIES} 
	${LUA_LIBRARIES}
	${EVENT_LIBRARIES}
	${WINSOCK_LIBRARIES}
	mysql-chassis-timing
	mysql-chassis-glibext
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
	mysql-chassis
	mysql-chassis-proxy
	mysql-chassis-glibext
	mysql-chassis-timing
)

IF(WIN32)
	ADD_EXECUTABLE(mysql-proxy-svc mysql-proxy-cli.c)
	TARGET_LINK_LIBRARIES(mysql-proxy-svc
		${GLIB_LIBRARIES} 
		${GMODULE_LIBRARIES} 
		${GTHREAD_LIBRARIES} 
		${LUA_LIBRARIES}
		${EVENT_LIBRARIES}
		mysql-chassis
		mysql-chassis-proxy
		mysql-chassis-glibext
		mysql-chassis-timing
	)
	SET_TARGET_PROPERTIES(mysql-proxy-svc
		PROPERTIES
		COMPILE_DEFINITIONS WIN32_AS_SERVICE
	)
ENDIF(WIN32)

IF(WIN32)
	CHASSIS_INSTALL_TARGET(mysql-proxy)
	CHASSIS_INSTALL_TARGET(mysql-proxy-svc)
ELSE(WIN32)
	# Unix platforms provide a wrapper script to avoid relinking at install time
	
	# figure out the correct name of the shared linker lookup path for this system, default to LD_LIBRARY_PATH
	SET(DYNLIB_PATH_VAR "LD_LIBRARY_PATH")
	
	IF("AIX" STREQUAL ${CMAKE_SYSTEM_NAME})
		SET(DYNLIB_PATH_VAR "LIBPATH")
	ENDIF("AIX" STREQUAL ${CMAKE_SYSTEM_NAME})
	
	IF("HP-UX" STREQUAL ${CMAKE_SYSTEM_NAME})
		SET(DYNLIB_PATH_VAR "SHLIB_PATH")
	ENDIF("HP-UX" STREQUAL ${CMAKE_SYSTEM_NAME})
	
	IF(APPLE)
		SET(DYNLIB_PATH_VAR "DYLD_LIBRARY_PATH")
	ENDIF(APPLE)
	
	# write the wrapper script, which uses DYNLIB_PATH_VAR
	CONFIGURE_FILE(
		${CMAKE_SOURCE_DIR}/scripts/mysql-proxy-binwrapper.in 
		${PROJECT_BINARY_DIR}/mysql-proxy.sh
		@ONLY
	)
	INSTALL(FILES ${PROJECT_BINARY_DIR}/mysql-proxy.sh
		DESTINATION bin/
		PERMISSIONS OWNER_EXECUTE OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
		RENAME mysql-proxy
	)

	INSTALL(TARGETS mysql-proxy
		RUNTIME DESTINATION libexec
	)
ENDIF(WIN32)

CHASSIS_INSTALL_TARGET(mysql-chassis)
CHASSIS_INSTALL_TARGET(mysql-chassis-proxy)
CHASSIS_INSTALL_TARGET(mysql-chassis-glibext)
CHASSIS_INSTALL_TARGET(mysql-chassis-timing)

INSTALL(FILES
	network-mysqld.h
	network-mysqld-lua.h
	network-mysqld-proto.h
	network_mysqld_type.h
	network_mysqld_proto_binary.h
	network-mysqld-binlog.h
	network-mysqld-packet.h
	network-mysqld-masterinfo.h
	network-conn-pool.h
	network-conn-pool-lua.h
	network-queue.h
	network-socket.h
	network-socket-lua.h
	network-address.h
	network-address-lua.h
	sys-pedantic.h
	chassis-plugin.h
	chassis-log.h
	chassis-keyfile.h
	chassis-mainloop.h
	chassis-shutdown-hooks.h
	chassis-path.h
	chassis-filemode.h
	chassis-limits.h
	chassis-event-thread.h
	glib-ext.h
	glib-ext-ref.h
	string-len.h
	lua-load-factory.h
	lua-scope.h
	lua-env.h
	network-injection.h
	network-injection-lua.h
	chassis-exports.h
	network-exports.h
	network-backend.h
	network-backend-lua.h
	disable-dtrace.h
	lua-registry-keys.h
	chassis-stats.h
	chassis-timings.h
	chassis-gtimeval.h
	chassis-frontend.h
	chassis-options.h
	chassis-unix-daemon.h
	chassis-win32-service.h
	my_rdtsc.h
	DESTINATION include/
	)
