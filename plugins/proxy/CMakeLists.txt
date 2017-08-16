#  $%BEGINLICENSE%$
#  Copyright (c) 2009, Oracle and/or its affiliates. All rights reserved.
# 
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License as
#  published by the Free Software Foundation; version 2 of the
#  License.
# 
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
#  02110-1301  USA
# 
#  $%ENDLICENSE%$

FIND_PATH(LEMON_INCLUDE_DIRS 
    NAMES "sqliteInt.h"
    PATHS "/usr/local/include/lemon")
FIND_LIBRARY(LEMON_LIB_PATH
    NAME "lemon_parser"
    PATHS "/usr/local/lib")


INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/src/)
INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}) # for config.h

INCLUDE_DIRECTORIES(${GLIB_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${MYSQL_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${LUA_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${EVENT_INCLUDE_DIRS})

INCLUDE_DIRECTORIES(${LEMON_INCLUDE_DIRS})
LINK_DIRECTORIES(${LEMON_LIB_PATH})

LINK_DIRECTORIES(${LUA_LIBRARY_DIRS})
LINK_DIRECTORIES(${GLIB_LIBRARY_DIRS})
LINK_DIRECTORIES(${LIBINTL_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${REDIS_INCLUDE_DIRS})
LINK_DIRECTORIES(${REDIS_LIB_PATH})

SET(_plugin_name proxy)

SET(PROJECT_ALL_FILES 
    ${_plugin_name}-plugin.c
    )                      

ADD_LIBRARY(${_plugin_name} SHARED "${PROJECT_ALL_FILES}")
TARGET_LINK_LIBRARIES(${_plugin_name}
    ${MYSQL_LIBRARY}
    ${LEMON_LIB_PATH}
    mysql-chassis-proxy
    mysql
    )

