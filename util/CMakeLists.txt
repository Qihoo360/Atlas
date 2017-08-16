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

#SET(JSON-C_INCLUDE_DIRS  CACHE PATH "json-c include dir ")
#SET(JSON-C_LIBRARY_DIRS  CACHE PATH "json-c library dir [see pkg-config json-c --libs]")

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

      #MESSAGE(STATUS "json include path: ${JSON_C_INCLUDE_DIRS}")
      #MESSAGE(STATUS "json lib path: ${JSON_C_LIB_PATH}")

INCLUDE_DIRECTORIES(${GLIB_INCLUDE_DIRS})
LINK_DIRECTORIES(${GLIB_LIBRARY_DIRS})

INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR}) # for config.h
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})
INCLUDE_DIRECTORIES(${JSON_C_INCLUDE_DIRS})

INCLUDE_DIRECTORIES(${GLIB_INCLUDE_DIRS})
LINK_DIRECTORIES(${GLIB_LIBRARY_DIRS})


STRING(REPLACE "." "" SHARED_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})

SET(util_sources
    parse_config_file.c
    save_config_file.c
    id_generator_local.c
)

ADD_LIBRARY(atlas-util SHARED ${util_sources})

TARGET_LINK_LIBRARIES(atlas-util
    ${JSON_C_LIB_PATH}
    ${REDIS_LIB_PATH}
) 
