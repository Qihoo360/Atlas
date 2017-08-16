#  $%BEGINLICENSE%$
#  Copyright (c) 2008, 2013, Oracle and/or its affiliates. All rights reserved.
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

MACRO(CHASSIS_PLUGIN_INSTALL _plugin_name)
	IF(NOT WIN32)
		INSTALL(TARGETS ${_plugin_name}
			DESTINATION lib/mysql-proxy/plugins)
	ELSE(NOT WIN32)
		## on win32 the chassis plugins gets prefixed with plugin- and end up in bin/
		GET_TARGET_PROPERTY(built_location ${_plugin_name} LOCATION)
		STRING(REPLACE "$(OutDir)" "${CMAKE_BUILD_TYPE}" built_location ${built_location})
		INSTALL(FILES ${built_location}
			DESTINATION bin/
			RENAME plugin-${_plugin_name}${CMAKE_SHARED_LIBRARY_SUFFIX}
		)
		## install the .pdb too
		IF(CMAKE_BUILD_TYPE MATCHES "RelWithDebInfo")
			STRING(REPLACE ${CMAKE_SHARED_LIBRARY_SUFFIX} ".pdb" pdb_location ${built_location})
			INSTALL(FILES
				${pdb_location}
				DESTINATION bin
			)
		ENDIF()
	ENDIF(NOT WIN32)
ENDMACRO(CHASSIS_PLUGIN_INSTALL _plugin_name)

