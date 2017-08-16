#  $%BEGINLICENSE%$
#  Copyright (c) 2009, 2013, Oracle and/or its affiliates. All rights reserved.
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

## print a few properties of a target
MACRO(PRINT_TARGET_PROPERTIES target)
	MESSAGE(STATUS "properties of target ${target}")
	FOREACH(prop "COMPILE_FLAGS" "IMPORT_PREFIX" "IMPORT_SUFFIX" "TYPE" "LOCATION")
		GET_TARGET_PROPERTY(value ${target} ${prop})
		MESSAGE(STATUS "  ${prop} = ${value}")
	ENDFOREACH()

ENDMACRO(PRINT_TARGET_PROPERTIES target)

## install a shared-lib or executable target incl. .pdb files on win32
##
## works around a bug in cmake that doesn't include TARGETS in cpack
MACRO(CHASSIS_INSTALL_TARGET target)
	IF(WIN32)
		GET_TARGET_PROPERTY(built_location ${target} LOCATION)
		GET_TARGET_PROPERTY(type ${target} TYPE)
		STRING(REPLACE "$(OutDir)" "${CMAKE_BUILD_TYPE}" built_location ${built_location})
		IF(type MATCHES "SHARED_LIBRARY")
			STRING(REPLACE ".dll" ".lib" lib_location ${built_location})
			INSTALL(FILES
				${built_location}
				DESTINATION bin
			)
			INSTALL(FILES
				${lib_location}
				DESTINATION lib
			)
			IF(CMAKE_BUILD_TYPE MATCHES "RelWithDebInfo")
				STRING(REPLACE ".dll" ".pdb" pdb_location ${built_location})
				INSTALL(FILES
					${pdb_location}
					DESTINATION bin
				)
			ENDIF()
		ENDIF()
		IF(type MATCHES "EXECUTABLE")
			INSTALL(FILES
				${built_location}
				DESTINATION bin
			)
			IF(CMAKE_BUILD_TYPE MATCHES "RelWithDebInfo")
				STRING(REPLACE ".exe" ".pdb" pdb_location ${built_location})
				INSTALL(FILES
					${pdb_location}
					DESTINATION bin
				)
			ENDIF()
		ENDIF()
	ELSE(WIN32)
		INSTALL(TARGETS ${target}
			RUNTIME DESTINATION bin
			ARCHIVE DESTINATION lib
			LIBRARY DESTINATION lib
		)
	ENDIF(WIN32)
ENDMACRO(CHASSIS_INSTALL_TARGET target)


