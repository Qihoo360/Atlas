# $%BEGINLICENSE%$
# $%ENDLICENSE%$

MACRO(CHASSIS_PLUGIN_INSTALL _plugin_name)
	IF(NOT WIN32)
		INSTALL(TARGETS ${_plugin_name}
			DESTINATION lib/mysql-proxy/plugins)
	ELSE(NOT WIN32)
		## on win32 the lua module gets prefixed with lua- and end up in bin/
		INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE}/${CMAKE_SHARED_LIBRARY_PREFIX}${_plugin_name}${CMAKE_SHARED_LIBRARY_SUFFIX}
			DESTINATION bin/
			RENAME plugin-${_plugin_name}${CMAKE_SHARED_LIBRARY_SUFFIX}
		)
	ENDIF(NOT WIN32)
ENDMACRO(CHASSIS_PLUGIN_INSTALL _plugin_name)

