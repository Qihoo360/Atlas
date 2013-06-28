## only run the tests if we have lua somewhere 
#
# for now this is unix specific as we use env-vars and shell-scripts
IF(LUA_EXECUTABLE AND UNIX )
	CONFIGURE_FILE(
		${CMAKE_CURRENT_SOURCE_DIR}/lua-tests-wrapper.sh.in 
		${CMAKE_CURRENT_BINARY_DIR}/lua-tests-wrapper.sh
		@ONLY)
	ADD_TEST(lua-tests/base 
		sh -x ${CMAKE_CURRENT_BINARY_DIR}/lua-tests-wrapper.sh 
		${LUA_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/run-tests.lua ${CMAKE_CURRENT_SOURCE_DIR}/base)
ENDIF(LUA_EXECUTABLE AND UNIX )
