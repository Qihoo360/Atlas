prefix=@CMAKE_INSTALL_PREFIX@
exec_prefix=@CMAKE_INSTALL_PREFIX@
libdir=@CMAKE_INSTALL_PREFIX@/lib
includedir=@CMAKE_INSTALL_PREFIX@/include

Name: mysql-chassis
Version: @PACKAGE_VERSION_STRING@
Description: the Chassis of the MySQL Proxy
URL: http://forge.mysql.com/wiki/MySQL_Proxy
Requires: glib-2.0 >= 2.16
Libs: -L${libdir} -lmysql-chassis -lmysql-chassis-timing -lmysql-chassis-glibext
Cflags: -I${includedir} 
