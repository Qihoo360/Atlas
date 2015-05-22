prefix=@CMAKE_INSTALL_PREFIX@
exec_prefix=@CMAKE_INSTALL_PREFIX@
libdir=@CMAKE_INSTALL_PREFIX@/lib
pkglibdir=${libdir}/mysql-proxy
lualibdir=${pkglibdir}/lua
plugindir=${pkglibdir}/plugins

Name: MySQL Proxy
Version: @PACKAGE_VERSION_STRING@
Description: MySQL Proxy
URL: http://forge.mysql.com/wiki/MySQL_Proxy
Requires: glib-2.0 >= 2.16, mysql-chassis >= @PACKAGE_VERSION_STRING@
Libs: -L${libdir} -lmysql-chassis-proxy
