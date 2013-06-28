@rem  $%BEGINLICENSE%$
@rem  Copyright (c) 2008, Oracle and/or its affiliates. All rights reserved.
@rem 
@rem  This program is free software; you can redistribute it and/or
@rem  modify it under the terms of the GNU General Public License as
@rem  published by the Free Software Foundation; version 2 of the
@rem  License.
@rem 
@rem  This program is distributed in the hope that it will be useful,
@rem  but WITHOUT ANY WARRANTY; without even the implied warranty of
@rem  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
@rem  GNU General Public License for more details.
@rem 
@rem  You should have received a copy of the GNU General Public License
@rem  along with this program; if not, write to the Free Software
@rem  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
@rem  02110-1301  USA
@rem 
@rem  $%ENDLICENSE%$
@echo "Run this from a shell started with the Visual Studio Build environment set!"
@echo "You can set DEPS_PATH (for the dependencies package) and MYSQL_DIR for a MySQL server installation"
@IF DEFINED DEPS_PATH (GOTO MYSQL_CONF)
@SET DEPS_PATH=%CD%\..\..\mysql-lb-deps\win32

@IF DEFINED GENERATOR (GOTO MYSQL_CONF)
@rem Sane default is VS2005, but maybe not what we really want...
@SET GENERATOR="Visual Studio 8 2005"
@GOTO MYSQL_CONF

:MYSQL_CONF
@IF DEFINED MYSQL_DIR (GOTO GENERAL_CONF)
@SET MYSQL_DIR="C:\Program Files\MySQL\MySQL Server 5.0"

:GENERAL_CONF
@SET GLIB_DIR=%DEPS_PATH%
@SET PATH=%DEPS_PATH%\bin;%PATH%
@SET NSISDIR=%DEPS_PATH%\bin

@echo Using MySQL server from %MYSQL_DIR%
@echo Using dependencies from %DEPS_PATH%
@echo Using %GENERATOR%

@rem echo Checking for NSIS...
@rem reg query HKLM\Software\NSIS /v VersionMajor
@rem IF %ERRORLEVEL% NEQ 0 (GOTO NONSIS)
@rem GOTO NSISOK

@rem :NONSIS
@rem echo using internal NSIS installation
@rem SET CLEANUP_NSIS=1

@rem reg add HKLM\Software\NSIS /ve /t REG_SZ /d %NSISDIR% /f
@rem reg add HKLM\Software\NSIS /v VersionMajor /t REG_DWORD /d 00000002 /f
@rem reg add HKLM\Software\NSIS /v VersionMinor /t REG_DWORD /d 00000025 /f
@rem reg add HKLM\Software\NSIS /v VersionRevision /t REG_DWORD /d 0 /f
@rem reg add HKLM\Software\NSIS /v VersionBuild /t REG_DWORD /d 0 /f

@GOTO ENDNSIS

:NSISOK
@rem echo found existing NSIS installation
@rem SET CLEANUP_NSIS=0
@rem GOTO ENDNSIS

:ENDNSIS

@rem MSVC 8 2005 doesn't seem to have devenv.com
@SET VS_CMD="%VS90COMNTOOLS%\..\IDE\VCExpress.exe"

@echo Copying dependencies to deps folder
@copy %DEPS_PATH%\packages\* deps\

@rem clear the cache if neccesary to let cmake recheck everything
@rem del CMakeCache.txt

:CMAKE
@cmake -G %GENERATOR% -DBUILD_NUMBER=%BUILD_NUMBER% -DMYSQL_LIBRARY_DIRS:PATH=%MYSQL_DIR%\lib\debug -DMYSQL_INCLUDE_DIRS:PATH=%MYSQL_DIR%\include -DGLIB_LIBRARY_DIRS:PATH=%GLIB_DIR%\lib -DGLIB_INCLUDE_DIRS:PATH=%GLIB_DIR%\include\glib-2.0;%GLIB_DIR%\lib\glib-2.0\include -DGMODULE_INCLUDE_DIRS:PATH=%GLIB_DIR%\include\glib-2.0;%GLIB_DIR%\lib\glib-2.0\include -DGTHREAD_INCLUDE_DIRS:PATH=%GLIB_DIR%\include\glib-2.0;%GLIB_DIR%\lib\glib-2.0\include -DCMAKE_BUILD_TYPE=Release .

@IF NOT %GENERATOR%=="NMake Makefiles" (GOTO VS08BUILD)
nmake

@GOTO CLEANUP

:VS08BUILD
%VS_CMD% mysql-proxy.sln /Clean
%VS_CMD% mysql-proxy.sln /Build Release
%VS_CMD% mysql-proxy.sln /Build Release /project RUN_TESTS
%VS_CMD% mysql-proxy.sln /Build Release /project PACKAGE
%VS_CMD% mysql-proxy.sln /Build Release /project INSTALL

@GOTO CLEANUP

@rem if you use VS8 to build then VS80COMNTOOLS should be set
@rem "%VS80COMNTOOLS%\..\IDE\devenv.com" mysql-proxy.sln /Clean
@rem "%VS80COMNTOOLS%\..\IDE\devenv.com" mysql-proxy.sln /Build
@rem "%VS80COMNTOOLS%\..\IDE\devenv.com" mysql-proxy.sln /Build Debug /project RUN_TESTS
@rem "%VS80COMNTOOLS%\..\IDE\devenv.com" mysql-proxy.sln /Build Debug /project PACKAGE
@rem "%VS80COMNTOOLS%\..\IDE\devenv.com" mysql-proxy.sln /Build Debug /project INSTALL

:CLEANUP

@rem IF %CLEANUP_NSIS% EQU 1 (GOTO REMOVEKEYS)
@rem echo leaving existing keys untouched
@GOTO END

:REMOVEKEYS
@rem echo removing temporary NSIS registry entries
@rem reg delete HKLM\Software\NSIS /va /f

:END
