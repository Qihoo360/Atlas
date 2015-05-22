--[[ $%BEGINLICENSE%$
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation; version 2 of the
 License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 02110-1301  USA

 $%ENDLICENSE%$ --]]
--
-- this file contains a list of tests to skip.
-- Add the tests to skip to the table below
--

tests_to_skip = {
    --  test name          reason
    --  --------------    ---------------------------
        ['dummy']       = 'Too ugly to show',
        ['bug_XYZ']     = 'Nobody cares anymore',
        ['end_session']     = 'Have to figure out the sequence',
        ['bug_30867']   = 'needs backends',
        ['xtab2']   = 'works, but needs a real mysql-server',
        ['select_affected_rows']   = 'needs backends',
        ['client_address_socket'] = 'waiting for bug#38416',
        ['change_user'] = 'works, but needs to run as root and configured with a valid user',
	['bug_45167'] = 'works, but mysqltest cant handle errors in change_user',
	['bug_61998'] = 'works, but needs a real mysql-server',
}

local build_os = os.getenv("BUILD_OS")

--
-- some older OSes run out of memory in the 32M byte test on our build
-- platforms. We disable just that test on those platforms
if build_os and
	(build_os == "i386-pc-solaris2.8" or
	 build_os == "x86_64-pc-solaris2.8" or
	 build_os == "sparc-sun-solaris2.8" or
	 build_os == "sparc-sun-solaris2.9" or
	 build_os == "powerpc-ibm-aix5.3.0.0") then
	tests_to_skip['overlong'] = "can't allocate more than 32M"
end


