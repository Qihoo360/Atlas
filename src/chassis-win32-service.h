/* $%BEGINLICENSE%$
 Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

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

 $%ENDLICENSE%$ */

#ifndef __CHASSIS_WIN32_SERVICE_H__
#define __CHASSIS_WIN32_SERVICE_H__

#ifdef _WIN32
#include <windows.h> /* for the DWORD */
#endif

#include <glib.h>

#include "chassis-exports.h"

CHASSIS_API gboolean chassis_win32_is_service(void);
CHASSIS_API int main_win32(int argc, char **argv, int (*main_cmdline)(int , char **));
#ifdef _WIN32
CHASSIS_API void chassis_win32_service_set_state(DWORD new_state, int wait_msec);
#endif

#endif
