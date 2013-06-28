/* $%BEGINLICENSE%$
 Copyright (c) 2009, 2010, Oracle and/or its affiliates. All rights reserved.

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
 

#ifndef _CHASSIS_LIMITS_H_
#define _CHASSIS_LIMITS_H_

#include <glib.h>    /* GPtrArray */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "chassis-exports.h"

CHASSIS_API int chassis_set_fdlimit(int max_files_number) G_GNUC_DEPRECATED; /* use chassis_fdlimit_set() instead */

CHASSIS_API int chassis_fdlimit_set(gint64 max_files_number);
CHASSIS_API gint64 chassis_fdlimit_get(void);

#endif
