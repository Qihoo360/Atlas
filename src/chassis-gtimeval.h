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

#ifndef __CHASSIS_GTIMEVAL_H__
#define __CHASSIS_GTIMEVAL_H__

#include <glib.h>
#include "glib-ext.h"

/**
 * stores the current time in the location passed as argument
 * if time is seen to move backwards, output an error message and
 * set the time to "0".
 * @param pointer to a GTimeVal struct
 * @param pointer to a return value, containing difference in usec
 *     between provided timestamp and "now"
 */

CHASSIS_API void chassis_gtime_testset_now(GTimeVal *gt, gint64 *delay);
#endif
