/* $%BEGINLICENSE%$
 Copyright (c) 2007, 2008, Oracle and/or its affiliates. All rights reserved.

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
/*
** LuaFileSystem
** Copyright Kepler Project 2004-2006 (http://www.keplerproject.org/luafilesystem)
**
** File system manipulation library.
** This library offers these functions:
**   lfs.attributes (filepath [, attributename])
**   lfs.chdir (path)
**   lfs.currentdir ()
**   lfs.dir (path)
**   lfs.lock (fh, mode)
**   lfs.mkdir (path)
**   lfs.rmdir (path)
**   lfs.touch (filepath [, atime [, mtime]])
**   lfs.unlock (fh)
**
** $Id: lfs.c,v 1.32 2006/03/14 13:39:38 tomas Exp $
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <sys/locking.h>
#include <sys/utime.h>
#else
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/types.h>
#include <utime.h>
#endif

#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"

/* Define 'chdir' for systems that do not implement it */
#ifndef HAVE_CHDIR
#define chdir(p)	(-1)
#define chdir_error	"Function 'chdir' not provided by system"
#else
#define chdir_error	strerror(errno)
#endif

#if defined(_WIN32)
# define LUAEXT_API __declspec(dllexport)
#else
# define LUAEXT_API extern
#endif

LUAEXT_API int luaopen_lfs (lua_State *L);
/* Define 'strerror' for systems that do not implement it */
#ifndef HAVE_STRERROR
#define strerror(_)	"System unable to describe the error"
#endif

/* Define 'getcwd' for systems that do not implement it */
#ifndef HAVE_GETCWD
#define getcwd(p,s)	NULL
#define getcwd_error	"Function 'getcwd' not provided by system"
#else
#define getcwd_error	strerror(errno)
#endif

#define DIR_METATABLE "directory metatable"
#define MAX_DIR_LENGTH 1023
typedef struct dir_data {
	int  closed;
#ifdef _WIN32
	long hFile;
	char pattern[MAX_DIR_LENGTH+1];
#else
	DIR *dir;
#endif
} dir_data;


/*
** This function changes the working (current) directory
*/
static int change_dir (lua_State *L) {
	const char *path = luaL_checkstring(L, 1);
	if (chdir(path)) {
		lua_pushnil (L);
		lua_pushfstring (L,"Unable to change working directory to '%s'\n%s\n",
				path, chdir_error);
		return 2;
	} else {
		lua_pushboolean (L, 1);
		return 1;
	}
}

/*
** This function returns the current directory
** If unable to get the current directory, it returns nil
**  and a string describing the error
*/
static int get_dir (lua_State *L) {
	char path[255+2];
	if (getcwd(path, 255) == NULL) {
		lua_pushnil(L);
		lua_pushstring(L, getcwd_error);
		return 2;
	}
	else {
		lua_pushstring(L, path);
		return 1;
	}
}

/*
** Check if the given element on the stack is a file and returns it.
*/
static FILE *check_file (lua_State *L, int idx, const char *funcname) {
	FILE **fh = (FILE **)luaL_checkudata (L, idx, "FILE*");
	if (fh == NULL) {
		luaL_error (L, "%s: not a file", funcname);
		return 0;
	} else if (*fh == NULL) {
		luaL_error (L, "%s: closed file", funcname);
		return 0;
	} else
		return *fh;
}


/*
**
*/
static int _file_lock (lua_State *L, FILE *fh, const char *mode, const long start, long len, const char *funcname) {
	int code;
#ifdef _WIN32
	/* lkmode valid values are:
	   LK_LOCK    Locks the specified bytes. If the bytes cannot be locked, the program immediately tries again after 1 second. If, after 10 attempts, the bytes cannot be locked, the constant returns an error.
	   LK_NBLCK   Locks the specified bytes. If the bytes cannot be locked, the constant returns an error.
	   LK_NBRLCK  Same as _LK_NBLCK.
	   LK_RLCK    Same as _LK_LOCK.
	   LK_UNLCK   Unlocks the specified bytes, which must have been previously locked.

	   Regions should be locked only briefly and should be unlocked before closing a file or exiting the program.

	   http://msdn.microsoft.com/library/default.asp?url=/library/en-us/vclib/html/_crt__locking.asp
	*/
	int lkmode;
	switch (*mode) {
		case 'r': lkmode = LK_NBLCK; break;
		case 'w': lkmode = LK_NBLCK; break;
		case 'u': lkmode = LK_UNLCK; break;
		default : return luaL_error (L, "%s: invalid mode", funcname);
	}
	if (!len) {
		fseek (fh, 0L, SEEK_END);
		len = ftell (fh);
	}
	fseek (fh, start, SEEK_SET);
	code = _locking (fileno(fh), lkmode, len);
#else
	struct flock f;
	switch (*mode) {
		case 'w': f.l_type = F_WRLCK; break;
		case 'r': f.l_type = F_RDLCK; break;
		case 'u': f.l_type = F_UNLCK; break;
		default : return luaL_error (L, "%s: invalid mode", funcname);
	}
	f.l_whence = SEEK_SET;
	f.l_start = (off_t)start;
	f.l_len = (off_t)len;
	code = fcntl (fileno(fh), F_SETLK, &f);
#endif
	return (code != -1);
}


/*
** Locks a file.
** @param #1 File handle.
** @param #2 String with lock mode ('w'rite, 'r'ead).
** @param #3 Number with start position (optional).
** @param #4 Number with length (optional).
*/
static int file_lock (lua_State *L) {
	FILE *fh = check_file (L, 1, "lock");
	const char *mode = luaL_checkstring (L, 2);
	const long start = luaL_optlong (L, 3, 0);
	long len = luaL_optlong (L, 4, 0);
	if (_file_lock (L, fh, mode, start, len, "lock")) {
		lua_pushboolean (L, 1);
		return 1;
	} else {
		lua_pushnil (L);
		lua_pushfstring (L, "%s", strerror(errno));
		return 2;
	}
}


/*
** Unlocks a file.
** @param #1 File handle.
** @param #2 Number with start position (optional).
** @param #3 Number with length (optional).
*/
static int file_unlock (lua_State *L) {
	FILE *fh = check_file (L, 1, "unlock");
	const long start = luaL_optlong (L, 2, 0);
	long len = luaL_optlong (L, 3, 0);
	if (_file_lock (L, fh, "u", start, len, "unlock")) {
		lua_pushboolean (L, 1);
		return 1;
	} else {
		lua_pushnil (L);
		lua_pushfstring (L, "%s", strerror(errno));
		return 2;
	}
}


static int make_dir (lua_State *L) {
	const char *path = luaL_checkstring (L, 1);
	int fail;
#ifdef _WIN32
	int oldmask = umask (0);
	fail = _mkdir (path);
#else
	mode_t oldmask = umask( (mode_t)0 );
	fail =  mkdir (path, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP |
	                     S_IWGRP | S_IXGRP | S_IROTH | S_IXOTH );
#endif
	if (fail) {
		lua_pushnil (L);
        lua_pushfstring (L, "%s", strerror(errno));
		return 2;
	}
	umask (oldmask);
	lua_pushboolean (L, 1);
	return 1;
}

/*
** Removes a directory.
** @param #1 Directory path.
*/
static int remove_dir (lua_State *L) {
	const char *path = luaL_checkstring (L, 1);
	int fail;

	fail = rmdir (path);

	if (fail) {
		lua_pushnil (L);
		lua_pushfstring (L, "%s", strerror(errno));
		return 2;
	}
	lua_pushboolean (L, 1);
	return 1;
}

/*
** Directory iterator
*/
static int dir_iter (lua_State *L) {
#ifdef _WIN32
	struct _finddata_t c_file;
#else
	struct dirent *entry;
#endif
	dir_data *d = (dir_data *)lua_touserdata (L, lua_upvalueindex (1));
	luaL_argcheck (L, !d->closed, 1, "closed directory");
#ifdef _WIN32
	if (d->hFile == 0L) { /* first entry */
		if ((d->hFile = _findfirst (d->pattern, &c_file)) == -1L) {
			lua_pushnil (L);
			lua_pushstring (L, strerror (errno));
			return 2;
		} else {
			lua_pushstring (L, c_file.name);
			return 1;
		}
	} else { /* next entry */
		if (_findnext (d->hFile, &c_file) == -1L) {
			/* no more entries => close directory */
			_findclose (d->hFile);
			d->closed = 1;
			return 0;
		} else {
			lua_pushstring (L, c_file.name);
			return 1;
		}
	}
#else
	if ((entry = readdir (d->dir)) != NULL) {
		lua_pushstring (L, entry->d_name);
		return 1;
	} else {
		/* no more entries => close directory */
		closedir (d->dir);
		d->closed = 1;
		return 0;
	}
#endif
}


/*
** Closes directory iterators
*/
static int dir_close (lua_State *L) {
	dir_data *d = (dir_data *)lua_touserdata (L, 1);
#ifdef _WIN32
	if (!d->closed && d->hFile) {
		_findclose (d->hFile);
		d->closed = 1;
	}
#else
	if (!d->closed && d->dir) {
		closedir (d->dir);
		d->closed = 1;
	}
#endif
	return 0;
}


/*
** Factory of directory iterators
*/
static int dir_iter_factory (lua_State *L) {
	const char *path = luaL_checkstring (L, 1);
	dir_data *d = (dir_data *) lua_newuserdata (L, sizeof(dir_data));
	d->closed = 0;
#ifdef _WIN32
	d->hFile = 0L;
	luaL_getmetatable (L, DIR_METATABLE);
	lua_setmetatable (L, -2);
	if (strlen(path) > MAX_DIR_LENGTH)
		luaL_error (L, "path too long: %s", path);
	else
		sprintf (d->pattern, "%s/*", path);
#else
	luaL_getmetatable (L, DIR_METATABLE);
	lua_setmetatable (L, -2);
	d->dir = opendir (path);
	if (d->dir == NULL)
		luaL_error (L, "cannot open %s: %s", path, strerror (errno));
#endif
	lua_pushcclosure (L, dir_iter, 1);
	return 1;
}


/*
** Creates directory metatable.
*/
static int dir_create_meta (lua_State *L) {
	luaL_newmetatable (L, DIR_METATABLE);
	/* set its __gc field */
	lua_pushstring (L, "__gc");
	lua_pushcfunction (L, dir_close);
	lua_settable (L, -3);

	return 1;
}


#ifdef _WIN32
 #ifndef S_ISDIR
   #define S_ISDIR(mode)  (mode&_S_IFDIR)
 #endif
 #ifndef S_ISREG
   #define S_ISREG(mode)  (mode&_S_IFREG)
 #endif
 #ifndef S_ISLNK
   #define S_ISLNK(mode)  (0)
 #endif
 #ifndef S_ISSOCK
   #define S_ISSOCK(mode)  (0)
 #endif
 #ifndef S_ISFIFO
   #define S_ISFIFO(mode)  (0)
 #endif
 #ifndef S_ISCHR
   #define S_ISCHR(mode)  (mode&_S_IFCHR)
 #endif
 #ifndef S_ISBLK
   #define S_ISBLK(mode)  (0)
 #endif
#endif
/*
** Convert the inode protection mode to a string.
*/
#ifdef _WIN32
static const char *mode2string (unsigned short mode) {
#else
static const char *mode2string (mode_t mode) {
#endif
  if ( S_ISREG(mode) )
    return "file";
  else if ( S_ISDIR(mode) )
    return "directory";
  else if ( S_ISLNK(mode) )
	return "link";
  else if ( S_ISSOCK(mode) )
    return "socket";
  else if ( S_ISFIFO(mode) )
	return "named pipe";
  else if ( S_ISCHR(mode) )
	return "char device";
  else if ( S_ISBLK(mode) )
	return "block device";
  else
	return "other";
}


/*
** Set access time and modification values for file
*/
static int file_utime (lua_State *L) {
	const char *file = luaL_checkstring (L, 1);
	struct utimbuf utb, *buf;

	if (lua_gettop (L) == 1) /* set to current date/time */
		buf = NULL;
	else {
		utb.actime = (time_t)luaL_optnumber (L, 2, 0);
		utb.modtime = (time_t)luaL_optnumber (L, 3, utb.actime);
		buf = &utb;
	}
	if (utime (file, buf)) {
		lua_pushnil (L);
		lua_pushfstring (L, "%s", strerror (errno));
		return 2;
	}
	lua_pushboolean (L, 1);
	return 1;
}


/* inode protection mode */
static void push_st_mode (lua_State *L, struct stat *info) {
	lua_pushstring (L, mode2string (info->st_mode));
}
/* device inode resides on */
static void push_st_dev (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_dev);
}
/* inode's number */
static void push_st_ino (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_ino);
}
/* number of hard links to the file */
static void push_st_nlink (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_nlink);
}
/* user-id of owner */
static void push_st_uid (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_uid);
}
/* group-id of owner */
static void push_st_gid (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_gid);
}
/* device type, for special file inode */
static void push_st_rdev (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_rdev);
}
/* time of last access */
static void push_st_atime (lua_State *L, struct stat *info) {
	lua_pushnumber (L, info->st_atime);
}
/* time of last data modification */
static void push_st_mtime (lua_State *L, struct stat *info) {
	lua_pushnumber (L, info->st_mtime);
}
/* time of last file status change */
static void push_st_ctime (lua_State *L, struct stat *info) {
	lua_pushnumber (L, info->st_ctime);
}
/* file size, in bytes */
static void push_st_size (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_size);
}
#ifndef _WIN32
/* blocks allocated for file */
static void push_st_blocks (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_blocks);
}
/* optimal file system I/O blocksize */
static void push_st_blksize (lua_State *L, struct stat *info) {
	lua_pushnumber (L, (lua_Number)info->st_blksize);
}
#endif

typedef void (*_push_function) (lua_State *L, struct stat *info);

struct _stat_members {
	const char *name;
	_push_function push;
};

struct _stat_members members[] = {
	{ "mode",         push_st_mode },
	{ "dev",          push_st_dev },
	{ "ino",          push_st_ino },
	{ "nlink",        push_st_nlink },
	{ "uid",          push_st_uid },
	{ "gid",          push_st_gid },
	{ "rdev",         push_st_rdev },
	{ "access",       push_st_atime },
	{ "modification", push_st_mtime },
	{ "change",       push_st_ctime },
	{ "size",         push_st_size },
#ifndef _WIN32
	{ "blocks",       push_st_blocks },
	{ "blksize",      push_st_blksize },
#endif
	{ NULL, NULL }
};

/*
** Get file information
*/
static int file_info (lua_State *L) {
	int i;
	struct stat info;
	const char *file = luaL_checkstring (L, 1);

	if (stat(file, &info)) {
		lua_pushnil (L);
		lua_pushfstring (L, "cannot obtain information from file `%s'", file);
		return 2;
	}
	if (lua_isstring (L, 2)) {
		int v;
		const char *member = lua_tostring (L, 2);
		if (strcmp (member, "mode") == 0) v = 0;
#ifndef _WIN32
		else if (strcmp (member, "blksize") == 0) v = 12;
#endif
		else /* look for member */
			for (v = 1; members[v].name; v++)
				if (*members[v].name == *member)
					break;
		/* push member value and return */
		members[v].push (L, &info);
		return 1;
	} else if (!lua_istable (L, 2))
		/* creates a table if none is given */
		lua_newtable (L);
	/* stores all members in table on top of the stack */
	for (i = 0; members[i].name; i++) {
		lua_pushstring (L, members[i].name);
		members[i].push (L, &info);
		lua_rawset (L, -3);
	}
	return 1;
}


/*
** Assumes the table is on top of the stack.
*/
static void set_info (lua_State *L) {
	lua_pushliteral (L, "_COPYRIGHT");
	lua_pushliteral (L, "Copyright (C) 2003-2006 Kepler Project");
	lua_settable (L, -3);
	lua_pushliteral (L, "_DESCRIPTION");
	lua_pushliteral (L, "LuaFileSystem is a Lua library developed to complement the set of functions related to file systems offered by the standard Lua distribution");
	lua_settable (L, -3);
	lua_pushliteral (L, "_VERSION");
	lua_pushliteral (L, "LuaFileSystem 1.2");
	lua_settable (L, -3);
}


static const struct luaL_reg fslib[] = {
	{"attributes", file_info},
	{"chdir", change_dir},
	{"currentdir", get_dir},
	{"dir", dir_iter_factory},
	{"lock", file_lock},
	{"mkdir", make_dir},
	{"rmdir", remove_dir},
	{"touch", file_utime},
	{"unlock", file_unlock},
	{NULL, NULL},
};

LUAEXT_API int luaopen_lfs (lua_State *L) {
	dir_create_meta (L);
	luaL_register (L, "lfs", fslib);
	set_info (L);
	return 1;
}
