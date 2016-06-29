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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h> /* wait4 */
#endif
#include <sys/stat.h>
#ifdef HAVE_SYS_RESOURCE_H
#include <sys/resource.h> /* getrusage */
#endif

#include <stdlib.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <errno.h>
#include <signal.h>

#include <glib.h>

#include "chassis-unix-daemon.h"

#define YY_EXIT_FAILURE 2

/**
 * start the app in the background 
 * 
 * UNIX-version
 */
void chassis_unix_daemonize(void) {
#ifdef _WIN32
	g_assert_not_reached(); /* shouldn't be tried to be called on win32 */
#else
#ifdef SIGTTOU
	signal(SIGTTOU, SIG_IGN);
#endif
#ifdef SIGTTIN
	signal(SIGTTIN, SIG_IGN);
#endif
#ifdef SIGTSTP
	signal(SIGTSTP, SIG_IGN);
#endif
	if (fork() != 0) exit(0);
	
	if (setsid() == -1) exit(0);

	signal(SIGHUP, SIG_IGN);

	if (fork() != 0) exit(0);
	
	chdir("/");
	
	umask(0);
#endif
}


/**
 * forward the signal to the process group, but not us
 */
static void chassis_unix_signal_forward(int sig) {
#ifdef _WIN32
	g_assert_not_reached(); /* shouldn't be tried to be called on win32 */
#else
	signal(sig, SIG_IGN); /* we don't want to create a loop here */

	kill(0, sig);
#endif
}

/**
 * keep the ourself alive 
 *
 * if we or the child gets a SIGTERM, we quit too
 * on everything else we restart it
 */
int chassis_unix_proc_keepalive(int *child_exit_status, const char *pid_file) {
#ifdef _WIN32
	g_assert_not_reached(); /* shouldn't be tried to be called on win32 */
	return 0; /* for VC++, to silence a warning */
#else
	int nprocs = 0;
	pid_t child_pid = -1;

	/* we ignore SIGINT and SIGTERM and just let it be forwarded to the child instead
	 * as we want to collect its PID before we shutdown too 
	 *
	 * the child will have to set its own signal handlers for this
	 */

	for (;;) {
		/* try to start the children */
		while (nprocs < 1) {
			pid_t pid = fork();

			if (pid == 0) {
				/* child */
				
				g_debug("%s: we are the child: %d",
						G_STRLOC,
						getpid());
				return 0;
			} else if (pid < 0) {
				/* fork() failed */

				g_critical("%s: fork() failed: %s (%d)",
					G_STRLOC,
					g_strerror(errno),
					errno);

				return -1;
			} else {
				/* we are the angel, let's see what the child did */
				g_message("%s: [angel] we try to keep PID=%d alive",
						G_STRLOC,
						pid);

				/* forward a few signals that are sent to us to the child instead */
				signal(SIGINT, chassis_unix_signal_forward);
				signal(SIGTERM, chassis_unix_signal_forward);
				signal(SIGHUP, chassis_unix_signal_forward);
				signal(SIGUSR1, chassis_unix_signal_forward);
				signal(SIGUSR2, chassis_unix_signal_forward);

				child_pid = pid;
				nprocs++;
			}
		}

		if (child_pid != -1) {
			struct rusage rusage;
			int exit_status;
			pid_t exit_pid;

			g_debug("%s: waiting for %d",
					G_STRLOC,
					child_pid);

#ifdef HAVE_WAIT4
			exit_pid = wait4(child_pid, &exit_status, 0, &rusage);
#else
			memset(&rusage, 0, sizeof(rusage)); /* make sure everything is zero'ed out */
			exit_pid = waitpid(child_pid, &exit_status, 0);
#endif
			g_debug("%s: %d returned: %d",
					G_STRLOC,
					child_pid,
					exit_pid);

			if (exit_pid == child_pid) {

				/* delete pid file */
				if (pid_file) {
					unlink(pid_file);
				}

				/* our child returned, let's see how it went */
				if (WIFEXITED(exit_status)) {
					if (child_exit_status) *child_exit_status = WEXITSTATUS(exit_status);

					if (*child_exit_status != YY_EXIT_FAILURE) {
						g_message("%s: [angel] PID=%d exited normally with exit-code = %d (it used %ld kBytes max)",
								G_STRLOC,
								child_pid,
								WEXITSTATUS(exit_status),
								rusage.ru_maxrss / 1024);
						return 1;
					} else {
						g_critical("%s: [angel] PID=%d died on yy_fatal_error ... waiting 2sec before restart", G_STRLOC, child_pid);
						signal(SIGINT, SIG_DFL);
						signal(SIGTERM, SIG_DFL);
						signal(SIGHUP, SIG_DFL);
						int time_towait = 2;
						while (time_towait > 0) time_towait = sleep(time_towait);
						nprocs--;
						child_pid = -1;
					}
				} else if (WIFSIGNALED(exit_status)) {
					int time_towait = 2;
					/* our child died on a signal
					 *
					 * log it and restart */

					g_critical("%s: [angel] PID=%d died on signal=%d (it used %ld kBytes max) ... waiting 2sec before restart",
							G_STRLOC,
							child_pid,
							WTERMSIG(exit_status),
							rusage.ru_maxrss / 1024);

					/**
					 * to make sure we don't loop as fast as we can, sleep a bit between 
					 * restarts
					 */
	
					signal(SIGINT, SIG_DFL);
					signal(SIGTERM, SIG_DFL);
					signal(SIGHUP, SIG_DFL);
					while (time_towait > 0) time_towait = sleep(time_towait);

					nprocs--;
					child_pid = -1;
				} else if (WIFSTOPPED(exit_status)) {
				} else {
					g_assert_not_reached();
				}
			} else if (-1 == exit_pid) {
				/* EINTR is ok, all others bad */
				if (EINTR != errno) {
					/* how can this happen ? */
					g_critical("%s: wait4(%d, ...) failed: %s (%d)",
						G_STRLOC,
						child_pid,
						g_strerror(errno),
						errno);

					return -1;
				}
			} else {
				g_assert_not_reached();
			}
		}
	}
#endif
}

