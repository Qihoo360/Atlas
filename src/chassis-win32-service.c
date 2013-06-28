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

#include "chassis-win32-service.h"
#include "chassis-mainloop.h" /* for chassis_set_shutdown */

#ifdef _WIN32
/**
 * win32 service functions
 *
 */

#include <windows.h>
#include <winsock2.h>

static char **shell_argv;
static int shell_argc;
static int (*shell_main)(int, char **);
static int chassis_win32_running_as_service = 0;
static SERVICE_STATUS chassis_win32_service_status;
static SERVICE_STATUS_HANDLE chassis_win32_service_status_handle = 0;

gboolean chassis_win32_is_service(void) {
	return chassis_win32_running_as_service;
}

void chassis_win32_service_set_state(DWORD new_state, int wait_msec) {
	DWORD status;
	
	/* safeguard against a missing if(chassis_win32_running_as_service) in other code */
	if (!chassis_win32_running_as_service) return;

	g_assert(chassis_win32_service_status_handle);
	
	switch(new_state) {
	case SERVICE_START_PENDING:
	case SERVICE_STOP_PENDING:
		chassis_win32_service_status.dwWaitHint = wait_msec;
		
		if (chassis_win32_service_status.dwCurrentState == new_state) {
			chassis_win32_service_status.dwCheckPoint++;
		} else {
			chassis_win32_service_status.dwCheckPoint = 0;
		}
		
		break;
	default:
		chassis_win32_service_status.dwWaitHint = 0;
		break;
	}
	
	chassis_win32_service_status.dwCurrentState = new_state;
	
	if (!SetServiceStatus (chassis_win32_service_status_handle, &chassis_win32_service_status)) {
		status = GetLastError();
	}
}

/**
 * the event-handler for the service
 * 
 * the SCM will send us events from time to time which we acknoledge
 */

static DWORD WINAPI chassis_win32_service_ctrl(DWORD Opcode, DWORD EventType, LPVOID EventData, LPVOID _udata) {
	switch(Opcode) {
	case SERVICE_CONTROL_SHUTDOWN:
	case SERVICE_CONTROL_STOP:
		chassis_win32_service_set_state(SERVICE_STOP_PENDING, 0);
		
		chassis_set_shutdown(); /* exit the main-loop */
		
		return NO_ERROR;
	case SERVICE_CONTROL_INTERROGATE:
		/* even if we don't implement it, we should return NO_ERROR here */
		return NO_ERROR;
	default:
		chassis_win32_service_set_state(Opcode, 0); /* forward the state changes */
		return ERROR_CALL_NOT_IMPLEMENTED;
	}
}

/**
 * trampoline us into the real main_cmdline
 */
static void WINAPI chassis_win32_service_start(DWORD argc, LPTSTR *argv) {
	const char *service_name = "MerlinAgent";
	int ret;
	
	/* tell the service controller that we are alive */
	chassis_win32_service_status.dwCurrentState       = SERVICE_START_PENDING;
	chassis_win32_service_status.dwCheckPoint         = 0;
	chassis_win32_service_status.dwServiceType        = SERVICE_WIN32_OWN_PROCESS;
	chassis_win32_service_status.dwControlsAccepted   = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
	chassis_win32_service_status.dwWin32ExitCode      = NO_ERROR;
	chassis_win32_service_status.dwServiceSpecificExitCode = 0;
	
	chassis_win32_service_status_handle = RegisterServiceCtrlHandlerEx(service_name, chassis_win32_service_ctrl, NULL); 
	
	if (chassis_win32_service_status_handle == (SERVICE_STATUS_HANDLE)0) {
		int err = GetLastError();

		g_critical("%s: RegisterServiceCtrlHandler(%s, ...) failed: %s (%d)",
				G_STRLOC,
				service_name,
				g_strerror(err),
				err);

		return; 
	}
	
	chassis_win32_service_set_state(SERVICE_START_PENDING, 1000); /* wait max 1sec before we get the to the next step */

	g_assert(shell_main);
	
	/* jump into the actual main */
	ret = shell_main(shell_argc, shell_argv);
	
	/* FIXME: should we log if we fail to start ? */
}

/**
 * Determine whether we are called as a service and set that up.
 * Then call main_cmdline to do the real work.
 */
int main_win32(int argc, char **argv, int (*_main_cmdline)(int, char **)) {
	WSADATA wsaData;

	SERVICE_TABLE_ENTRY dispatch_tab[] = {
		{ "", chassis_win32_service_start }, /* we use SERVICE_WIN32_OWN_PROCESS, so the name can be "" */
		{ NULL, NULL } 
	};

	if (0 != WSAStartup(MAKEWORD( 2, 2 ), &wsaData)) {
		g_critical("WSAStartup failed to initialize the socket library.\n");

		return -1;
	}

	/* save the arguments because the service controller will clobber them */
	shell_main = _main_cmdline;
	shell_argc = argc;
	shell_argv = argv;

	/* speculate that we are running as a service, reset to 0 on error */
	chassis_win32_running_as_service = TRUE;
	
	if (!StartServiceCtrlDispatcher(dispatch_tab)) {
		int err = GetLastError();
		
		switch(err) {
		case ERROR_FAILED_SERVICE_CONTROLLER_CONNECT:
			/* we weren't called as a service, carry on with the cmdline handling */
			chassis_win32_running_as_service = FALSE;
			return shell_main(shell_argc, shell_argv);
		case ERROR_SERVICE_ALREADY_RUNNING:
			g_critical("service is already running, shutting down");
			return 0;
		default:
			g_critical("unhandled error-code (%d) for StartServiceCtrlDispatcher(), shutting down", err);
			return -1;
		}
	} else {
		/* the service-thread is started, return to the shell */
	}
	return 0;
}
#else
/**
 * just a stub in case we aren't on win32
 */
int main_win32(int G_GNUC_UNUSED argc, char G_GNUC_UNUSED **argv, int G_GNUC_UNUSED (*_main_cmdline)(int, char **)) {
	return -1;
}
#endif
