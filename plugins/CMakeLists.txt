IF(WIN32) 
	## make sure we only use the smallest set of 
	## headers on win32. Otherwise we get clashes 
	## between winsock2.h and winsock.h
	ADD_DEFINITIONS(-DWIN32_LEAN_AND_MEAN)
ENDIF(WIN32)
ADD_SUBDIRECTORY(debug)
ADD_SUBDIRECTORY(proxy)
ADD_SUBDIRECTORY(admin)
ADD_SUBDIRECTORY(replicant)
## needs readline
# ADD_SUBDIRECTORY(cli)
