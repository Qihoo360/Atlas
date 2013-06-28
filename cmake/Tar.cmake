# $%BEGINLICENSE%$
# $%ENDLICENSE%$

MACRO(TAR_UNPACK _file _wd)
	FIND_PROGRAM(GTAR_EXECUTABLE NAMES gtar tar)
	FIND_PROGRAM(GZIP_EXECUTABLE NAMES gzip)

	MESSAGE(STATUS "gtar: ${GTAR_EXECUTABLE}")
	MESSAGE(STATUS "gzip: ${GZIP_EXECUTABLE}")

	IF(GTAR_EXECUTABLE AND GZIP_EXECUTABLE)
		# On Windows gzip -cd $file | tar xvf - sometimes reports
		# "Broken pipe" which causes cmake to assume an error has occured
		# In fact, this is harmless, but because of it, this is done in
		# two steps
		MESSAGE(STATUS "unzipping ${_file} with ${GZIP_EXECUTABLE}")
		EXECUTE_PROCESS(COMMAND ${GZIP_EXECUTABLE} "-cd" ${_file}
		  OUTPUT_FILE "${_file}.tar"
			WORKING_DIRECTORY ${_wd}
			ERROR_VARIABLE _err)
		IF(_err)
			MESSAGE(SEND_ERROR "GZIP_UNPACK()-err: ${_err}")
		ENDIF(_err)
		# untar after successful unzip
		MESSAGE(STATUS "untarring ${_file}.tar with ${GTAR_EXECUTABLE}")
		EXECUTE_PROCESS(COMMAND ${GTAR_EXECUTABLE} "xvf" "-"
			INPUT_FILE "${_file}.tar"
			WORKING_DIRECTORY ${_wd}
			OUTPUT_VARIABLE _out
			ERROR_VARIABLE _err)
		IF(_err)
			MESSAGE(SEND_ERROR "TAR_UNPACK()-err: ${_err}")
		ENDIF(_err)
		IF(_out)
			MESSAGE(DEBUG "TAR_UNPACK()-out: ${_out}")
		ENDIF(_out)
		FILE(REMOVE "${_file}.tar")
	ELSE(GTAR_EXECUTABLE AND GZIP_EXECUTABLE)
		MESSAGE(STATUS "gnutar not found")
	ENDIF(GTAR_EXECUTABLE AND GZIP_EXECUTABLE)
ENDMACRO(TAR_UNPACK _file _wd)


MACRO(ZIP_UNPACK _file _wd)
	FIND_PROGRAM(7Z_EXECUTABLE NAMES 7z 7za)

	MESSAGE(STATUS "7zip: ${7Z_EXECUTABLE}")

	IF(7Z_EXECUTABLE)
		EXECUTE_PROCESS(COMMAND ${7Z_EXECUTABLE} x "-o${_wd}" "${_file}")
	ELSE(7Z_EXECUTABLE)
		MESSAGE(STATUS "7zip not found")
	ENDIF(7Z_EXECUTABLE)
ENDMACRO(ZIP_UNPACK _file _wd)


