#!/bin/sh
# $%BEGINLICENSE%$
# $%ENDLICENSE%$

VERSION_INFO=`bzr version-info --custom --template="{branch_nick} Revision {revno} from {date}"`
echo "\"$1 - $VERSION_INFO\""
