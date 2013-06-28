EXTRA_DIST=

example_scripts = \
	tutorial-basic.lua \
	tutorial-constants.lua \
	tutorial-inject.lua \
	tutorial-keepalive.lua \
	tutorial-monitor.lua \
	tutorial-packets.lua \
	tutorial-query-time.lua \
	tutorial-prep-stmts.lua \
	tutorial-resultset.lua \
	tutorial-rewrite.lua \
	tutorial-routing.lua \
	tutorial-scramble.lua \
	tutorial-states.lua \
	tutorial-tokenize.lua \
	tutorial-union.lua \
	tutorial-warnings.lua

if USE_WRAPPER_SCRIPT
dist_doc_DATA = ${example_scripts}
else
EXTRA_DIST += ${example_scripts}
endif

EXTRA_DIST+=CMakeLists.txt
