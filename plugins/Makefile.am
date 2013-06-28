SUBDIRS=\
	admin \
	proxy \
	replicant \
	debug 
# the cli plugin needs readline and we don't have it on all platforms
# cli

EXTRA_DIST=CMakeLists.txt

check-local:
	mkdir -p $(top_builddir)/plugins/; 
	for plugin in $(SUBDIRS); do \
		for i in `cat $(top_builddir)/plugins/$$plugin/.libs/lib$$plugin.la | grep '^dlname' | sed "s/.*=//;s/'//g"`; do \
			echo $$i; \
			ln -fs $(top_builddir)/plugins/$$plugin/.libs/$$i $(top_builddir)/plugins/$$i; \
		done; \
	done;

clean-local:
	## remove left-overs from the check-local hack in the plugings
