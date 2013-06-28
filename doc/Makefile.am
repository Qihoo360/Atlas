SUBDIRS=chapter

EXTRA_DIST = \
	     lua-classes.dot \
	     architecture.dot \
	     architecture-overview.dot \
	     architecture.txt \
	     core.txt \
	     chassis.txt \
	     plugins.txt \
	     protocol.txt \
	     tests.txt \
	     scripting.txt \
	     lifecycle.msc

clean-local:
	rm -f *.html

html-local: book.html protocol.html scripting.html

## we use http://docutils.sourceforge.net/rst.html to generate the docs
book.html: book.txt chapter/scripting.txt chapter/protocol.txt
	${RST2HTML} $< $@

protocol.html: protocol.txt chapter/protocol.txt
	${RST2HTML} $< $@

scripting.html: scripting.txt chapter/scripting.txt
	${RST2HTML} $< $@

