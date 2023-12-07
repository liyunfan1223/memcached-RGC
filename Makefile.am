bin_PROGRAMS = memhc
pkginclude_HEADERS = protocol_binary.h xxhash.h
noinst_PROGRAMS = memhc-debug sizes testapp timedrun

BUILT_SOURCES=

testapp_SOURCES = testapp.c util.c util.h stats_prefix.c stats_prefix.h jenkins_hash.c murmur3_hash.c hash.h cache.c crc32c.c

timedrun_SOURCES = timedrun.c

memhc_SOURCES = memhc.c memhc.h \
                    hash.c hash.h \
                    jenkins_hash.c jenkins_hash.h \
                    murmur3_hash.c murmur3_hash.h \
                    queue.h \
                    slabs.c slabs.h \
                    items.c items.h \
                    assoc.c assoc.h \
                    thread.c daemon.c \
                    stats_prefix.c stats_prefix.h \
                    util.c util.h \
                    trace.h cache.c cache.h sasl_defs.h \
                    bipbuffer.c bipbuffer.h \
                    base64.c base64.h \
                    logger.c logger.h \
                    crawler.c crawler.h \
                    itoa_ljust.c itoa_ljust.h \
                    slab_automove.c slab_automove.h \
                    authfile.c authfile.h \
                    restart.c restart.h \
                    proto_text.c proto_text.h \
                    proto_bin.c proto_bin.h

if BUILD_SOLARIS_PRIVS
memhc_SOURCES += solaris_priv.c
endif

if BUILD_LINUX_PRIVS
memhc_SOURCES += linux_priv.c
endif

if BUILD_OPENBSD_PRIVS
memhc_SOURCES += openbsd_priv.c
endif

if BUILD_FREEBSD_PRIVS
memhc_SOURCES += freebsd_priv.c
endif

if BUILD_DARWIN_PRIVS
memhc_SOURCES += darwin_priv.c
endif

if ENABLE_SASL
memhc_SOURCES += sasl_defs.c
endif

if ENABLE_PROXY
memhc_SOURCES += proto_proxy.c proto_proxy.h vendor/mcmc/mcmc.h \
					 proxy_xxhash.c proxy.h \
					 proxy_await.c proxy_ustats.c \
					 proxy_jump_hash.c proxy_request.c \
					 proxy_network.c proxy_lua.c \
					 proxy_config.c proxy_ring_hash.c \
					 proxy_internal.c \
					 md5.c md5.h
endif

if ENABLE_EXTSTORE
memhc_SOURCES += extstore.c extstore.h \
                     crc32c.c crc32c.h \
                     storage.c storage.h \
                     slab_automove_extstore.c slab_automove_extstore.h
endif

if ENABLE_TLS
memhc_SOURCES += tls.c tls.h
endif

memhc_debug_SOURCES = $(memhc_SOURCES)
memhc_CPPFLAGS = -DNDEBUG
memhc_debug_LDADD = @PROFILER_LDFLAGS@
memhc_debug_CFLAGS = @PROFILER_FLAGS@

memhc_LDADD =
memhc_DEPENDENCIES =
memhc_debug_DEPENDENCIES =
CLEANFILES=

if BUILD_LINUX_PRIVS
memhc_LDADD += -lseccomp
memhc_debug_LDADD += -lseccomp
endif

if BUILD_DTRACE
BUILT_SOURCES += memhc_dtrace.h
CLEANFILES += memhc_dtrace.h
endif

if DTRACE_INSTRUMENT_OBJ
memhc_LDADD += memhc_dtrace.o
memhc_DEPENDENCIES += memhc_dtrace.o
memhc_debug_LDADD += memhc_debug_dtrace.o
memhc_debug_DEPENDENCIES += memhc_debug_dtrace.o
CLEANFILES += memhc_dtrace.o memhc_debug_dtrace.o
endif

if ENABLE_PROXY
memhc_LDADD += vendor/lua/src/liblua.a vendor/mcmc/mcmc.o
memhc_debug_LDADD += vendor/lua/src/liblua.a vendor/mcmc/mcmc.o
endif

if ENABLE_PROXY_URING
memhc_LDADD += vendor/liburing/src/liburing.a
memhc_debug_LDADD += vendor/liburing/src/liburing.a
endif

memhc_debug_CFLAGS += -Dmemhc_DEBUG

# build fails on Darwin with const signature replacements.
if DARWIN
memhc_dtrace.h: memhc_dtrace.d
	${DTRACE} -h -s memhc_dtrace.d
else
memhc_dtrace.h: memhc_dtrace.d
	${DTRACE} -h -s memhc_dtrace.d
	sed -e 's,void \*,const void \*,' memhc_dtrace.h | \
	sed -e 's,char \*,const char \*,g' | tr '\t' ' ' > mmc_dtrace.tmp
	mv mmc_dtrace.tmp memhc_dtrace.h
endif

memhc_dtrace.o: $(memhc_OBJECTS)
	$(DTRACE) $(DTRACEFLAGS) -G -o memhc_dtrace.o -s ${srcdir}/memhc_dtrace.d $(memhc_OBJECTS)

memhc_debug_dtrace.o: $(memhc_debug_OBJECTS)
	$(DTRACE) $(DTRACEFLAGS) -G -o memhc_debug_dtrace.o -s ${srcdir}/memhc_dtrace.d $(memhc_debug_OBJECTS)


SUBDIRS = doc
DIST_DIRS = scripts
EXTRA_DIST = doc scripts t memhc.spec memhc_dtrace.d version.m4 README.md LICENSE.bipbuffer
EXTRA_DIST += vendor/Makefile vendor/lua vendor/mcmc

if ENABLE_PROXY
SUBDIRS += vendor
endif

MOSTLYCLEANFILES = *.gcov *.gcno *.gcda *.tcov

if ENABLE_TLS
test_tls:
	$(MAKE) SSL_TEST=1 test

test_basic_tls:
	@if test $(SSL_TEST)1 != 1; then \
	  echo "Running basic tests with TLS"; \
	  $(builddir)/testapp && \
	  prove $(srcdir)/t/binary.t $(srcdir)/t/getset.t $(srcdir)/t/ssl* && \
	  echo "Finished running basic TLS tests"; \
	else \
	  echo "Set SSL_TEST=1 to enable TLS tests"; \
	fi
endif

test:	memhc-debug sizes testapp
	$(builddir)/sizes
	$(builddir)/testapp
if ENABLE_TLS
	@if test $(SSL_TEST)1 = 1; then \
          $(MAKE) SSL_TEST=1  test_basic_tls; \
	fi
endif
	@if test -n "${PARALLEL}"; then \
	  prove $(srcdir)/t -j ${PARALLEL}; \
	else \
	  prove $(srcdir)/t; \
	fi
	@if test `basename $(PROFILER)` = "gcov"; then \
	  for file in memhc_debug-*.gc??; do \
	    mv -f $$file `echo $$file | sed 's/memhc_debug-//'`; \
	  done && \
	  for file in *.gcda; do \
	    srcfile=`echo $$file | sed 's/.gcda/.c/'`; \
	    if test -n "`echo $(memhc_debug_SOURCES) | grep $$srcfile`"; then \
	      echo `$(PROFILER) $$srcfile` | sed 's/'$$srcfile':.*//'; \
	    fi \
	  done \
	elif test `basename $(PROFILER)` = "tcov"; then \
	  files=`grep SRCFILE memhc-debug.profile/tcovd | sed 's/SRCFILE://' | sort | uniq` && \
	  $(PROFILER) -x memhc-debug.profile $$files 2>&1; \
	  for file in *.tcov; do \
	    srcfile=`echo $$file | sed 's/.tcov//'`; \
	    if test -n "`echo $(memhc_debug_SOURCES) | grep $$srcfile`"; then \
	      echo $$srcfile : `grep 'Percent of the file executed' $$file`; \
	    fi \
	  done \
	else :; fi

docs:
	(cat Doxyfile ; echo "PROJECT_NUMBER=`cat version.num`") | doxygen -

dist-hook:
	rm -f $(distdir)/*/*~ $(distdir)/t/lib/*~ $(distdir)/*~

maintainer-clean-local:
	-rm Makefile.in
	-rm aclocal.m4
	-rm config.guess
	-rm config.sub
	-rm depcomp
	-rm install-sh
	-rm ltmain.sh
	-rm missing
	-rm configure
	-rm config.log
	-rm config.status
