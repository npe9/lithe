# Add the m4 directory to the include path for aclocal
ACLOCAL_AMFLAGS=-I m4

# Set up global variables
LIBNAME = lithe
CAP_LIBNAME = Lithe
SRCDIR = $(srcdir)/@SRCDIR@

LIB_CFILES = \
  @SRCDIR@/fatal.c         \
  @SRCDIR@/defaults.c      \
  @SRCDIR@/barrier.c \
  @SRCDIR@/lithe.c         \
  @SRCDIR@/condvar.c         \
  @SRCDIR@/semaphore.c         \
  @SRCDIR@/futex.c         \
  @SRCDIR@/mutex.c \
  @SRCDIR@/fork_join_sched.c

LIB_CXXFILES = \
  @SRCDIR@/sched.cc \
  @SRCDIR@/context.cc

LIB_HFILES = \
  @SRCDIR@/context.h       \
  @SRCDIR@/defaults.h      \
  @SRCDIR@/fatal.h         \
  @SRCDIR@/hart.h          \
  @SRCDIR@/barrier.h \
  @SRCDIR@/condvar.h   \
  @SRCDIR@/semaphore.h   \
  @SRCDIR@/futex.h   \
  @SRCDIR@/mutex.h   \
  @SRCDIR@/lithe.h         \
  @SRCDIR@/sched.h \
  @SRCDIR@/fork_join_sched.h

LIB_HHFILES = \
  @SRCDIR@/lithe.hh \
  @SRCDIR@/sched.hh \
  @SRCDIR@/context.hh

TEST_EXECS = \
  test_cls        \
  test_mutex        \
  test_syscalls        \
  test_recursive_mutex        \
  test_condvar        \
  test_parent       \
  test_scheduler    \
  test_mutex_cc     \
  test_recursive_mutex_cc        \
  test_condvar_cc     \
  test_parent_cc    \
  test_scheduler_cc

# Setup parameters to build the library
lib_LTLIBRARIES = liblithe.la
liblithe_la_CFLAGS = $(AM_CFLAGS)
liblithe_la_CXXFLAGS = $(AM_CXXFLAGS)
liblithe_la_SOURCES = $(LIB_CFILES) $(LIB_HFILES)
# Exclude C++ files to make it a pure C library
# liblithe_la_SOURCES += $(LIB_CXXFILES) $(LIB_HHFILES)
if STATIC_ONLY 
liblithe_la_LIBADD = $(LPARLIB) -lpthread
liblithe_la_LDFLAGS = -all-static 
else
liblithe_la_LIBADD = $(LPARLIB)
endif 

# Setup a directory where all of the include files will be installed
litheincdir = $(includedir)/$(LIBNAME)
dist_litheinc_DATA = $(LIB_HFILES) $(LIB_HHFILES)

# Setup parameters to build the test programs
check_PROGRAMS = $(TEST_EXECS)

test_cls_SOURCES = @TESTSDIR@/test-cls.c
test_cls_CFLAGS = $(AM_CFLAGS)
test_cls_CFLAGS += -I$(srcdir)
test_cls_LDADD = -lithe $(LPARLIB)

test_syscalls_SOURCES = @TESTSDIR@/test-syscalls.c
test_syscalls_CFLAGS = $(AM_CFLAGS)
test_syscalls_CFLAGS += -I$(srcdir)
test_syscalls_LDADD = -lithe $(LPARLIB)

test_mutex_SOURCES = @TESTSDIR@/test-mutex.c
test_mutex_CFLAGS = $(AM_CFLAGS)
test_mutex_CFLAGS += -I$(srcdir)
test_mutex_LDADD = -lithe $(LPARLIB)

test_recursive_mutex_SOURCES = @TESTSDIR@/test-recursive-mutex.c
test_recursive_mutex_CFLAGS = $(AM_CFLAGS)
test_recursive_mutex_CFLAGS += -I$(srcdir)
test_recursive_mutex_LDADD = -lithe $(LPARLIB)

test_condvar_SOURCES = @TESTSDIR@/test-condvar.c
test_condvar_CFLAGS = $(AM_CFLAGS)
test_condvar_CFLAGS += -I$(srcdir)
test_condvar_LDADD = -lithe $(LPARLIB)

test_parent_SOURCES = @TESTSDIR@/test-parent.c
test_parent_CFLAGS = $(AM_CFLAGS)
test_parent_CFLAGS += -I$(srcdir)
test_parent_LDADD = -lithe $(LPARLIB)

test_scheduler_SOURCES = @TESTSDIR@/test-scheduler.c
test_scheduler_CFLAGS = $(AM_CFLAGS)
test_scheduler_CFLAGS += -I$(srcdir)
test_scheduler_LDADD = -lithe $(LPARLIB)

test_mutex_cc_SOURCES = @TESTSDIR@/test-mutex.cc
test_mutex_cc_CXXFLAGS = $(AM_CXXFLAGS)
test_mutex_cc_CXXFLAGS += -I$(srcdir)
test_mutex_cc_LDADD = -lithe $(LPARLIB)

test_recursive_mutex_cc_SOURCES = @TESTSDIR@/test-recursive-mutex.cc
test_recursive_mutex_cc_CFLAGS = $(AM_CFLAGS)
test_recursive_mutex_cc_CFLAGS += -I$(srcdir)
test_recursive_mutex_cc_LDADD = -lithe $(LPARLIB)

test_condvar_cc_SOURCES = @TESTSDIR@/test-condvar.cc
test_condvar_cc_CXXFLAGS = $(AM_CXXFLAGS)
test_condvar_cc_CXXFLAGS += -I$(srcdir)
test_condvar_cc_LDADD = -lithe $(LPARLIB)

test_parent_cc_SOURCES = @TESTSDIR@/test-parent.cc
test_parent_cc_CXXFLAGS = $(AM_CXXFLAGS)
test_parent_cc_CXXFLAGS += -I$(srcdir)
test_parent_cc_LDADD = -lithe $(LPARLIB)

test_scheduler_cc_SOURCES = @TESTSDIR@/test-scheduler.cc
test_scheduler_cc_CXXFLAGS = $(AM_CXXFLAGS)
test_scheduler_cc_CXXFLAGS += -I$(srcdir)
test_scheduler_cc_LDADD = -lithe $(LPARLIB)

if SPHINX_BUILD
man_MANS = \
  doc/man/$(LIBNAME).1

# Don't suppport doing things this way for now because automake is dumb and
# doesn't allow 'generated' texi files...
# Instead we'll add the generated .info file and install it manually...
#info_TEXINFOS = \
#  doc/texinfo/$(CAP_LIBNAME).texi

# Include pre-generated documentation in the distribution
EXTRA_DIST = \
  doc/html \
  doc/singlehtml \
  doc/man \
  doc/texinfo/$(CAP_LIBNAME).info \
  doc/latex/$(CAP_LIBNAME).pdf

$(abs_builddir)/doc/%: | $(abs_builddir)/doc
	cd $(srcdir)/doc; \
	if [ -f Makefile ]; then \
	  make BUILDDIR=$(abs_builddir)/doc $(@F); \
	else \
	  cp -R $(@F) $(@D); \
	fi;

$(abs_builddir)/doc/texinfo/$(CAP_LIBNAME).info: | $(abs_builddir)/doc
	cd $(srcdir)/doc; \
	if [ -f Makefile ]; then \
	  make BUILDDIR=$(abs_builddir)/doc info; \
	else \
	  cp -R texinfo $(@D); \
	fi;

$(abs_builddir)/doc/latex/$(CAP_LIBNAME).pdf: | $(abs_builddir)/doc
	cd $(srcdir)/doc; \
	if [ -f Makefile ]; then \
	  make BUILDDIR=$(abs_builddir)/doc latexpdf; \
	else \
	  cp -R latex $(@D); \
	fi;

$(abs_builddir)/doc:
	mkdir -p $@

doc/%: $(abs_builddir)/doc/%
doc/man/$(LIBNAME).1: $(abs_builddir)/doc/man
doc/texinfo/$(CAP_LIBNAME).info: $(abs_builddir)/doc/texinfo/$(CAP_LIBNAME).info
doc/latex/$(CAP_LIBNAME).pdf: $(abs_builddir)/doc/latex/$(CAP_LIBNAME).pdf

doc: \
  $(abs_builddir)/doc/html \
  $(abs_builddir)/doc/singlehtml \
  $(abs_builddir)/doc/man \
  $(abs_builddir)/doc/texinfo/$(CAP_LIBNAME).info \
  $(abs_builddir)/doc/latex/$(CAP_LIBNAME).pdf

clean-local:
	rm -rf doc

install-data-hook: doc
	install -d $(docdir)
	install -d $(infodir)
	cp -r $(abs_builddir)/doc/html $(docdir)
	cp -r $(abs_builddir)/doc/singlehtml $(docdir)
	cp -r $(abs_builddir)/doc/texinfo/$(CAP_LIBNAME).info $(infodir)
	cp -r $(abs_builddir)/doc/latex/$(CAP_LIBNAME).pdf $(docdir)
endif

ChangeLog:
	cd $(srcdir); \
	if [ -f git-changelog.sh ]; then \
	  ./git-changelog.sh; \
	fi;

uninstall-hook:
	rm -rf $(litheincdir)
	rm -rf $(docdir)
	rm -rf $(infodir)/$(CAP_LIBNAME).info

.PHONY: ChangeLog

