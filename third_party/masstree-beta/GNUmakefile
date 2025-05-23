AR = ar
CC = gcc
CXX = g++
CPPFLAGS = 
CXXFLAGS = -g -W -Wall -O3
RANLIB = ranlib
DEPSDIR := .deps
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP
ifeq ($(strip $(MEMMGR)), )
  MEMMGR = -ljemalloc
endif
ifneq ($(strip $(KEYSWAP)), )
  CPPFLAGS += -DKEYSWAP
endif
ifneq ($(strip $(NOPREFETCH)), )
  CPPFLAGS += -DNOPREFETCH
endif
ifneq ($(strip $(NOSUPERPAGE)), )
  CPPFLAGS += -DNOSUPERPAGE
endif
LIBS = -lnuma  -lpthread -lm
LDFLAGS = 

all: test_atomics mtd mtclient mttest

%.o: %.c config.h $(DEPSDIR)/stamp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(DEPCFLAGS) -include config.h -c -o $@ $<

%.o: %.cc config.h $(DEPSDIR)/stamp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(DEPCFLAGS) -include config.h -c -o $@ $<

%.S: %.o
	objdump -S $< > $@

libjson.a: json.o string.o straccum.o str.o msgpack.o \
	clp.o kvrandom.o compiler.o memdebug.o kvthread.o
	@rm -f $@
	$(AR) cr $@ $^
	$(RANLIB) $@

KVTREES = query_masstree.o \
	value_string.o value_array.o value_versioned_array.o \
	string_slice.o

mtd: mtd.o log.o checkpoint.o file.o misc.o $(KVTREES) \
	kvio.o libjson.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

mtclient: mtclient.o misc.o testrunner.o kvio.o libjson.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

mttest: mttest.o misc.o checkpoint.o $(KVTREES) testrunner.o \
	kvio.o libjson.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

test_string: test_string.o string.o straccum.o compiler.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

test_atomics: test_atomics.o string.o straccum.o kvrandom.o \
	json.o compiler.o kvio.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

jsontest: jsontest.o string.o straccum.o json.o compiler.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

msgpacktest: msgpacktest.o string.o straccum.o json.o compiler.o msgpack.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

scantest: scantest.o compiler.o misc.o $(KVTREES) libjson.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

unit-mt: unit-mt.o compiler.o misc.o libjson.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(MEMMGR) $(LDFLAGS) $(LIBS)

config.h: stamp-h

GNUmakefile: GNUmakefile.in config.status
	CONFIG_FILES=$@ CONFIG_HEADERS= $(SHELL) ./config.status

configure config.h.in: configure.ac
	autoreconf -i
	touch config.h.in

config.status: configure
	./configure 

$(DEPSDIR)/stamp:
	mkdir -p $(DEPSDIR)
	touch $@

stamp-h: config.h.in config.status
	CONFIG_FILES= $(SHELL) ./config.status
	echo > stamp-h

clean:
	rm -f mtd mtclient mttest test_string test_atomics *.o libjson.a
	rm -rf .deps

DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif

.PHONY: clean all
