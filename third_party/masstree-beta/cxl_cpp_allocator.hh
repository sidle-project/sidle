#ifndef CXL_ALLOCATOR_HH
#define CXL_ALLOCATOR_HH

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <new>
#include <memkind.h>

extern "C" {
    #include "../cxl_utils/cxl_allocator.h"
}

#define CXL 1

extern void * 
operator new(std::size_t size);

extern void *
operator new[](std::size_t size);

extern void 
operator delete(void* ptr) noexcept;

extern void
operator delete[](void* ptr) noexcept;

#endif  // CXL_ALLOCATOR_HH