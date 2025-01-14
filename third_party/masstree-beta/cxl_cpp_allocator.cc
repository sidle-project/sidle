#include "cxl_cpp_allocator.hh"

void *
operator new(std::size_t size)
{
    #ifdef CXL
    malloc_with_cxl(size);
    #else
    ::operator new(size);
    #endif
}

void *
operator new[](std::size_t size)
{
    #ifdef CXL
    malloc_with_cxl(size);
    #else
    ::operator new[](size);
    #endif
}

extern void 
operator delete(void* ptr) noexcept
{
    #ifdef CXL
    free_with_cxl(ptr);
    #else 
    ::operator delete(ptr);
    #endif
}

extern void
operator delete[](void* ptr) noexcept
{
    #ifdef CXL
    free_with_cxl(ptr);
    #else
    ::operator delete[](ptr);
    #endif
}