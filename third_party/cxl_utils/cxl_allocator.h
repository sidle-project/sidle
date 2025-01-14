#ifndef CXL_ALLOCATOR_H
#define CXL_ALLOCATOR_H

static const size_t CXL_MAX_SIZE = 1024ULL * 1024ULL * 1024ULL * 30ULL;

/**
 * @brief Initialize the CXL memory allocator
 * @param wanted_size The desired size of CXL memory 
 * @param percentage The percentage of CXL memory to use
 */
extern void 
cxl_init(const size_t wanted_size, const int percentage);

/**
 * @brief Allocate memory using CXL or Local DRAM with the specified ratio, similar to standard malloc
 * @param size The size of memory to allocate
 * @return Pointer to allocated memory, NULL if allocation fails
 */
extern void *
malloc_with_cxl(size_t size);

/**
 * @brief Allocate and initialize memory to zero using CXL or DRAM, similar to standard calloc
 * @param num Number of elements
 * @param size Size of each element
 * @return Pointer to allocated memory, NULL if allocation fails
 */
extern void *
calloc_with_cxl(size_t num, size_t size);

/**
 * @brief Reallocate memory using CXL or DRAM, similar to standard realloc
 * @param ptr Original memory pointer
 * @param new_size New size of memory
 * @return Pointer to reallocated memory, NULL if reallocation fails
 */
extern void *
realloc_with_cxl(void *ptr, size_t new_size);

/**
 * @brief Free previously allocated CXL or DRAM memory
 * @param ptr Pointer to memory to be freed
 */
extern void 
free_with_cxl(void *ptr);

/**
 * @brief Map memory on CXL or DRAM, similar to standard mmap
 * @param addr Suggested mapping address
 * @param length Length of mapping
 * @param prot Memory protection flags
 * @param flags Mapping flags
 * @param fd File descriptor
 * @param offset File offset
 * @return Pointer to mapped memory, MAP_FAILED if mapping fails
 */
extern void *
mmap_with_cxl(void *addr, size_t length, int prot, int flags, int fd, off_t offset);

/**
 * @brief Unmap memory previously mapped with mmap_with_cxl
 * @param ptr Pointer to memory to be unmapped
 * @param size Size of memory region to unmap
 */
extern void 
munmap_with_cxl(void * const ptr, const size_t size);

/**
 * @brief Allocate aligned memory using CXL or DRAM, similar to standard posix_memalign
 * @param memptr Pointer to store allocated memory address
 * @param alignment Required alignment value
 * @param size Size of memory to allocate
 * @return 0 on success, error code on failure
 */
extern int
posix_memalign_with_cxl(void **memptr, size_t alignment, size_t size);

/**
 * @brief Clean up and destroy CXL memory allocator
 */
extern void
cxl_destroy();

/**
 * @brief Try to allocate memory on CXL
 * @param size Size of memory to allocate
 * @param ptr Address to store allocated memory pointer
 * @return true if allocation succeeds, false otherwise
 */
extern int
malloc_on_cxl(size_t size, void **ptr);

/**
 * @brief Try to allocate and initialize memory to zero on CXL
 * @param num Number of elements
 * @param size Size of each element
 * @param ptr Address to store allocated memory pointer
 * @return true if allocation succeeds, false otherwise
 */
extern int
calloc_on_cxl(size_t num, size_t size, void **ptr);

/**
 * @brief Try to mmap CXL memory
 * @param addr Suggested mapping address
 * @param length Length of mapping
 * @param prot Memory protection flags
 * @param flags Mapping flags (should be MAP_SHARED)
 * @param fd File descriptor (not used)
 * @param offset File offset
 * @param ptr Address to store mapped memory pointer
 * @return true if mapping succeeds, false otherwise
 */
extern int
mmap_on_cxl(void *addr, size_t length, int prot, int flags, int fd, off_t offset, void **ptr);

/**
 * @brief Try to allocate aligned memory on CXL
 * @param memptr Address to store allocated memory pointer
 * @param alignment Alignment requirement
 * @param size Size to allocate
 * @return 0 on success, error code on failure
 */
extern int
posix_memalign_on_cxl(void **memptr, size_t alignment, size_t size);

#endif  // CXL_ALLOCATOR_H