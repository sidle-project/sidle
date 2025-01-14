#include <fcntl.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <threads.h>
#include <unistd.h>

#include <memkind.h>

#include "cxl_allocator.h"

#define thread_local _Thread_local

static struct memkind *cxl_kind = NULL;
static size_t cxl_current_size;
static const char *path = "/dev/dax0.0";
static int percentage_on_cxl;
static char *mmap_start_point = NULL;
static char *mmap_end_point = NULL;
static int cxl_fd = 0;
static atomic_size_t cxl_offset = CXL_MAX_SIZE;
static size_t CXL_MIN_SIZE = 2 * 1024 * 1024;
static int local_stride;
static int cxl_stride;
static thread_local size_t local_ticket;
static thread_local size_t cxl_ticket;

enum DEVICE_TYPE {
    CXL_DEV, 
    LOCAL_DEV,
    UNKNOWN_DEV
};

inline static void print_err_message(int err)
{
    char error_message[MEMKIND_ERROR_MESSAGE_SIZE];
    memkind_error_message(err, error_message, MEMKIND_ERROR_MESSAGE_SIZE);
    fprintf(stderr, "%s\n", error_message);
}

int gcd(int a, int b) {
    while (b != 0) {
        int temp = b;
        b = a % b;
        a = temp;
    }
    return a;
}

void exit_handler(int singal_num) {
    if (cxl_kind != NULL) {
        int err = memkind_destroy_kind(cxl_kind);
        if (err) {
            print_err_message(err);
        }   
    }

    // tear down mmap
    if (munmap(mmap_start_point, cxl_current_size) == -1) {
        fprintf(stderr, "tear down mmap fail\n");
        exit(EXIT_FAILURE);
    }
    close(cxl_fd);
}

// inline enum DEVICE_TYPE
enum DEVICE_TYPE stride_scheduler() {   
    enum DEVICE_TYPE result = UNKNOWN_DEV;
    if (cxl_stride == 0) {
        result = CXL_DEV;
    } else if (local_stride == 0) {
        result = LOCAL_DEV;
    } else {
        result = cxl_ticket < local_ticket ? CXL_DEV : LOCAL_DEV;
    }

    // update ticket
    switch (result)
    {
    case CXL_DEV:
        // check in case overlflow
        if (SIZE_MAX - cxl_ticket < (size_t)cxl_stride) {
            local_ticket = 0;
            cxl_ticket = 0;
        }
        cxl_ticket += cxl_stride;
        break;
    case LOCAL_DEV:
        // check in case overlflow
        if (SIZE_MAX - local_ticket < (size_t)local_stride) {
            local_ticket = 0;
            cxl_ticket = 0;
        }
        local_ticket += local_stride;
        break;
    default:
        break;
    }
    return result;
}

void 
cxl_init(const size_t wanted_size, const int percentage) {   
    // if the cxl device is already initialized, return   
    if (cxl_fd != 0) {
        return;
    }

    // open the cxl file
    cxl_fd = open(path, O_RDWR);
    if (cxl_fd < 0) {
        fprintf(stderr, "the path for cxl /dev/dax0.0 not exist\n");
        exit(EXIT_FAILURE);
    }

    // create the mmap area
    cxl_current_size = wanted_size > CXL_MAX_SIZE ? CXL_MAX_SIZE : wanted_size;
    cxl_current_size = cxl_current_size < CXL_MIN_SIZE ? CXL_MIN_SIZE : cxl_current_size;
    void *mapped_memory = mmap(NULL, cxl_current_size, PROT_READ | PROT_WRITE, MAP_SHARED, cxl_fd, 0);
    if (mapped_memory == MAP_FAILED) {
        perror("CXL init error");
        exit(EXIT_FAILURE);
    }
    mmap_start_point = (char *)mapped_memory;
    mmap_end_point = mmap_start_point + cxl_current_size;
    printf("[DEBUG] mmap start point: %p, mmap end point: %p\n", mmap_start_point, mmap_end_point);

    // create the cxl partition with specific size
    int err = memkind_create_fixed(mapped_memory, cxl_current_size, &cxl_kind);
    if (err) {
        print_err_message(err);
        exit(EXIT_FAILURE);
    }

    if (percentage >= 100) {
        percentage_on_cxl = 100;
    } else if (percentage > 0) {
        percentage_on_cxl = percentage;
    }

    // init the strides
    if (percentage_on_cxl == 100) {
        local_stride = 1;
        cxl_stride = 0;
    } else if (percentage_on_cxl == 0) {
        local_stride = 0;
        cxl_stride = 1;
    } else {
        int multiple_result = percentage_on_cxl * (100 - percentage_on_cxl) / gcd(percentage_on_cxl, 100 - percentage_on_cxl);
        cxl_stride = multiple_result /  percentage_on_cxl;
        local_stride = multiple_result / (100 -  percentage_on_cxl);
    }

    // register the exit handler
    struct sigaction sa;
    sa.sa_handler = exit_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGSEGV, &sa, NULL);
    sigaction(SIGABRT, &sa, NULL);
    sigaction(SIGKILL, &sa, NULL);
}

void* malloc_with_cxl(size_t size) {
    void *result = NULL;
    enum DEVICE_TYPE dev_type = UNKNOWN_DEV;
    if (cxl_kind != NULL) {
        dev_type = stride_scheduler();
        if (dev_type == CXL_DEV) {
            result = memkind_malloc(cxl_kind, size);
            if (result != NULL) {
                return result;
            }
        }
    }
    result = malloc(size);
    // if malloc fail, try to re-malloc in cxl
    if (result == NULL && dev_type != CXL_DEV) {
        result = memkind_malloc(cxl_kind, size);
    }
    return result;
}

void* calloc_with_cxl(size_t num, size_t size) {   
    void *result = NULL;
    enum DEVICE_TYPE dev_type = UNKNOWN_DEV;
    if (cxl_kind != NULL) {
        dev_type = stride_scheduler();
        if (dev_type == CXL_DEV) {
            result = memkind_calloc(cxl_kind, num, size);
            if (result != NULL) {
                return result;
            }
        }
    }
    result = calloc(num, size);
    // if malloc fail, try to re-malloc in cxl
    if (result == NULL && dev_type != CXL_DEV) {
        result = memkind_calloc(cxl_kind, num, size);
    }
    return result;
}

void* realloc_with_cxl(void *ptr, size_t new_size) {
    void *result = NULL;
    if ((char *)ptr >= mmap_start_point && (char *)ptr < mmap_end_point) { 
        result = memkind_realloc(cxl_kind, ptr, new_size);
    } else {
        result = realloc(ptr, new_size);
    }
    // if realloc on the same kind fabric fail, return null ptr
    if (!result) {
        fprintf(stderr, "[realloc_with_cxl] realloc fail\n");
    }
    return result;
}

void free_with_cxl(void *ptr) {   
    // allocate in mmap area (cxl)
    if ((char *)ptr >= mmap_start_point && (char *)ptr < mmap_end_point) {
        memkind_free(cxl_kind, ptr);
    } else {
        // allocate in heap
        free(ptr);
    }
}

// only mmap the cxl part, if shouldn't mmap on cxl, return mmap fail
void* mmap_with_cxl(void *addr, size_t length, int prot, int flags, int fd, off_t offset) {
    void *result = NULL;
    if (cxl_kind != NULL && stride_scheduler() == CXL_DEV) {
        // should mmap CXL memory
        result = mmap(addr, length, prot, flags | MAP_SHARED, cxl_fd, cxl_offset);
        atomic_fetch_add(&cxl_offset, length);
        if (result == MAP_FAILED) {
            perror("mmap on cxl fail");
        }
        if (result != MAP_FAILED) {
            return result;
        }
    }
    return MAP_FAILED;

}

void munmap_with_cxl(void * const ptr, const size_t size) 
{   
    munmap(ptr, size);
}

int posix_memalign_with_cxl(void **memptr, size_t alignment, size_t size) {
    enum DEVICE_TYPE dev_type = UNKNOWN_DEV;
    int result = -1;
    if (cxl_kind != NULL) {
        dev_type = stride_scheduler();
        if (dev_type == CXL_DEV) {
            // memalign on CXL memory
            result = memkind_posix_memalign(cxl_kind, memptr, alignment, size);
            if (result == 0) {
                return result;
            }
        }
    } 

    // memalign on local memory
    result = posix_memalign(memptr, alignment, size);
    if (result != 0 && dev_type != CXL_DEV) {
        result = memkind_posix_memalign(cxl_kind, memptr, alignment, size);
    }
    return result;
}

int malloc_on_cxl(size_t size, void **ptr) {
    *ptr = memkind_malloc(cxl_kind, size);
    if (*ptr != NULL) {
        return 1;
    }
    *ptr = malloc(size);
    return 0;
}

int calloc_on_cxl(size_t num, size_t size, void **ptr) {
    *ptr = memkind_calloc(cxl_kind, num, size);
    if (*ptr != NULL) {
        return 1;
    }
    *ptr = calloc(num, size);
    return 0;
}

int mmap_on_cxl(void *addr, size_t length, int prot, int flags, int fd, off_t offset, void **ptr) {
    length = (length + CXL_MIN_SIZE - 1) / CXL_MIN_SIZE * CXL_MIN_SIZE;
    *ptr = mmap(addr, length, prot, flags | MAP_SHARED, cxl_fd, cxl_offset);
    atomic_fetch_add(&cxl_offset, length);
    if (*ptr == MAP_FAILED) {
        perror("mmap on cxl fail");
        *ptr = mmap(addr, length, prot, flags, fd, offset);
        return 0;
    }
    return 1;
}

int posix_memalign_on_cxl(void **memptr, size_t alignment, size_t size) {
    int result = memkind_posix_memalign(cxl_kind, memptr, alignment, size);
    // printf("[DEBUG] posix_memalign_on_cxl, result: %p\n", *memptr);
    if (result != 0 && size != 0) {
        // perror("posix_memalign on cxl fail");
        result = posix_memalign(memptr, alignment, size);
    }
    return result;
}

void cxl_destroy() 
{
    exit_handler(0);
}

