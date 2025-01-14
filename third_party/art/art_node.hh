/**
 *    author:     UncP
 *    date:    2019-02-05
 *    license:    BSD-3
**/

#ifndef _art_node_hh_
#define _art_node_hh_

#include <cstddef>
#include <cstdint>

#ifdef Debug
#include <cassert>
#define debug_assert_art(v) assert(v)
#else
#define debug_assert_art(v)
#endif // Debug

#define fuck printf("fuck\n");

#define CXL

#ifdef CXL
extern "C" {
  #include "../cxl_utils/cxl_allocator.h"
}
#endif

// #ifdef __cplusplus
// extern "C" {
// #endif

#include "art_def.hh"
#include "art_node.hh"
#include "sidle_policy.hh"
#include "sidle_frontend.hh"

namespace art {

art_node* new_art_node(art_node* parent);
uintptr_t alloc_leaf(art_node* parent, const void* key, size_t len, 
                    sidle::node_mem_type target_type = 
                      sidle::node_mem_type::remote, 
                      bool is_migration = false);
void free_art_node(art_node *an);
art_node** art_node_add_child(art_node *an, unsigned char byte, art_node *child, art_node **new_);
art_node** art_node_find_child(art_node *an, uint64_t version, unsigned char byte);
int art_node_is_full(art_node *an);
void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len);
const char* art_node_get_prefix(art_node *an);
int art_node_prefix_compare(art_node *an, uint64_t version, const void *key, size_t len, size_t off);
unsigned char art_node_truncate_prefix(art_node *an, int off);
uint64_t art_node_get_version_unsafe(art_node *an);
uint64_t art_node_get_stable_expand_version(art_node *an);
// uint64_t art_node_get_stable_insert_version(art_node *an);
int art_node_version_get_prefix_len(uint64_t version);
int art_node_version_compare_expand(uint64_t version1, uint64_t version2);
// int art_node_version_compare_insert(uint64_t version1, uint64_t version2);
int art_node_lock(art_node *an);
art_node* art_node_get_locked_parent(art_node *an);
void art_node_set_parent_unsafe(art_node *an, art_node *parent);
void art_node_unlock(art_node *an);
int art_node_version_is_old(uint64_t version);
art_node* art_node_replace_leaf_child(art_node *parent, art_node *an, const void *key, size_t len, size_t off);
void art_node_replace_child(art_node *parent, unsigned char byte, art_node *old, art_node *new_);
art_node* art_node_expand_and_insert(art_node *parent, art_node *an, const void *key, size_t len, size_t off, int common);
size_t art_node_version_get_offset(uint64_t version);
void art_node_set_new_node(art_node *old, art_node *new_);
void art_node_set_version(art_node *an, uint64_t version);
void art_node_traverse(art_node *cur_node, node_callback cb);
#ifdef Debug
void art_node_print(art_node *an);
void print_key(const void *key, size_t len);
#endif
} // namespace art


// #ifdef __cplusplus
// }
// #endif

#endif /* _art_node_hh_ */
