/**
 *    author:     UncP
 *    date:    2019-02-16
 *    license:    BSD-3
**/

#ifndef _adapitve_radix_tree_hh_
#define _adaptive_radix_tree_hh_

// #ifdef __cplusplus
// extern "C" {
// #endif

#include "art_node.hh"
#include "sidle_meta.hh"

#include <cstddef>

namespace art {

art_node* _new_art_node(size_t size, art_node* parent, size_t old_size,
                          sidle::node_mem_type target_type, 
                          bool is_migration = false);
uint64_t art_node_get_version(art_node *an);
adaptive_radix_tree* new_adaptive_radix_tree(int cxl_percentage,
                                            uint64_t local_memory_amount = 110);
void free_adaptive_radix_tree(adaptive_radix_tree *art);
int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len);
void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len);
void adaptive_radix_tree_traverse(adaptive_radix_tree *art, art_callback cb); 
void adaptive_radix_tree_traverse_mt(adaptive_radix_tree *art, art_callback cb);
void init_thread_pool(int start_tid);
/// @return migrate success or not
art_node* leaf_migration(leaf_node* cur_node, art_node* parent, sidle::node_mem_type target_type);
/// @return return parent node if migrate successfully, else return nullptr
art_node* internode_migration(art_node* original_node, sidle::node_mem_type target_type, 
                        bool need_lock_first);
void unlock_node(art_node* an);
leaf_node* get_art_leaf(art_node* an);
bool is_art_leaf(art_node* an);
} // namespace art

#endif /* _adaptive_radix_tree_hh_ */
