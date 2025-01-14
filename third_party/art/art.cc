/**
 *    author:     UncP
 *    date:    2019-02-16
 *    license:    BSD-3
**/

#include <cstdlib>
#include <cstdint>
#include <cassert>
#ifdef Debug
#include <cstring>
#include <cstdio>
#endif

#include "art.hh"
#include "thread_pool.h"
#include "sidle_meta.hh"

// #define RECORD_ART_LEVEL
namespace art {

/* ------------------------ func from art_node.cc ------------------------------*/

static ThreadPool *pool;

uint64_t art_node_get_version(art_node *an)
{
  uint64_t version;
  __atomic_load(&an->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

inline uint64_t art_node_get_version_unsafe(art_node *an)
{
  return an->version;
}

inline size_t art_node_version_get_offset(uint64_t version)
{
  return get_offset(version);
}

inline int art_node_version_compare_expand(uint64_t version1, uint64_t version2)
{
  return (is_expanding(version1) != is_expanding(version2)) || (get_vexpand(version1) != get_vexpand(version2));
}

inline int art_node_version_get_prefix_len(uint64_t version)
{
  return get_prefix_len(version);
}

inline void art_node_set_parent_unsafe(art_node *an, art_node *parent)
{
  an->parent = parent;
}

/* ------------------------ end func from art_node.cc --------------------------*/

adaptive_radix_tree* new_adaptive_radix_tree(int cxl_percentage,
                                            uint64_t local_memory_amount)
{
#ifdef Allocator
  init_allocator();
#endif

#ifdef CXL
  // init the sidle strategy manager
  cxl_init(CXL_MAX_SIZE, cxl_percentage);
  sidle::strategy_manager = sidle::sidle_strategy(local_memory_amount, cxl_percentage);
#endif
  adaptive_radix_tree *art = static_cast<adaptive_radix_tree *>(malloc(sizeof(adaptive_radix_tree)));
  art->root = 0;

  return art;
}

void free_adaptive_radix_tree(adaptive_radix_tree *art)
{
  (void)art;
}

// return  0 on success,
// return +1 on existed,
// return -1 on retry
static int adaptive_radix_tree_replace_leaf(art_node *parent, art_node **ptr, art_node *an,
  const void *key, size_t len, size_t off)
{
  art_node *new_ = art_node_replace_leaf_child(parent, an, key, len, off);
  if (likely(new_)) {
    if (likely(parent)) {
      if (unlikely(art_node_lock(parent))) {
        // parent is old
        free_art_node(new_);
        return -1;
      } else {
        art_node *now;
        __atomic_load(ptr, &now, __ATOMIC_ACQUIRE);
        if (unlikely(now != an)) {
          // leaf has been replaced by another thread
          art_node_unlock(parent);
          free_art_node(new_);
          return -1;
        }
        __atomic_store(ptr, &new_, __ATOMIC_RELEASE);
        // update 
        art_node_unlock(parent);
        return 0;
      }
    } else { // art with only one leaf
      if (likely(__atomic_compare_exchange_n(ptr, &an, new_, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE))) {
        return 0;
      } else {
        free_art_node(new_);
        return -1;
      }
    }
  } else {
    return 1;
  }
}

// return  0 on success,
// return +1 on existed,
// return -1 for retry
static int _adaptive_radix_tree_put(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off)
{
  art_node *an;
  int first = 1;

  begin:
  // this is fucking ugly!
  if (first)  {
    first = 0;
  } else if (parent) {
    // this is not the first time we enter `begin`, so
    // we need to make sure that `ptr` is still valid because `parent` might changed
    uint64_t pv = art_node_get_version(parent);
    if (art_node_version_is_old(pv))
      return -1; // return -1 so that we can retry from root
    // `ptr` is still valid, we can proceed
  }

  // NOTE: __ATOMIC_RELAXED is not ok
  __atomic_load(ptr, &an, __ATOMIC_ACQUIRE);

  if (unlikely(is_leaf(an)))
    return adaptive_radix_tree_replace_leaf(parent, ptr, an, key, len, off);

#ifdef CAL_NODE_HOTNESS
  ++an->access_count;
#endif

  // verify node prefix
  uint64_t v = art_node_get_stable_expand_version(an);
  if (unlikely(art_node_version_get_offset(v) != off))
    goto begin;

  if (unlikely(art_node_version_is_old(v)))
    goto begin;

  int p = art_node_prefix_compare(an, v, key, len, off);

  uint64_t v1 = art_node_get_version(an);
  if (unlikely(art_node_version_is_old(v1) || art_node_version_compare_expand(v, v1)))
    goto begin;
  v = v1;

  if (p != art_node_version_get_prefix_len(v)) {
    if (unlikely(art_node_lock(an)))
      goto begin;
    // still need to check whether prefix has been changed!
    if (unlikely(art_node_version_compare_expand(v, art_node_get_version_unsafe(an)))) {
      art_node_unlock(an);
      goto begin;
    }
    debug_assert_art(art_node_version_is_old(art_node_get_version_unsafe(an)) == 0);
    parent = art_node_get_locked_parent(an);
    art_node *new_ = art_node_expand_and_insert(parent, an, key, len, off, p);
    art_node_set_parent_unsafe(an, new_);
    if (likely(parent)) {
      debug_assert_art(off);
      art_node_replace_child(parent, ((unsigned char *)key)[off - 1], an, new_);
      art_node_unlock(parent);
    } else { // this is root
      __atomic_store(ptr, &new_, __ATOMIC_RELEASE);
    }
    art_node_unlock(an);
    return 0;
  }

  off += p;
  debug_assert_art(off < len);

  // prefix is matched, we can descend
  art_node **next = art_node_find_child(an, v, ((unsigned char *)key)[off]);

  v = art_node_get_version(an);

  if (unlikely(art_node_version_is_old(v))) {
    off -= p;
    goto begin;
  }

  if (next)
    return _adaptive_radix_tree_put(an, next, key, len, off + 1);

  if (unlikely(art_node_lock(an))) {
    off -= p;
    goto begin;
  }

  art_node *new_ = 0;
  next = art_node_add_child(an, ((unsigned char *)key)[off], 
                            (art_node *)alloc_leaf(an, key, len), &new_);
  if (unlikely(new_)) {
    parent = art_node_get_locked_parent(an);
    if (likely(parent)) {
      debug_assert_art((int)off > p);
      art_node_replace_child(parent, ((unsigned char *)key)[off - p - 1], an, new_);
      art_node_unlock(parent);
    } else {
      __atomic_store(ptr, &new_, __ATOMIC_RELEASE);
    }
  }
  art_node_unlock(an);

  // another thread might inserted same byte before we acquire lock
  if (unlikely(next))
    return _adaptive_radix_tree_put(an, next, key, len, off + 1);

  return 0;
}

// return 0 on success
// return 1 on duplication
int adaptive_radix_tree_put(adaptive_radix_tree *art, const void *key, size_t len)
{
  //print_key(key, len);

  art_node *root = art->root;
  if (unlikely(root == 0)) { // empty art
    art_node *leaf = (art_node *)alloc_leaf(nullptr, key, len);
    // art_node *leaf = (art_node *)make_leaf(key);
    if (__atomic_compare_exchange_n(&art->root, &root, leaf, 0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE))
      return 0;
    // else another thread has replaced empty root
  }
  int ret;
  // retry should be rare
  while (unlikely((ret = _adaptive_radix_tree_put(0 /* parent */, &art->root, key, len, 0 /* off */)) == -1))
    ;
  return ret;
}

#ifdef RECORD_ART_LEVEL
static void* _adaptive_radix_tree_get(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off, uint64_t level = 0)
#else
static void* _adaptive_radix_tree_get(art_node *parent, art_node **ptr, const void *key, size_t len, size_t off)
#endif
{
  art_node *an;

  debug_assert_art(off <= len);

  int first = 1; // is this the first time we enter `begin`
  begin:
  // this is fucking ugly!
  if (first)  {
    first = 0;
  } else if (parent) {
    // this is not the first time we enter `begin`, so
    // we need to make sure that `ptr` is still valid because `parent` might changed
    uint64_t pv = art_node_get_version(parent);
    if (art_node_version_is_old(pv))
      return (void *)1; // return 1 so that we can retry from root
    // `ptr` is still valid, we can proceed
  }
  __atomic_load(ptr, &an, __ATOMIC_ACQUIRE);

  if (unlikely(is_leaf(an))) {
#ifdef RECORD_ART_LEVEL
    if (level != 0) {
      printf("[art_get] level: %lu\n", level);
    }
#endif
    const char *k1 = get_leaf_key(an), *k2 = (const char *)key;
    size_t l1 = get_leaf_len(an), l2 = len, i;
    for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
      ;
    if (i == l1 && i == l2) {
      sidle::update_access<leaf_node>(get_leaf(reinterpret_cast<char*>(an)));
      return (void *)k1; // key exists
    }
    return 0;
  }
#ifdef CAL_NODE_HOTNES
  else {
    ++an->access_count;
  }
#endif

  uint64_t v = art_node_get_stable_expand_version(an);
  if (unlikely(art_node_version_get_offset(v) != off))
    goto begin;
  if (unlikely(art_node_version_is_old(v)))
    goto begin;

  int p = art_node_prefix_compare(an, v, key, len, off);

  uint64_t v1 = art_node_get_version(an);
  if (unlikely(art_node_version_is_old(v1) || art_node_version_compare_expand(v, v1)))
    goto begin;
  v = v1;

  if (p != art_node_version_get_prefix_len(v))
    return 0;

  off += art_node_version_get_prefix_len(v);
  debug_assert_art(off <= len);

  int advance = off != len;
  unsigned char byte = advance ? ((unsigned char *)key)[off] : 0;

  art_node **next = art_node_find_child(an, v, byte);

  v1 = art_node_get_version(an);

  if (unlikely(art_node_version_is_old(v1)))
    goto begin;

  if (next) {
#ifdef RECORD_ART_LEVEL
    return _adaptive_radix_tree_get(an, next, key, len, off + advance, level + 1);
#else
    return _adaptive_radix_tree_get(an, next, key, len, off + advance);
#endif
  }

  // art_node_print(an);
  // printf("off:%lu\n", off);
  return 0;
}

void* adaptive_radix_tree_get(adaptive_radix_tree *art, const void *key, size_t len)
{
  void *ret;
  if (unlikely(art->root == 0))
    return 0;
  while (unlikely((uint64_t)(ret = _adaptive_radix_tree_get(0, &art->root, key, len, 0)) == 1))
    ;
  return ret;
}

bool _adaptive_radix_tree_traverse(art_node *parent, art_node *node, art_callback cb, 
                                   bool full = false) {
  // check whether the node is up-to-date
  auto is_valid_node = [](art_node *node) {
    uint64_t pv = art_node_get_version(node);
    if (art_node_version_is_old(pv)) {
      return false;
    }
    return true;
  };

  if (is_leaf(node)) {
    cb(parent, get_leaf(reinterpret_cast<char*>(node)));
    return true;
  }
  if (full) {
    cb(node, nullptr);
  }

  if (!is_valid_node(node)) {
    return false;
  }
  int child_count = 0;
  int i = 0;
  uint64_t v = art_node_get_stable_expand_version(node);
  switch (get_node_type(v)) {
  case node4: {
    child_count = get_count(node->version);
    art_node4 *n4 = reinterpret_cast<art_node4*>(node);
    for (i = 0; i < child_count && is_valid_node(node); ++i) {
      while (!_adaptive_radix_tree_traverse(node, n4->child[i], cb)) {
        if (!is_valid_node(node)) {
          return false;
        }
      }
    }
    if (i != child_count) {
      return false;
    }
    break;
  }
  case node16: {
    child_count = get_count(node->version);
    art_node16 *n16 = reinterpret_cast<art_node16*>(node);
    for (i = 0; i < child_count && is_valid_node(node); ++i) {
      while (!_adaptive_radix_tree_traverse(node, 
                        reinterpret_cast<art_node16*>(node)->child[i], cb)) {
        if (!is_valid_node(node)) {
          return false;
        }
      }
    }
    if (i != child_count) {
      return false;
    }
    break;
  }
  case node48: {
    art_node48 *n48 = reinterpret_cast<art_node48*>(node);
    for (i = 0; i < 256 && is_valid_node(node); ++i) {
      char index = n48->index[i];
      if (index) {
        while (!_adaptive_radix_tree_traverse(node, n48->child[index - 1], cb)) {
          if (!is_valid_node(node)) {
            return false;
          }
        }
      }
    }
    if (i != 256) {
      return false;
    }
    break;
  }
  case node256: {
    art_node256 *n256 = reinterpret_cast<art_node256*>(node);
    for (i = 0; i < 256 && is_valid_node(node); ++i) {
      if (n256->child[i]) {
        while (!_adaptive_radix_tree_traverse(node, n256->child[i], cb)) {
          if (!is_valid_node(node)) {
            return false;
          }
        }
      }
    }
    if (i != 256) {
      return false;
    }
    break;
  }
  default: 
    throw std::runtime_error("encounter invalid node type when traversing");
  }

  return true;
}

void adaptive_radix_tree_traverse(adaptive_radix_tree *art, art_callback cb) {
  if (unlikely(art->root == 0)) {
    return;
  }
  /// @note it is @b best-effort traverse, if cannot traverse the whole tree 
  // because of inconsistency, not retry
  _adaptive_radix_tree_traverse(nullptr, art->root, cb);
}

// traverse the tree in multi-thread
bool _adaptive_radix_tree_traverse_mt(art_node *parent, art_node *node, art_callback cb) {
  if (is_leaf(node)) {
    cb(parent, get_leaf(reinterpret_cast<char*>(node)));
    return true;
  }
  uint64_t v = art_node_get_stable_expand_version(node);
  if (art_node_version_is_old(v)) {
    return false;
  }
  std::vector<std::future<bool>> results;
  int child_count = 0;
  switch (get_node_type(v))
  {
  case node4: {
    child_count = get_count(node->version);
    art_node4 *n4 = reinterpret_cast<art_node4*>(node);
    for (int i = 0; i < child_count; ++i) {
      results.emplace_back(pool->enqueue([=] {
        return _adaptive_radix_tree_traverse(node, n4->child[i], cb);
      }));
    }
    break;
  }
  case node16: {
    child_count = get_count(node->version);
    art_node16 *n16 = reinterpret_cast<art_node16*>(node);
    for (int i = 0; i < child_count; ++i) {
      results.emplace_back(pool->enqueue([=] {
        return _adaptive_radix_tree_traverse(node, n16->child[i], cb);
      }));
    }
    break;
  }
  case node48: {
    art_node48 *n48 = reinterpret_cast<art_node48*>(node);
    for (int i = 0; i < 256; ++i) {
      char index = n48->index[i];
      if (index) {
        results.emplace_back(pool->enqueue([=] {
          return _adaptive_radix_tree_traverse(node, n48->child[index - 1], cb);
        }));
      }
    }
    break;
  }
  case node256: {
    art_node256 *n256 = reinterpret_cast<art_node256*>(node);
    for (int i = 0; i < 256; ++i) {
      if (n256->child[i]) {
        results.emplace_back(pool->enqueue([=] {
          return _adaptive_radix_tree_traverse(node, n256->child[i], cb);
        }));
      }
    }
    break;
  }
  default:
    break;
  }
  bool final_result = true;
  for (auto &&result : results) {
    final_result &= result.get();
  }
  return final_result;
}

void adaptive_radix_tree_traverse_mt(adaptive_radix_tree *art, art_callback cb) {
  if (unlikely(art->root == 0)) {
    return;
  }
  /// @note it is @b best-effort traverse, if cannot traverse the whole tree 
  // because of inconsistency, not retry
  _adaptive_radix_tree_traverse_mt(nullptr, art->root, cb);
}

void init_thread_pool(int start_tid) {
  pool = new ThreadPool(4, start_tid);
}

/// @pre the @param parent node has already been locked
void replace_old_child_ptr(art_node* parent, const art_node* old_child, 
                          art_node* new_child) {
  SIDLE_CHECK(parent != nullptr, "[DEBUG] parent node shouldn't be empty");
  int child_count = get_count(parent->version);
  switch (get_node_type(parent->version)) {
  case node4: {
    art_node4* p4 = reinterpret_cast<art_node4*>(parent);
    for (int i = 0; i < child_count; ++i) {
      if (p4->child[i] == old_child) {
        p4->child[i] = new_child;
        break;
      }
    }
    break;
  }
  case node16: {
    art_node16* p16 = reinterpret_cast<art_node16*>(parent);
    for (int i = 0; i < child_count; ++i) {
      if (p16->child[i] == old_child) {
        p16->child[i] = new_child;
        break;
      }
    }
    break;
  }
  case node48: {
    art_node48* p48 = reinterpret_cast<art_node48*>(parent);
    for (int i = 0; i < 256; ++i) {
      char index = p48->index[i];
      if (index && p48->child[index - 1] == old_child) {
        p48->child[index - 1] = new_child;
        break;
      }
    }
    break;
  }
  case node256: {
    art_node256* p256 = reinterpret_cast<art_node256*>(parent);
    for (int i = 0; i < 256; ++i) {
      if (p256->child[i] == old_child) {
        p256->child[i] = new_child;
        break;
      }
    }
    break;
  }
  }
}

/// @brief get the child pointers of the current node
inline art_node** get_children(art_node* cur_node) {
  SIDLE_CHECK(cur_node != nullptr,
      "[DEBUG] [get_children] cur_node shouldn't be empty");  
  art_node** child = nullptr;
  switch (get_node_type(cur_node->version)) {
  case node4: {
    child = reinterpret_cast<art_node4*>(cur_node)->child;
    break;
  }
  case node16: {
    child = reinterpret_cast<art_node16*>(cur_node)->child;
    break;
  }
  case node48: {
    child = reinterpret_cast<art_node48*>(cur_node)->child;
    break;
  }
  case node256: {
    child = reinterpret_cast<art_node256*>(cur_node)->child;
    break;
  }
  default: 
    throw std::runtime_error("[DEBUG] [get_children] invalid node type");
  }
  return child;
}

/// @brief let the parent field of the child of the current node point to 
// the current node
/// @pre cur_node should have been locked 
void replace_old_parent_ptr(art_node* cur_node) {
  SIDLE_CHECK(cur_node != nullptr, 
      "[DEBUG] [replace_old_parent_ptr] cur_node shouldn't be empty");
  art_node_traverse(cur_node, [cur_node](art_node* child) {
    if (is_leaf(child)) {
      return true;
    }
    child->parent = cur_node;
    return true;
  });
}


art_node* leaf_migration(leaf_node* cur_node, art_node* parent, sidle::node_mem_type target_type) {
  if (unlikely(!cur_node)) {
    return nullptr;
  }

  // check whether current node has already been migrated
  if (unlikely(cur_node->sidle_meta.metadata.type == target_type)) {
    return nullptr;
  }
  
  if (!parent) {
    throw std::runtime_error("the root node should not be migrated");
  }
  // use the parent node's version number for concurrency control
  uint64_t pv = art::art_node_get_version(parent);
  if (unlikely(art::art_node_version_is_old(pv) || is_locked(pv))) {
    return nullptr;
  }

  // lock the parent node
  if (unlikely(art::art_node_lock(parent))) {
    return nullptr;
  }

  // SIDLE_RECORD(WORKER_DEBUG, "[migrate_leaf] migrate node %p to %s, parent node: %p, level: %d, parent level: %d\n", 
  //   cur_node, target_type == node_mem_type::local ? "local" : "remote", 
  //   parent, cur_node->sidle_meta.metadata.depth, parent->sidle_meta.depth);

  uintptr_t new_node_ptr = art::alloc_leaf(parent, cur_node->key,
                                          cur_node->key_length, target_type, 
                                          true);
  leaf_node* new_node = get_leaf(reinterpret_cast<char*>(new_node_ptr));
  new_node->sidle_meta = cur_node->sidle_meta;
  new_node->sidle_meta.metadata.type = target_type;
  if (!parent) {
    throw std::runtime_error("the root node should not be migrated");
  }
  // replace the old leaf node pointer with the new one
  replace_old_child_ptr(parent, reinterpret_cast<art_node*>(make_leaf(cur_node)),
                        reinterpret_cast<art_node*>(new_node_ptr));

  return parent;
}

art_node* internode_migration(art_node* original_node, sidle::node_mem_type target_type, bool need_lock_first) {
  using namespace sidle;
  if (need_lock_first) {
    // lock the node
    uint64_t v = art::art_node_get_version(original_node);
    if (unlikely(art::art_node_version_is_old(v) || is_locked(v))) {
      return nullptr;
    }
    if (unlikely(art::art_node_lock(original_node))) {
      return nullptr;
    }
  }

  uint64_t v = art::art_node_get_version(original_node);
  // check the node status to ensure concurrency safety
  SIDLE_CHECK(!is_locked(v), 
          "[DEBUG] [migrate_internode] the original node should be locked");
  SIDLE_CHECK(original_node->sidle_meta.depth > 1,
      "[DEBUG] [migrate_internode] the original node shouldn't be the root");
  if (unlikely(art::art_node_version_is_old(v))) {
    art::art_node_unlock(original_node);
    return nullptr;
  }

  if (unlikely(!IS_VALID_ADDR(reinterpret_cast<uint64_t>(original_node->parent)))) {
    fprintf(stderr, "node %p's parent addr is invalid %p\n", original_node,
            original_node->parent);
    art::art_node_unlock(original_node);
    return nullptr;
  }
  if (unlikely(original_node->sidle_meta.depth == 1)) {
    art::art_node_unlock(original_node);
    return nullptr;
  }
  size_t new_node_size = 0;
  switch (get_node_type(v)) {
  case node4:
    new_node_size = sizeof(art_node4);
    break;
  case node16:
    new_node_size = sizeof(art_node16);
    break;
  case node48:
    new_node_size = sizeof(art_node48);
    break;
  case node256:
    new_node_size = sizeof(art_node256);
    break;
  default:
    throw std::runtime_error("[migrate_internode] invalid node type");
  }
  art_node* new_node = art::_new_art_node(new_node_size, nullptr, 0, target_type, true);
  memcpy(new_node, original_node, new_node_size);
  new_node->sidle_meta.type = target_type;
  art_node* parent = art::art_node_get_locked_parent(original_node);
  SIDLE_CHECK(parent != nullptr,
          "[migrate_internode] the parent node shouldn't be empty");
  
  // replace the old child pointer with the new one
  replace_old_child_ptr(parent, original_node, new_node);
  
  // change the original node's chidren's parent pointer
  replace_old_parent_ptr(new_node);

  // update the new node information and mark the original node as deleted
  art::art_node_set_new_node(original_node, new_node);
  art::art_node_set_version(original_node, set_old(v));
  art::art_node_unlock(original_node);
  art::art_node_unlock(new_node);
  return parent;
}

void unlock_node(art_node* an) {
  if (an && is_locked(art::art_node_get_version(an))) {
    art::art_node_unlock(an);
  }
}

leaf_node* get_art_leaf(art_node* an) {
  return get_leaf(reinterpret_cast<char*>(an));
}

bool is_art_leaf(art_node* an) {
  return is_leaf(an);
}


} // namespace art

