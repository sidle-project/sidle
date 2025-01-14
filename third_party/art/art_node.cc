/**
 *    author:     UncP
 *    date:    2019-02-06
 *    license:    BSD-3
**/

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cassert>
#include <emmintrin.h>

#ifdef Debug
#include <cstdio>
#endif

#ifdef Allocator
#include "../palm/allocator.h"
#endif

#include "art_node.hh"
#include "sidle_meta.hh"

// #define CAL_TOTAL_MEM_USAGE

#ifdef CAL_TOTAL_MEM_USAGE
uint64_t total_memory_usage = 0;
#endif

// #define RECORD_NODE_GEN

namespace art {

#ifdef CAL_TOTAL_MEM_USAGE
inline void update_memory_usage(int64_t memory_usage) {
  static uint64_t node_count = 0;
  if (memory_usage < 0) {
    total_memory_usage -= static_cast<uint64_t>(-memory_usage);
  } else {
    total_memory_usage += static_cast<uint64_t>(memory_usage);
  }
  if (node_count % 100 == 0) {
    printf("[DEBUG] total_memory_usage: %ld\n", total_memory_usage);
  }
  ++node_count;
}
#endif

inline uint64_t art_node_get_version(art_node *an)
{
  uint64_t version;
  __atomic_load(&an->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

void art_node_set_version(art_node *an, uint64_t version)
{
  __atomic_store(&an->version, &version, __ATOMIC_RELEASE);
}

inline uint64_t art_node_get_version_unsafe(art_node *an)
{
  return an->version;
}

inline void art_node_set_version_unsafe(art_node *an, uint64_t version)
{
  an->version = version;
}

static inline void art_node_set_offset(art_node *an, size_t off)
{
  debug_assert_art(off < 256);
  an->version = set_offset(an->version, off);
}

static inline art_node* art_node_get_parent(art_node *an)
{
  art_node *parent;
  __atomic_load(&an->parent, &parent, __ATOMIC_ACQUIRE);
  return parent;
}


/// @param old_size is used for node expansion 
art_node* _new_art_node(size_t size, art_node* parent, size_t old_size,
                  sidle::node_mem_type target_type, bool is_migration)
{
  #ifdef Allocator
  art_node *an = (art_node *)allocator_alloc(size);
  #else  
  art_node *an = sidle::sidle_alloc<art_node, art_node>(size, parent, target_type, 
                                          is_migration, false, old_size);
  #endif
#ifdef RECORD_NODE_GEN
  // static uint64_t record_cnt = 0;
  // if (record_cnt % 100 == 0) {
    printf("[DEBUG] parent depth: %d, parent: %p, estimate depth: %u, new node: %p\n", 
      parent == nullptr ? -1 : parent->sidle_meta.depth, parent, cur_depth, an);
  // }
  // record_cnt++;
#endif
  an->version = 0;
  an->new_ = 0;
  an->parent = 0;
#ifdef CAL_TOTAL_MEM_USAGE
  update_memory_usage(size - old_size);
#endif
  return an;
}

static inline art_node* new_art_node4(art_node* parent, size_t old_size = 0)
{
  art_node *an = _new_art_node(sizeof(art_node4), parent, old_size,     
                                sidle::node_mem_type::unknown, false);
  an->version = set_type(an->version, node4);
  return an;
}

static inline art_node* new_art_node16(art_node* parent, size_t old_size = 0)
{
  art_node *an = _new_art_node(sizeof(art_node16), parent, old_size, 
                                sidle::node_mem_type::unknown, false);           
  an->version = set_type(an->version, node16);
  return an;
}

static inline art_node* new_art_node48(art_node* parent, size_t old_size = 0)
{
  art_node *an = _new_art_node(sizeof(art_node48), parent, old_size, 
                                sidle::node_mem_type::unknown, false);         
  an->version = set_type(an->version, node48);
  memset(((art_node48 *)an)->index, 0, 256);
  return an;
}

static inline art_node* new_art_node256(art_node* parent, size_t old_size = 0)
{
  art_node *an = _new_art_node(sizeof(art_node256), parent, old_size, 
                                sidle::node_mem_type::unknown, false); 
  memset(((art_node256 *)an)->child, 0, 256 * sizeof(art_node *));
  an->version = set_type(an->version, node256);
  return an;
}

art_node* new_art_node(art_node* parent)
{
  return new_art_node4(parent);
}

uintptr_t alloc_leaf(art_node* parent, const void* key, size_t len,
                    sidle::node_mem_type target_type, bool is_migration) 
{
  size_t leaf_size = sizeof(leaf_node) + len;
  leaf_node* node = sidle::sidle_alloc<leaf_node, art_node>(leaf_size, parent, 
                                      target_type, is_migration, true);
  node->key_length = static_cast<uint8_t>(len);
  memcpy(node->key, key, len);
#ifdef CAL_TOTAL_MEM_USAGE
  update_memory_usage(leaf_size);
#endif
  return make_leaf(reinterpret_cast<char*>(node) + 1);
}

void free_art_node(art_node *an)
{
  #ifdef Allocator
  (void)an;
  #else
#ifdef CXL
  int64_t memory_usage = 0;
  switch (get_node_type(an->version)) {
  case node4:
    memory_usage = sizeof(art_node4);
    break;
  case node16:
    memory_usage = sizeof(art_node16);
    break;
  case node48:
    memory_usage = sizeof(art_node48);
    break;
  case node256:
    memory_usage = sizeof(art_node256);
    break;
  default:
    break;
  }
#ifdef CAL_TOTAL_MEM_USAGE
  update_memory_usage(-memory_usage);
#endif
  sidle::sidle_free<art_node>(an, memory_usage);
#else
  free((void *)an);
#endif
  #endif
}

art_node** art_node_find_child(art_node *an, uint64_t version, unsigned char byte)
{
  debug_assert_art(is_leaf(an) == 0);

  switch (get_node_type(version)) {
  case node4: {
    art_node4 *an4 = (art_node4*)an;
    debug_assert_art(get_count(version) < 5);
    for (int i = 0, count = get_count(version); i < count; ++i)
      if (an4->key[i] == byte) {
        debug_assert_art(an4->child[i]);
        return &(an4->child[i]);
      }
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)an;
    debug_assert_art(get_count(version) < 17);
    __m128i key = _mm_set1_epi8(byte);
    __m128i key2 = _mm_loadu_si128((__m128i *)an16->key);
    __m128i cmp = _mm_cmpeq_epi8(key, key2);
    int mask = (1 << get_count(version)) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    if (bitfield) {
      debug_assert_art(an16->child[__builtin_ctz(bitfield)]);
      return &(an16->child[__builtin_ctz(bitfield)]);
    }
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    debug_assert_art(get_count(version) < 49);
    int index = an48->index[byte];
    if (index) {
      debug_assert_art(an48->child[index - 1]);
      return &(an48->child[index - 1]);
    }
  }
  break;
  case node256: {
    art_node256 *an256 = (art_node256 *)an;
    if (an256->child[byte])
      return &(an256->child[byte]);
  }
  break;
  default:
    assert(0);
  }
  return 0;
}

void art_node_set_new_node(art_node *old, art_node *new_)
{
  __atomic_store(&old->new_, &new_, __ATOMIC_RELAXED);
}

static inline art_node* art_node_get_new_node(art_node *old)
{
  art_node *new_;
  __atomic_load(&old->new_, &new_, __ATOMIC_RELAXED);
  return new_;
}

// require: node is locked
static art_node* art_node_grow(art_node *an)
{
  art_node *new_;
  uint64_t version = an->version;

  debug_assert_art(is_locked(version));

  switch (get_node_type(version)) {
  case node4: {
    art_node16 *an16 = (art_node16 *)(new_ = new_art_node16(an->parent, 
                                      sizeof(art_node16) - sizeof(art_node4)));
    art_node4 *an4 = (art_node4 *)an;
    debug_assert_art(get_count(version) == 4);
    memcpy(an16->prefix, an4->prefix, 8);
    an16->version = set_prefix_len(an16->version, get_prefix_len(version));
    an16->parent = an4->parent;
    for (int i = 0; i < 4; ++i) {
      an16->key[i] = an4->key[i];
      an16->child[i] = an4->child[i];
      if (!is_leaf(an4->child[i]))
        an4->child[i]->parent = new_;
    }
    an16->version = set_count(an16->version, 4);
  }
  break;
  case node16: {
    art_node48 *an48 = (art_node48 *)(new_ = new_art_node48(an->parent, 
                                      sizeof(art_node48) - sizeof(art_node16)));
    art_node16 *an16 = (art_node16 *)an;
    debug_assert_art(get_count(version) == 16);
    memcpy(an48->prefix, an16->prefix, 8);
    an48->version = set_prefix_len(an48->version, get_prefix_len(version));
    an48->parent = an16->parent;
    for (int i = 0; i < 16; ++i) {
      an48->child[i] = an16->child[i];
      if (!is_leaf(an16->child[i]))
        an16->child[i]->parent = new_;
      an48->index[an16->key[i]] = i + 1;
    }
    an48->version = set_count(an48->version, 16);
  }
  break;
  case node48: {
    art_node256 *an256 = (art_node256 *)(new_ = new_art_node256(an->parent,
                                    sizeof(art_node256) - sizeof(art_node48)));
    art_node48 *an48 = (art_node48 *)an;
    debug_assert_art(get_count(version) == 48);
    memcpy(an256->prefix, an48->prefix, 8);
    an256->version = set_prefix_len(an256->version, get_prefix_len(version));
    an256->parent = an48->parent;
    for (int i = 0; i < 256; ++i) {
      int index = an48->index[i];
      if (index) {
        an256->child[i] = an48->child[index - 1];
        if (!is_leaf(an48->child[index - 1]))
          an48->child[index - 1]->parent = new_;
      }
    }
  }
  break;
  default:
    // node256 is not growable
    assert(0);
  }

  assert(art_node_lock(new_) == 0);
  art_node_set_offset(new_, get_offset(version));
  art_node_set_new_node(an, new_);
  art_node_set_version(an, set_old(version));
  return new_;
}

// add a child to art_node, return 0 on success, otherwise return next layer
// require: node is locked
art_node** art_node_add_child(art_node *an, unsigned char byte, art_node *child, art_node **new_)
{
  debug_assert_art(is_leaf(an) == 0);

  uint64_t version = an->version;
  debug_assert_art(is_locked(version));

  art_node **next;
  if ((next = art_node_find_child(an, version, byte)))
    return next;

  // grow if necessary
  if (unlikely(art_node_is_full(an))) {
    *new_ = art_node_grow(an);
#ifdef RECORD_NODE_GEN
    printf("[DEBUG] art_node_add_child grow from %p to %p\n",
      an, *new_);
#endif
    an = *new_;
    version = an->version;
  }

  switch (get_node_type(version)) {
  case node4: {
    art_node4 *an4 = (art_node4 *)an;
    debug_assert_art(get_count(version) < 4);
    for (int i = 0, count = get_count(version); i < count; ++i)
      debug_assert_art(an4->key[i] != byte);
    // no need to be ordered
    int count = get_count(version);
    an4->child[count] = child;
    an4->key[count] = byte;
    an4->version = incr_count(version);
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)an;
    #ifdef Debug
    __m128i key = _mm_set1_epi8(byte);
    __m128i key2 = _mm_loadu_si128((__m128i *)an16->key);
    __m128i cmp = _mm_cmpeq_epi8(key, key2);
    int mask = (1 << get_count(version)) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    debug_assert_art(bitfield == 0);
    #endif
    // no need to be ordered
    int count = get_count(version);
    an16->child[count] = child;
    an16->key[count] = byte;
    an16->version = incr_count(version);
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    debug_assert_art(an48->index[byte] == 0);
    version = incr_count(version);
    an48->child[get_count(version) - 1] = child;
    an48->index[byte] = get_count(version);
    an48->version = version;
  }
  break;
  case node256: {
    art_node256 *an256 = (art_node256 *)an;
    debug_assert_art(an256->child[byte] == 0);
    an256->child[byte] = child;
  }
  break;
  default:
    assert(0);
  }
  if (!is_leaf(child)) {
    child->parent = an;
  }

  if (new_ && *new_)
    art_node_unlock(*new_);
  return 0;
}

// require: node is locked
inline int art_node_is_full(art_node *an)
{
  uint64_t version = an->version;

  debug_assert_art(is_locked(version));

  switch (get_node_type(version)) {
  case node4 : return get_count(version) == 4;
  case node16: return get_count(version) == 16;
  case node48: return get_count(version) == 48;
  default: return 0;
  }
}

void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len)
{
  memcpy(an->prefix, (char *)key + off, prefix_len);
  an->version = set_prefix_len(an->version, prefix_len);
}

// return the first offset that differs
int art_node_prefix_compare(art_node *an, uint64_t version, const void *key, size_t len, size_t off)
{
  debug_assert_art(off <= len);

  int prefix_len = get_prefix_len(version);
  const char *prefix = an->prefix, *cur = (const char *)key;
  debug_assert_art(prefix_len >= 0 && prefix_len <= 8);

  int i = 0;
  for (; i < prefix_len && off < len; ++i, ++off) {
    if (prefix[i] != cur[off])
      return i;
  }

  return i;
}

// require: node is locked
unsigned char art_node_truncate_prefix(art_node *an, int off)
{
  uint64_t version = an->version;

  debug_assert_art(is_locked(version));

  debug_assert_art(off < get_prefix_len(version));

  // mark expand bit before truncate prefix
  version = set_expand(version);
  art_node_set_version(an, version);

  int prefix_len = get_prefix_len(version);
  char *prefix = an->prefix;
  unsigned char ret = prefix[off];
  for (int i = 0, j = off + 1; j < prefix_len; ++i, ++j)
    prefix[i] = prefix[j];

  version = set_prefix_len(version, prefix_len - off - 1);
  off += get_offset(version) + 1;
  version = set_offset(version, off);
  art_node_set_version_unsafe(an, version);

  return ret;
}


uint64_t art_node_get_stable_expand_version(art_node *an)
{
  int loop = 4;
  uint64_t version = art_node_get_version_unsafe(an);
  while (is_expanding(version)) {
    for (int i = 0; i < loop; ++i)
      __asm__ volatile("pause" ::: "memory");
    if (loop < 128)
      loop += loop;
    version = art_node_get_version_unsafe(an);
  }
  return version;
}

// return 0 on success, 1 on failure
int art_node_lock(art_node *an)
{
  while (1) {
    // must use `acquire` operation to avoid deadlock
    uint64_t version = art_node_get_version(an);
    if (is_locked(version)) {
      // __asm__ __volatile__ ("pause");
      continue;
    }
    if (unlikely(is_old(version)))
      return 1;
    if (__atomic_compare_exchange_n(&an->version, &version, set_lock(version),
      1 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
      break;
  }
  return 0;
}

art_node* art_node_get_locked_parent(art_node *an)
{
  art_node *parent;
  while (1) {
    if ((parent = art_node_get_parent(an)) == 0)
      break;
    if (unlikely(art_node_lock(parent)))
      continue;
    if (art_node_get_parent(an) == parent)
      break;
    art_node_unlock(parent);
  }
  return parent;
}

// require: node is locked
void art_node_unlock(art_node *an)
{
  uint64_t version = an->version;

  debug_assert_art(is_locked(version));

  //if (is_inserting(version)) {
  //  incr_vinsert(version);
  //  version = unset_insert(version);
  //}
  if (is_expanding(version)) {
    version = incr_vexpand(version);
    version = unset_expand(version);
  }

  art_node_set_version(an, unset_lock(version));
}

int art_node_version_is_old(uint64_t version)
{
  return is_old(version);
}

art_node* art_node_replace_leaf_child(art_node *parent, art_node *an, 
                                      const void *key, size_t len, size_t off)
{
  debug_assert_art(is_leaf(an));

  const char *k1 = get_leaf_key(an), *k2 = (const char *)key;
  size_t l1 = get_leaf_len(an), l2 = len, i;
  for (i = off; i < l1 && i < l2 && k1[i] == k2[i]; ++i)
    ;
  if (unlikely(i == l1 && i == l2)) {
    // update the leaf's access time
#ifdef REAL_WORLD
    sidle::update_access<leaf_node>(get_leaf(reinterpret_cast<char*>(an)));
    auto tmp_sidle_meta = get_leaf(reinterpret_cast<char*>(an))->sidle_meta;
    an = (art_node *)alloc_leaf(parent, k2, len, 
                                sidle::node_mem_type::unknown);
    get_leaf(reinterpret_cast<char*>(an))->sidle_meta = tmp_sidle_meta;
#else
    sidle::update_access<leaf_node>(get_leaf(reinterpret_cast<char*>(an)));
#endif
    return 0; // key exists
  }

  art_node *new_ = new_art_node(parent);
  new_->parent = parent;
  art_node_set_offset(new_, off);
  assert(art_node_lock(new_) == 0);
  // TODO: i - off might be bigger than 8
  assert(i - off <= 8);
  art_node_set_prefix(new_, k1, off, i - off);
  off = i;
  unsigned char byte;
  byte = off == l1 ? 0 : k1[off];
  assert(art_node_add_child(new_, byte, an, 0) == 0);
  byte = off == l2 ? 0 : k2[off];
  assert(art_node_add_child(new_, byte, 
        (art_node *)alloc_leaf(new_, k2, len), 0) == 0);
  // update the leaf node's depth
  get_leaf(reinterpret_cast<char*>(an))->sidle_meta.metadata.depth = 
        std::max(get_leaf(reinterpret_cast<char*>(an))->sidle_meta.metadata.depth,
                          static_cast<uint8_t>(new_->sidle_meta.depth + 1));
#ifdef RECORD_NODE_GEN
  printf("[DEBUG] line 640, update new node depth: %d\n", 
      get_leaf(reinterpret_cast<char*>(an))->sidle_meta.metadata.depth);
#endif
  // assert(art_node_add_child(new_, byte, (art_node *)make_leaf(k2), 0) == 0);
  art_node_unlock(new_);

  return new_;
}

// require: node is locked
art_node* art_node_expand_and_insert(art_node *parent, art_node *an, 
                            const void *key, size_t len, size_t off, int common)
{
  debug_assert_art(is_locked(an->version));
  
  art_node* new_ = new_art_node(parent);
  new_->parent = parent;
  art_node_set_offset(new_, off);
  assert(art_node_lock(new_) == 0);
  art_node_set_prefix(new_, key, off, common);
  unsigned char byte;
  byte = (off + common < len) ? ((unsigned char *)key)[off + common] : 0;
  assert(art_node_add_child(new_, byte, 
        (art_node *)alloc_leaf(new_, key, len), 0) == 0);
  // assert(art_node_add_child(new_, byte, (art_node *)make_leaf(key), 0) == 0);
  byte = art_node_truncate_prefix(an, common);
  assert(art_node_add_child(new_, byte, an, 0) == 0);
  // update old node's depth
  an->sidle_meta.depth = std::max(an->sidle_meta.depth, 
                              static_cast<uint8_t>(new_->sidle_meta.depth + 1));
#ifdef RECORD_NODE_GEN
  printf("[DEBUG] update new node %p depth: %dm\n", an, an->sidle_meta.depth);
  art_node_unlock(new_);
#endif

  return new_;
}

// require: parent is locked
void art_node_replace_child(art_node *parent, unsigned char byte, art_node *old, art_node *new_)
{
  (void)old;
  uint64_t version = parent->version;
  debug_assert_art(is_locked(version));

  art_node **child = art_node_find_child(parent, version, byte);

  debug_assert_art(child && *child == old);

  __atomic_store(child, &new_, __ATOMIC_RELEASE);
  new_->parent = parent;
}

void art_node_traverse(art_node *cur_node, node_callback cb) {
    switch (get_node_type(cur_node->version)) {
    case node4: {
      int child_count = get_count(cur_node->version);
      art_node** child = reinterpret_cast<art_node4*>(cur_node)->child;
      for (int i = 0; i < child_count; ++i) {
        if (!cb(child[i])) {  // early stop
          break;
        }
      }
      break;
    }
    case node16: {
      int child_count = get_count(cur_node->version);
      art_node** child = reinterpret_cast<art_node16*>(cur_node)->child;
      for (int i = 0; i < child_count; ++i) {
        if (!cb(child[i])) {  // early stop
          break;
        }
      }
      break;
    }
    case node48: {
      art_node48* n48 = reinterpret_cast<art_node48*>(cur_node);
      for (int i = 0; i < 256; ++i) {
        char index = n48->index[i];
        if (index) {
          if (!cb(n48->child[index - 1])) { // early stop
            break;
          }
        }
      }
      break;
    }
    case node256: {
      art_node256* n256 = reinterpret_cast<art_node256*>(cur_node);
      for (int i = 0; i < 256; ++i) {
        if (n256->child[i]) {
          if (!cb(n256->child[i])) {  // early stop
            break;
          }
        }
      }
      break;
    }
    }
}

#ifdef Debug
void art_node_print(art_node *an)
{
  uint64_t version = art_node_get_version(an);

  if (an->new_) {
    printf("has new:\n");
    art_node_print(an->new_);
  }

  printf("%p\n", an);
  printf("is_locked:  %u\n", !!is_locked(version));
  printf("is_old:  %u\n", !!is_old(version));
  printf("is_expand:  %u  vexpand:  %u\n", !!is_expanding(version), get_vexpand(version));
  printf("prefix_len: %d\n", get_prefix_len(version));
  for (int i = 0; i < get_prefix_len(version); ++i) {
    printf("%d ", (unsigned char)an->prefix[i]);
  }
  printf("\n");
  switch (get_node_type(version)) {
  case node4: {
    printf("type 4\n");
    art_node4 *an4 = (art_node4 *)an;
    for (int i = 0; i < get_count(version); ++i) {
      if (!is_leaf(an4->child[i]))
        printf("%d %p\n", an4->key[i], an4->child[i]);
      else {
        printf("%d ", an4->key[i]);
        print_key(get_leaf_key(an4->child[i]), 8);
      }
    }
  }
  break;
  case node16: {
    printf("type 16\n");
    art_node16 *an16 = (art_node16 *)an;
    for (int i = 0; i < get_count(version); ++i) {
      if (!is_leaf(an16->child[i]))
        printf("%d %p\n", an16->key[i], an16->child[i]);
      else {
        printf("%d ", an16->key[i]);
        print_key(get_leaf_key(an16->child[i]), 8);
      }
    }
  }
  break;
  case node48: {
    printf("type 48\n");
    art_node48 *an48 = (art_node48 *)an;
    for (int i = 0; i < 256; ++i)
      if (an48->index[i]) {
        if (!is_leaf(an48->child[an48->index[i] - 1]))
          printf("%d %p\n", i, an48->child[an48->index[i] - 1]);
        else {
          printf("%d ", i);
          print_key(get_leaf_key(an48->child[an48->index[i] - 1]), 8);
        }
      }
  }
  break;
  case node256: {
    printf("type 256\n");
    art_node256 *an256 = (art_node256 *)an;
    for (int i = 0; i < 256; ++i)
      if (an256->child[i]) {
        if (!is_leaf(an256->child[i]))
          printf("%d %p\n", i, an256->child[i]);
        else {
          printf("%d ", i);
          print_key(get_leaf_key(an256->child[i]), 8);
        }
      }
  }
  break;
  default:
    assert(0);
  }
  printf("\n");
}

void print_key(const void *key, size_t len)
{
  unsigned char *n = (unsigned char *)key;
  for (int i = 0; i < (int)len; ++i) {
    printf("%d ", n[i]);
  }
  printf("\n");
}

#endif // Debug


} // namespace art
