#ifndef _art_def_hh_
#define _art_def_hh_

#include <cstdint>
#include <functional>
#include <vector>

#include "sidle_struct.hh"

// #define CAL_NODE_HOTNESS

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

#define node4    ((uint64_t)0x000000)
#define node16   ((uint64_t)0x100000)
#define node48   ((uint64_t)0x200000)
#define node256  ((uint64_t)0x300000)

/**
 *   node version layout(64 bits)
 *       off               count    prefix_len              type  old  lock insert expand vinsert   vexpand
 *   |    8    |   8    |    8     |    8    |     10     |  2  |  1  |  1  |  1  |  1  |    8    |    8    |
 *
**/
#define OLD_BIT    ((uint64_t)1 << 19)
#define LOCK_BIT   ((uint64_t)1 << 18)
#define INSERT_BIT ((uint64_t)1 << 17)
#define EXPAND_BIT ((uint64_t)1 << 16)

#define set_offset(version, offset)  (((version) & (~(((uint64_t)0xff) << 54))) | ((uint64_t)(offset) << 54))
#define get_offset(version)          (size_t)(((version) >> 54) & 0xff)
#define get_prefix_len(version)      (int)(((version) >> 32) & 0xff)
#define set_prefix_len(version, len) (((version) & (~(((uint64_t)0xff) << 32))) | (((uint64_t)(len)) << 32))
#define get_count(version)           (int)(((version) >> 40) & 0xff)
#define set_count(version, count)    (((version) & (~(((uint64_t)0xff) << 40))) | (((uint64_t)(count)) << 40))
#define incr_count(version)          ((version) + ((uint64_t)1 << 40))
#define get_node_type(version)            (int)((version) & node256)
#define set_type(version, type)      ((version) | type)

#define is_old(version)       ((version) & OLD_BIT)
#define is_locked(version)    ((version) & LOCK_BIT)
#define is_inserting(version) ((version) & INSERT_BIT)
#define is_expanding(version) ((version) & EXPAND_BIT)

#define set_old(version)    ((version) | OLD_BIT)
#define set_lock(version)   ((version) | LOCK_BIT)
#define set_insert(version) ((version) | INSERT_BIT)
#define set_expand(version) ((version) | EXPAND_BIT)

#define unset_lock(version)   ((version) & (~LOCK_BIT))
#define unset_insert(version) ((version) & (~INSERT_BIT))
#define unset_expand(version) ((version) & (~EXPAND_BIT))

#define get_vinsert(version)  ((int)(((version) >> 8) & 0xff))
#define incr_vinsert(version) (((version) & (~((uint64_t)0xff << 8))) | (((version) + (1 << 8)) & (0xff << 8))) // overflow is handled

#define get_vexpand(version)  ((int)((version) & 0xff))
#define incr_vexpand(version) (((version) & ~((uint64_t)0xff)) | (((version) + 1) & 0xff)) // overflow is handled

#define LEAF_DEPTH 8 // leaf node depth
#define IS_VALID_ADDR(addr) ((addr >> 48) == 0)

namespace art {

struct art_node;

#define art_node_header \
  uint64_t version;     \
  char prefix[8];       \
  art_node *new_;        \
  art_node *parent;      \
  sidle::node_metadata sidle_meta;  

// 32 bytes
struct art_node
{
  art_node_header;
#ifdef CAL_NODE_HOTNESS
  uint32_t access_count;
#endif
};

struct art_node4
{
  art_node_header;
#ifdef CAL_NODE_HOTNESS
  uint32_t access_count;
#endif
  unsigned char key[4];
  unsigned char unused[4];
  art_node *child[4];
  char meta[0];
};

struct art_node16
{
  art_node_header;
#ifdef CAL_NODE_HOTNESS
  uint32_t access_count;
#endif
  unsigned char key[16];
  art_node *child[16];
  char meta[0];
};

struct art_node48
{
  art_node_header;
#ifdef CAL_NODE_HOTNESS
  uint32_t access_count;
#endif
  unsigned char index[256];
  art_node *child[48];
  char meta[0];
};

struct art_node256
{
  art_node_header;
#ifdef CAL_NODE_HOTNESS
  uint32_t access_count;
#endif
  art_node *child[256];
  char meta[0];
};

#pragma pack(push, 1)
struct leaf_node 
{
  uint8_t key_length;
  sidle::leaf_metadata sidle_meta;
  char key[];
};
#pragma pack(pop)

struct adaptive_radix_tree
{
  art_node *root;
};

#define is_leaf(ptr) ((uintptr_t)(ptr) & 1)
#define make_leaf(ptr) ((uintptr_t)((const char *)(ptr) - 1) | 1)
#define get_leaf_key(ptr) (((const char *)((uintptr_t)(ptr) & (~(uintptr_t)1))) + 4)
#define get_leaf_len(ptr) ((size_t)*(char *)((uintptr_t)(ptr) & (~(uintptr_t)1)))
#define get_leaf(ptr) ((leaf_node *)((uintptr_t)(ptr) & (~(uintptr_t)1)))

typedef std::function<void(art_node*, leaf_node*)> art_callback;
/// @brief if the callback returns false, the traversal will be stopped
typedef std::function<bool(art_node*)> node_callback;

} // namespace art

#endif