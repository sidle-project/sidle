#ifndef MASSTREE_SIDLE_HH
#define MASSTREE_SIDLE_HH

#include <functional>
#include <tuple>

#include "sidle_meta.hh"
#include "btree_leaflink.hh"
#include "kvthread.hh"
#include "masstree.hh"
#include "masstree_scan.hh"
#include "masstree_struct.hh"
// #include "migration_policy.hh"
// #include "migration_struct.hh"
#include "query_masstree.hh"

namespace Masstree {

template <typename T>
using base_node_t = node_base<T>;
template <typename T>
using leaf_node_t = leaf<T>;
template <typename T>
using internode_t = internode<T>;
template <typename T>
using tree_t = basic_table<T>;
template <typename T>
using masstree_cb = std::function<void(internode_t<T>*, leaf_node_t<T>*)>;

template <typename T>
using node_cb = std::function<bool(base_node_t<T>*)>;

using sidle::node_mem_type;

/// @brief convert numeric value to string key
/// @note if the value is little endian, reverse it
uint64_t to_str_order_key(uint64_t value) {
  uint64_t reversed = 0;
  for (size_t idx = 0; idx < sizeof(uint64_t); idx++) {
    uint8_t byte = value & 0xFF;
    reversed = (reversed << 8) | byte;
    value = value >> 8;
  }
  return reversed;
  return value;
}

template <typename T, typename ...Args>
void masstree_leaf_traverse(tree_t<T>* tree, masstree_cb<T> cb, Args... args) {
  using leaf_iterator = leaf_iterator<T>;
  using tracker_t = typename leaf_iterator::tracker_t;
  leaf_iterator it(tree ? tree->root() : nullptr);
  uint64_t str_order_key = to_str_order_key(0);
  threadinfo *ti = nullptr;
  if constexpr (sizeof...(args) > 0) {
    auto args_tuple = std::make_tuple(args...);
    ti = std::get<0>(args_tuple);
  }
  it.init((char *)&str_order_key, *ti);
  while (it.state() != tracker_t::scan_end) {
    if (!it.node()) {
      it.next(*ti);
      continue;
    }
    cb(nullptr, it.node());
    it.next(*ti);
  }
}

/// @return the parent node
template <typename T, typename ...Args>
internode_t<T>* masstree_leaf_migration(leaf_node_t<T>* cur_node, internode_t<T>* parent, sidle::node_mem_type target_type, Args... args) {
  using leaf_type = leaf_node_t<T>;
  using internode_type = internode_t<T>;
  threadinfo *ti = nullptr;
  if constexpr (sizeof...(args) > 0) {
    auto args_tuple = std::make_tuple(args...);
    ti = std::get<0>(args_tuple);
  }
  // the current leaf node is valid
  if (!cur_node) {
    return nullptr;
  }
  if (cur_node->deleted()) {
    return nullptr;
  }
  if (cur_node->sidle_meta.metadata.type == target_type) {
    return nullptr;
  }
  // ensure the node is not being migrating, being inserted or splitted
  if (cur_node->migrating() || cur_node->inserting() ||
      cur_node->splitting()) {
    return nullptr;
  }
  // ensure the node is not being inserted or splitting
  typename base_node_t<T>::nodeversion_type v;
  do {
    v = *cur_node;
    fence();
  } while (cur_node->has_changed(v));

  leaf_type *new_node = leaf_type::make_with_cxl_policy(
    cur_node->ksuf_used_capacity(), cur_node->phantom_epoch(), *ti,
    cur_node->sidle_meta.metadata.depth, target_type,
    cur_node->sidle_meta.access_time, true);
  cur_node->lock(*cur_node, ti->lock_fence(tc_leaf_lock));
  memcpy(new_node, cur_node, sizeof(*new_node));
  new_node->sidle_meta.metadata.type = target_type;
  cur_node->mark_migration();

  internode_type *p = cur_node->locked_parent(*ti);
  assert(!p || p->locked());
  // check whether parent node is valid
  if ((!p && !cur_node->is_root()) || p->deleted()) {
    fprintf(stderr,
            "[migrate_leaf] the parent node for node %lu is deleted or not "
            "exist\n",
            reinterpret_cast<uint64_t>(cur_node));
    // SPDLOG_ERROR("the parent node {} for node {} is deleted or not exist",
    // reinterpret_cast<uint64_t>(p), reinterpret_cast<uint64_t>(cur_node));
    cur_node->unlock();
    new_node->unlock();
    if (p) {
      p->unlock();
    }
    return nullptr;
  }

  assert(!p || p->locked());
  // change the parent-child relation pointer
  int posi_in_parent =
      internode_type::bound_type::upper(cur_node->ikey0_[0], *p);
  assert(posi_in_parent >= 0 && posi_in_parent <= internode_type::width);
  if (p->child_[posi_in_parent] != cur_node) {
    int posi_in_parent_for_debug = -1;
    for (int i = 0; i <= p->nkeys_; ++i) {
      if (p->child_[i] == cur_node) {
        posi_in_parent_for_debug = i;
        break;
      }
    }
    if (posi_in_parent_for_debug != -1) {
      posi_in_parent = posi_in_parent_for_debug;
    } else {
      fprintf(stderr,
              "[migrate_leaf] %s executor: invalid child node position, the "
              "position in parent is %d, actual node is: %lu, current node "
              "is: %lu\n",
              target_type == node_mem_type::local ? "promotion" : "demotion",
              posi_in_parent,
              reinterpret_cast<uint64_t>(p->child_[posi_in_parent]),
              reinterpret_cast<uint64_t>(cur_node));
      cur_node->unlock();
      new_node->unlock();
      p->unlock();
      return nullptr;
    }
  }

  p->assign_copy(posi_in_parent - 1, new_node);
  assert(!p || p->locked());

  // update the leaf node list
  btree_leaflink<leaf_type, T::concurrent>::change_link(
    static_cast<leaf_type *>(cur_node), static_cast<leaf_type *>(new_node));

  // begin to delete the original node
  cur_node->mark_deleted();
  cur_node->deallocate_rcu(*ti);
  if (cur_node->locked()) {
    cur_node->unlock();
  }
  if (new_node->locked()) {
    new_node->unlock();
  }

  return p;
}

template <typename T, typename ...Args>
internode_t<T>* masstree_internode_migration(internode_t<T>* original_node, sidle::node_mem_type target_type, bool is_new, Args... arg) {
  using internode_type = internode_t<T>;
  threadinfo *ti = nullptr;
  if constexpr (sizeof...(arg) > 0) {
    auto arg_tuple = std::make_tuple(arg...);
    ti = std::get<0>(arg_tuple);
  }
  if (is_new) {
    original_node->lock(*original_node, ti->lock_fence(tc_internode_lock));
  }

  masstree_precondition(original_node->locked());
  if (original_node->deleted() ||
      original_node->sidle_meta.type == target_type) {
    original_node->unlock();
    return nullptr;
  }
  if (original_node->sidle_meta.depth == 1) {
    assert(target_type == node_mem_type::local);
  }

  internode_type *new_node = internode_type::make_with_cxl_policy(
        original_node->height_, *ti, original_node->sidle_meta.depth,
        target_type, true);
  memcpy(new_node, original_node, sizeof(*new_node));
  new_node->sidle_meta.type = target_type;
  original_node->mark_migration();
  internode_type *p = original_node->locked_parent(*ti);
  // check whether the parent node is valid
  if ((!p && !original_node->is_root()) || (p && p->deleted())) {
    fprintf(stderr,
            "[migrate_internode] [PID %lu] the parent node for node %lu is "
            "deleted or not exist\n",
            getpid(), reinterpret_cast<uint64_t>(original_node));
    new_node->unlock();
    original_node->unlock();
    if (p) {
      p->unlock();
    }
    return nullptr;
  }

  // change the parent-child relation
  if (!p) {
    new_node->make_layer_root();
    fence();
  } else {
    int posi_in_parent =
        internode_type::bound_type::upper(original_node->ikey0_[0], *p);
    assert(posi_in_parent >= 0 && posi_in_parent <= internode_type::width);
    if (p->child_[posi_in_parent] != original_node) {
      int posi_in_parent_for_debug = -1;
      for (int i = 0; i <= p->nkeys_; ++i) {
        if (p->child_[i] == original_node) {
          posi_in_parent_for_debug = i;
          break;
        }
      }
      if (posi_in_parent_for_debug != -1) {
        posi_in_parent = posi_in_parent_for_debug;
      } else {
        fprintf(stderr,
                "[migrate_internode] %s executor: invalid child node "
                "position, the position in parent is %d, actual node is: "
                "%lu, current node is: %lu\n",
                target_type == node_mem_type::local ? "promotion"
                                                    : "demotion",
                posi_in_parent,
                reinterpret_cast<uint64_t>(p->child_[posi_in_parent]),
                reinterpret_cast<uint64_t>(original_node));
        original_node->unlock();
        new_node->unlock();
        p->unlock();
        return nullptr;
      }
    }
    // assert(p->child_[posi_in_parent] == original_node);
    p->assign_copy(posi_in_parent - 1, new_node);
  }

  // change the children's parent
  for (int i = 0; i <= original_node->nkeys_; ++i) {
    original_node->child_[i]->set_parent(new_node);
  }

  // begin to delete the original node
  original_node->mark_deleted();
  original_node->deallocate_rcu(*ti);
  original_node->unlock();
  new_node->unlock();
  return p;
}

template <typename T>
void masstree_unlock(internode_t<T>* node) {
  if (node && node->locked()) {
    node->unlock();
  }
}

template <typename T>
void masstree_internode_traverse(internode_t<T>* parent_node, node_cb<T> cb) {
  for (int i = 0; i <= parent_node->nkeys_; ++i) {
    if (!cb(parent_node->child_[i])) {
      break;
    }
  }
}

template <typename T>
bool masstree_is_leaf(base_node_t<T>* node) {
  return node != nullptr && node->isleaf();
}

template <typename T>
leaf_node_t<T>* masstree_get_leaf(base_node_t<T>* node) {
  return static_cast<leaf_node_t<T>*>(node);
}
} // namespace Masstree

#endif