#ifndef SIDLE_WORKER_HH
#define SIDLE_WORKER_HH

#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "art.hh"
#include "art_def.hh"
#include "art_node.hh"
#include "kvthread.hh"
#include "sidle_meta.hh"
#include "sidle_struct.hh"
#include "sidle_policy.hh"
#include "sidle_frontend.hh"

namespace sidle {

#define WORKER_DEBUG 0

using base = art_worker_base;
using leaf_node = art::leaf_node;

enum class tree_type {
  art = 0,
  masstree,
};

// N is the base node type
template <typename T, typename P, typename H, typename N, typename... Args>
struct tree_op {
  using tree_cb = std::function<void(P*, T*)>;
  using traverse_func_t = std::function<void(H*, tree_cb, Args...)>;
  using leaf_migration_t = std::function<P*(T*, P*, node_mem_type, Args...)>;
  using internode_migration_t = std::function<P*(P*, node_mem_type, bool, Args...)>;
  using unlock_func_t = std::function<void(P*)>;
  using node_cb_t = std::function<bool(N*)>;
  using node_traverse_func_t = std::function<void(P*, node_cb_t)>;
  using get_leaf_t = std::function<T*(N*)>;
  using is_leaf_t = std::function<bool(N*)>;

  traverse_func_t traverse_func_;
  leaf_migration_t leaf_migration_;
  internode_migration_t internode_migration_;
  unlock_func_t unlock_;
  node_traverse_func_t node_traverse_func_;
  get_leaf_t get_leaf_;
  is_leaf_t is_leaf_;
  tree_type type_;
};


class sidle_histogram {
public:
  enum class type { hot = 0, warm, cold };

  sidle_histogram(sidle_threshold* threshold_manager = nullptr,
    const size_t size = 16): histogram_(size, 0), next_histogram_(size, 0),  
                              threshold_manager_(threshold_manager) {} 

  ~sidle_histogram() = default;

  /// @brief update node access statistic info and decide its current hotness
  type update(uint16_t access_count) {
    if (access_count <= 1) {
      ++next_histogram_[0];
    } else {
      size_t cur_index = get_idx(access_count);
      ++next_histogram_[cur_index];
    }
    if (access_count > hot_threshold_) {
      return type::hot;
    }
    if (access_count < cold_threshold_) {
      return type::cold;
    }
    return type::warm;
  }

  /// @param total_number is the total number of leaf nodes in the new traversal
  void refresh(uint64_t total_number, uint64_t total_access_count) {
    total_number_ = total_number;
    total_access_count_ = total_access_count;
    // replace the histogram
    histogram_ = std::move(next_histogram_);
    next_histogram_.assign(histogram_.size(), 0);
  }

  // refer https://github.com/cosmoss-jigu/memtis
  void adjust_threshold() {
    while (is_cooling.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    uint64_t active_count = 0;
    uint64_t active_access = 0;
    if (total_number_ == 0) {
      return;
    }
    uint64_t active_lower_bound = threshold_manager_->get_hot_watermark() * 
                                  total_number_ / 100;
    uint64_t inactive_upper_bound = (100 - threshold_manager_->get_cold_watermark())
                                   * total_number_ / 100;
    int hot_index = -1, cold_index = -1;
    int weight = 1 << 15;
    for (int index = 15; index >= 0; --index) {
      // SIDLE_RECORD(WORKER_DEBUG, "[adjust_threshold] the histogram[%d] is %ld, active lower bound: %ld, inactive upper bound %ld\n", 
        // index, histogram_[index], active_lower_bound, inactive_upper_bound);
      if (active_count + histogram_[index] > active_lower_bound &&
          hot_index == -1) {
        hot_index = index;
      }
      if (active_count + histogram_[index] > inactive_upper_bound &&
          cold_index == -1) {
        cold_index = index;
        break;
      }
      active_count += histogram_[index];
      active_access += histogram_[index];
    }

    if (hot_index != 15) {
      ++hot_index;
    }
    if (cold_index == -1) {
      cold_index = 0;
    }
    hot_threshold_ = 1 << hot_index;
    cold_threshold_ = 1 << cold_index;
    SIDLE_RECORD(WORKER_DEBUG, "[adjust_threshold] the hot threshold is %d, the cold threshold is %d, hot watermark: %d, cold watermark: %d\n",   
      hot_threshold_, cold_threshold_, threshold_manager_->get_hot_watermark(), 
      threshold_manager_->get_cold_watermark());
  }

  void adjust_for_cooling() {
    for (int i = 0; i < 15; ++i) {
      histogram_[i] = histogram_[i + 1];
    }
    histogram_[15] = 0;
    hot_threshold_ = hot_threshold_ >> 1;
    cold_threshold_ = cold_threshold_ >> 1;
    is_cooling.store(false);
  }

  inline void decrease_tolerance_for_cold(int value_for_decrease = 1) {
    cold_threshold_ = cold_threshold_ + value_for_decrease;
  }

  inline void notify_cooling() { is_cooling.store(true); }

  inline bool cooling() { return is_cooling.load(); }

private:  
  // refer https://github.com/cosmoss-jigu/memtis
  std::size_t get_idx(uint16_t num) {
    unsigned int cnt = 0;
    num++;
    while (1) {
      num = num >> 1;
      if (num)
        cnt++;
      else {
        return cnt;
      }

      if (cnt == 15)
        break;
    }
    return cnt;
  }

  // The x-axis is in exponential form to adapt to the zip distribution
  std::vector<uint64_t> histogram_;
  /// @brief the next histogram for the next round
  std::vector<uint64_t> next_histogram_;
  uint16_t hot_threshold_{default_hot_threshold};
  uint16_t cold_threshold_{default_cold_threshold};

  std::atomic_bool is_cooling{false};
  // The total number of the leaf node
  uint64_t total_number_{0};
  uint64_t total_access_count_{0};
  sidle_threshold* threshold_manager_;
};

/// @tparam T is the type of leaf node, P is the type of internal node, 
// H is the type of tree
template <typename T, typename P, typename H, typename N, typename... Args>
class art_migration_trigger : public art_worker_base {
public:
  using base = art_worker_base;
  using histogram_ptr_t = std::shared_ptr<sidle_histogram>;
  using node_hotness = typename sidle_histogram::type;
  using tree_op_t = tree_op<T, P, H, N, Args...>;

  art_migration_trigger() = default;

  art_migration_trigger(std::chrono::milliseconds interval, 
                    H* tree, histogram_ptr_t histogram_, 
                    tree_op_t& tree_ops):
      art_worker_base(interval * 5), histogram_(histogram_), table_(tree), 
      tree_op_(tree_ops), ti_(nullptr) {
    // initialize the threadinfo
    if (tree_op_.type_ == tree_type::masstree) {
      ti_ = threadinfo::make(threadinfo::TI_MIGRATION, -1);
    }
  }
  
  ~art_migration_trigger() = default;

  void run_job() override {
    while (base::is_running_) {
      std::unique_lock<std::mutex> lock(base::trigger_wakeup_mtx_);
      auto now = std::chrono::system_clock::now();
      bool after_cooling = false;
      if (!base::trigger_wakeup_cv_.wait_until(
          lock, now + base::interval_,
          []() { return base::is_triggering_migration_.load();} )) {
        // is_triggering_migration_ is timeout
        if (!base::can_trigger_.load(std::memory_order_relaxed)) {
          continue;
        }
        base::is_triggering_migration_.store(true);
      } else {
        if (!base::can_trigger_.load(std::memory_order_relaxed)) {
          continue;
        }
        call_by_adjuster_ = true;
      }

#ifdef WORKER_DEBUG
      auto t1 = std::chrono::high_resolution_clock::now();
      SIDLE_RECORD(WORKER_DEBUG, "[migration_trigger] wakeup\n");
#endif

      // while for the art_cooler to finish
      while (histogram_->cooling()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        after_cooling = true;
      }

      // if the migration trigger is waken up by the threshold adjuster or the
      // threshold is changed after cooling, decrease the tolerance for cold to
      // accelerate the migration again
      if (call_by_adjuster_ && after_cooling) {
        histogram_->decrease_tolerance_for_cold();
      }

      ++round_counter_;
      // traverse the tree and check promotion or demotion
      if constexpr (sizeof...(Args) > 0) {
        tree_op_.traverse_func_(table_, 
                              [this](P* parent, T* cur) {
          this->trigger_migration_cb(parent, cur);
        }, ti_);
      } else {
        tree_op_.traverse_func_(table_, 
                              [this](P* parent, T* cur) {
          this->trigger_migration_cb(parent, cur);
        }); 
      }
      

#ifdef WORKER_DEBUG
      auto t2 = std::chrono::high_resolution_clock::now();
      SIDLE_RECORD(WORKER_DEBUG, "[migration_trigger] end, time usage: %ld\n",
        std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count());
#endif
      // refresh the histogram
      histogram_->refresh(node_counter_, access_counter_);
      node_counter_ = 0;
      access_counter_ = 0;
      // if the queue is not empty, notify the migration executor 
      if (base::queue_.get_promotion_queue_length() > 0 &&
          base::can_promote_.load(std::memory_order_relaxed)) {
        check_execute_promotion();
      }
      if (base::queue_.get_demotion_queue_length() > 0) {
        check_execute_demotion();
      }

      // update the hot/cold threshold every round
      histogram_->adjust_threshold();
      base::is_triggering_migration_.store(false);
    }
  }

private:
  void check_execute_demotion() {
    if (!base::need_demotion_ &&
        base::queue_.get_demotion_queue_length() > queue_waiting_threshold) {
      std::lock_guard<std::mutex> lock(base::demotion_mtx_);
      base::need_demotion_.store(true);
      base::demotion_trigger_cv_.notify_all();
    }
  }

  void check_execute_promotion() {
    if (!base::need_promotion_ &&
        base::queue_.get_promotion_queue_length() > queue_waiting_threshold) {
      std::lock_guard<std::mutex> lock(base::promotion_mtx_);
      base::need_promotion_.store(true);
      base::promotion_trigger_cv_.notify_all();
    }
  }

  void trigger_migration_cb(P* parent, T* cur) {
    node_hotness hotness = histogram_->update(cur->sidle_meta.access_time);
    ++node_counter_;
    access_counter_ += cur->sidle_meta.access_time;
    // check whether promotion operation can be performed at this time
    if (base::can_promote_.load(std::memory_order_relaxed) &&
        cur->sidle_meta.metadata.type == node_mem_type::remote && 
        hotness == node_hotness::hot) {
      SIDLE_CHECK(parent != nullptr, 
      "[trigger_migration_cb] when trigger migration the parent cannot be nullptr");
      if (tree_op_.type_ == tree_type::art) {
        /// @note because art leaf node not store the parent node information ,
        // need to store the parent node address to the queue after leaf node
        base::queue_.add_multi_task(task_type::promotion, 
                                    {make_leaf(reinterpret_cast<char*>(cur) + 1), 
                                    reinterpret_cast<uint64_t>(parent)});
      } else {
        base::queue_.add_task(task_type::promotion, 
                              reinterpret_cast<uint64_t>(cur));         
      }
      // SIDLE_RECORD(WORKER_DEBUG, "[trigger_migration_cb] new promotion candidate: %p, queue length: %d\n", 
      //           cur, base::queue_.get_promotion_queue_length());
      check_execute_promotion();
    } else if (hotness == node_hotness::cold &&
      (cur->sidle_meta.metadata.type == node_mem_type::local || call_by_adjuster_)) {
      // check whether demotion operation can be performed at this time
      SIDLE_CHECK(parent != nullptr, 
      "[trigger_migration_cb] when trigger migration the parent cannot be nullptr");
      if (tree_op_.type_ == tree_type::art) {
        base::queue_.add_multi_task(task_type::demotion, 
                                  {make_leaf(reinterpret_cast<char*>(cur) + 1), 
                                  reinterpret_cast<uint64_t>(parent)});
      } else {
        base::queue_.add_task(task_type::demotion,
                            reinterpret_cast<uint64_t>(cur));
      }
      check_execute_demotion();
    }
  }

  // sidle_threshold* threshold_manager_;
  histogram_ptr_t histogram_;
  H* table_;
  uint32_t round_counter_{0};
  uint64_t node_counter_{0};
  uint64_t access_counter_{0};
  bool call_by_adjuster_{false};
  tree_op_t tree_op_;
  threadinfo *ti_;
};


/// @brief the base class for migration executor
/// @tparam T is the type of leaf node, P is the type of internal node
template <typename T, typename P, typename H, typename N, typename... Args>
class art_executor_base : public art_worker_base {
public:
  using base = art_worker_base;
  using tree_op_t = tree_op<T, P, H, N, Args...>;

  art_executor_base() = default;
  art_executor_base(sidle_threshold* thresholds, std::chrono::milliseconds interval,
                    tree_op_t& tree_ops)
      : base(interval), threshold_manager_(thresholds), tree_op_(tree_ops) {}
  virtual ~art_executor_base() = default;

protected:
  /// @pre the parent hasn't been lock
  void migrate_leaf(T *cur_node, P* parent, 
                    node_mem_type target_type, threadinfo *ti) {
    P* p = nullptr;
    if constexpr (sizeof...(Args) > 0) {
      p = tree_op_.leaf_migration_(cur_node, parent, target_type, ti);
    } else {
      p = tree_op_.leaf_migration_(cur_node, parent, target_type);
    }
    if (!p) {
      return;
    }

    if (target_type == node_mem_type::remote) {
      auto status = need_continue_migrate(p, target_type);
      if (status == migration_state::proceed) {
        migrate_internode(p, target_type, ti);
      } else {
        if (status == migration_state::delay) {
          if (tree_op_.type_ == tree_type::art) {
            base::queue_.add_multi_task(task_type::demotion,
                                      {reinterpret_cast<uint64_t>(p), 0});
          } else {
            base::queue_.add_task(task_type::demotion, 
                                  reinterpret_cast<uint64_t>(p));
          }
        }
        tree_op_.unlock_(p);
      } 
    } else {
      migrate_internode(p, target_type, ti);
    }
  }

  /// @brief recursively migrate the internodes
  /// @pre the original node has been locked
  /// @param has_migrated for migrate local to remote, record how many nodes in
  /// the path have been migrated
  void migrate_internode(P* original_node, node_mem_type target_type,
                        threadinfo *ti = nullptr,
                        int has_migrated = 1) {
    SIDLE_CHECK(original_node != nullptr, 
            "[DEBUG] [migrate_internode] the original node is nullptr");
    // if the original is the internode popped from the queue, 
    // should check whether need to migrate it
    bool is_new = false;
    if (has_migrated == 0 && target_type == node_mem_type::remote) {
      auto state = need_continue_migrate(original_node, target_type, has_migrated);
      if (state == migration_state::stop) {
        return;
      }
      is_new = true;
    }
    P* parent = nullptr;
    if constexpr (sizeof...(Args) > 0) {
      parent = tree_op_.internode_migration_(original_node, target_type, is_new, ti);
    } else {
      parent = tree_op_.internode_migration_(original_node, target_type, is_new);
    }
    if (parent == nullptr) {
      return;
    }
    
    // check whether need continue to migrate the parent node
    auto state = need_continue_migrate(parent, target_type, has_migrated + 1); 
    if (state == migration_state::proceed) {
      migrate_internode(parent, target_type, ti, has_migrated + 1);
    } else if (state == migration_state::delay) {
      SIDLE_CHECK(target_type == node_mem_type::remote,
          "[DEBUG] [migrate_internode] delay node's type should be remote");
      if (tree_op_.type_ == tree_type::art) {
        base::queue_.add_multi_task(task_type::demotion, 
                                  {reinterpret_cast<uint64_t>(parent), 0}); 
      } else {
        base::queue_.add_task(task_type::demotion, reinterpret_cast<uint64_t>(parent));
      }
    }

    tree_op_.unlock_(parent);
  }

private:
  /// @brief check whether need to migrate the parent node in recursion 
  /// @note the parent node may not be locked
  migration_state need_continue_migrate(P* parent_node,
                                        node_mem_type target_type,
                                        int has_migrated = 1) {
    if (!parent_node) {
      return migration_state::stop;
    }

    if (target_type == node_mem_type::remote) {
      if (parent_node->sidle_meta.depth <=
          threshold_manager_->get_demotion_depth_threshold() ||
          parent_node->sidle_meta.depth == 1) {
        return migration_state::stop;
      }
      // check whether there exists a local node in the child nodes
      bool has_local = false;
      tree_op_.node_traverse_func_(parent_node, [&has_local, this](N* child) {
        if (this->tree_op_.is_leaf_(child)) {
          T* leaf = this->tree_op_.get_leaf_(child);
          if (leaf->sidle_meta.metadata.type == node_mem_type::local) {
            has_local = true;
            return false;
          }
        } else {
          if (static_cast<P*>(child)->sidle_meta.type == node_mem_type::local) {
            has_local = true;
            return false;
          }
        }
        return true;
      });
      
      // After all child nodes have been traversed and the parent has
      // been poped, but there are still child nodes in the local memory, 
      // stop demotion
      if (has_migrated == 0) {
        return has_local ? migration_state::stop : migration_state::proceed;
      }
      return has_local ? migration_state::delay : migration_state::proceed;
    }

    if (target_type == node_mem_type::local) {
      return parent_node->sidle_meta.type != node_mem_type::local ? 
              migration_state::proceed : migration_state::stop;
    }

    return migration_state::stop;
  }

  sidle_threshold* threshold_manager_{nullptr};
  tree_op_t tree_op_;
};

/// @brief the background worker to execute promotion
// for every node waiting for promotion, promote all remote ancestors
template <typename T, typename P, typename H, typename N, typename... Args>
class art_promotion_executor : public art_executor_base<T, P, H, N, Args...> {
public:
  using base = art_worker_base;
  using art_executor_base = art_executor_base<T, P, H, N, Args...>;
  using tree_op_t = tree_op<T, P, H, N, Args...>;

  art_promotion_executor() = default;
  art_promotion_executor(sidle_threshold* thresholds,
    std::chrono::milliseconds interval, tree_op_t& tree_ops) : 
    art_executor_base(thresholds, interval, tree_ops), tree_op_(tree_ops) {
    if (tree_op_.type_ == tree_type::masstree) {
      ti_ = threadinfo::make(threadinfo::TI_MIGRATION, -1);
    }
  }
  ~art_promotion_executor() = default;

  void run_job() override {
    while (base::is_running_) {
      auto now = std::chrono::system_clock::now();
      std::unique_lock<std::mutex> lock(base::promotion_mtx_);
      if (!base::promotion_trigger_cv_.wait_until(
          lock, now + base::interval_,
          []() { return base::need_promotion_.load(); })) {
        base::need_promotion_.store(true);
      }

      // SIDLE_RECORD(WORKER_DEBUG, "[art_promotion_executor] wakeup\n");
      while (base::can_promote_.load(std::memory_order_relaxed) && 
            base::is_running_) {
        // get the leaf node address and parent node address
        if (tree_op_.type_ == tree_type::art) {
          auto [addr, parent_addr] = base::queue_.get_multi_task(task_type::promotion);
          if (addr == 0 || parent_addr == 0) {
            break;
          }
          SIDLE_CHECK(tree_op_.is_leaf_(reinterpret_cast<N*>(addr)), 
            "[art_promotion_executor] the promotion candidate should be leaf node");
          this->migrate_leaf(tree_op_.get_leaf_(reinterpret_cast<N*>(addr)),
              reinterpret_cast<P*>(parent_addr), node_mem_type::local, ti_);
        } else {
          auto addr = base::queue_.get_task(task_type::promotion);
          if (!addr) {
            break;
          }
          this->migrate_leaf(tree_op_.get_leaf_(reinterpret_cast<N*>(addr)),
              nullptr, node_mem_type::local, ti_);
        }
      }
      // if the promotion is interrupted, clear current promotion queue
      if (tree_op_.type_ == tree_type::art) {
        while (base::queue_.get_multi_task(task_type::promotion).first && 
              !base::can_promote_.load(std::memory_order_relaxed)) {}
      } else {
        while (base::queue_.get_task(task_type::promotion) && 
              !base::can_promote_.load(std::memory_order_relaxed)) {}
      }
      base::need_promotion_.store(false);
    }
  }

private:
  threadinfo *ti_{nullptr};
  tree_op_t tree_op_;
};

/// @brief the background job worker to execute demotion
template <typename T, typename P, typename H, typename N, typename... Args>
class art_demotion_executor : public art_executor_base<T, P, H, N, Args...> {
public:
  using base = art_worker_base;
  using art_executor_base = art_executor_base<T, P, H, N, Args...>;
  using tree_op_t = tree_op<T, P, H, N, Args...>;

  art_demotion_executor() = default;
  art_demotion_executor(sidle_threshold* thresholds,
    std::chrono::milliseconds interval, tree_op_t& tree_ops): 
    art_executor_base(thresholds, interval, tree_ops), tree_op_(tree_ops) {
      if (tree_op_.type_ == tree_type::masstree) {
        ti_ = threadinfo::make(threadinfo::TI_MIGRATION, -1);
      }
  }
  ~art_demotion_executor() = default;

  void run_job() override {
    while (base::is_running_) {
      auto now = std::chrono::system_clock::now();
      std::unique_lock<std::mutex> lock(base::demotion_mtx_);
      if (!base::demotion_trigger_cv_.wait_until(
          lock, now + base::interval_,
          []() { return base::need_demotion_.load(); })) {
        base::need_demotion_.store(true);
      }

      // SIDLE_RECORD(WORKER_DEBUG, "[art_demotion_executor] wakeup\n");
      // flag to indicate that demotion executor's job is running
      base::is_demoting_.store(true);
      while (base::can_demote_.load(std::memory_order_relaxed) && 
            base::is_running_) {
        if (tree_op_.type_ == tree_type::art) {
          // get the leaf node address and parent node address
          auto [addr, parent_addr] = base::queue_.get_multi_task(task_type::demotion);
          if (!addr) {
            break;
          }
          // internode, migrate directly
          if (parent_addr == 0) {
            // SIDLE_RECORD(WORKER_DEBUG, 
            //   "[art_demotion_executor] new internode demotion candidate: %p\n", addr);
            this->migrate_internode(reinterpret_cast<P*>(addr), 
                              node_mem_type::remote, 0);
            continue;
          }
          // SIDLE_RECORD(WORKER_DEBUG, 
          //   "[art_demotion_executor] new demotion candidate: %p, parent node: %p\n", 
          //   addr, parent_addr);
          T* cur = tree_op_.get_leaf_(reinterpret_cast<N*>(addr));
          // T* cur = get_leaf(reinterpret_cast<char*>(addr));
          if constexpr (sizeof...(Args) == 0) {
            if (cur->sidle_meta.metadata.type == node_mem_type::remote) {
            find_first_local_ancestor(reinterpret_cast<P*>(parent_addr), 
                                      reinterpret_cast<P*>(cur));
            } else {
              this->migrate_leaf(cur,
              reinterpret_cast<P*>(parent_addr), node_mem_type::remote, ti_);
            }
          }
        } else {
          auto addr = base::queue_.get_task(task_type::demotion);
          if (!addr) {
            break;
          }
          auto node = reinterpret_cast<N*>(addr);
          if (tree_op_.is_leaf_(node)) {
            T* cur = tree_op_.get_leaf_(node);
            this->migrate_leaf(cur, nullptr, node_mem_type::remote, ti_);
          } else {
            this->migrate_internode(reinterpret_cast<P*>(node),
                            node_mem_type::remote, ti_, 0);
          }
        }
      }
      base::need_demotion_.store(false);
      base::is_demoting_.store(false);
      // if the demotion is interrupted, clear current demotion queue
      while (base::queue_.get_multi_task(task_type::demotion).first && 
            !base::can_demote_.load(std::memory_order_relaxed)) {}
      // clear the demotion map
      demotion_map_.clear();
      // After the demotion is over, the threshold adjuster should be notified
      // to continue execution.
      {
        std::lock_guard<std::mutex> lock(base::demotion_complete_mtx_);
        base::demoting_complete_.store(true);
        base::demotion_complete_cv_.notify_all();
      }
    }
  }

private:
  void find_first_local_ancestor(P* node, P* child) {
    if (!node) {
      return;
    }
    uint64_t v = art::art_node_get_version(node);
    if (art::art_node_version_is_old(v)) {
      demotion_map_.erase(reinterpret_cast<uint64_t>(node));
      return;
    }
    int child_count = get_count(v);
    if (node->sidle_meta.type == node_mem_type::local) {
      auto it = demotion_map_.find(reinterpret_cast<uint64_t>(node));
      if (it == demotion_map_.end()) {
        demotion_map_[reinterpret_cast<uint64_t>(node)].insert(reinterpret_cast<uint64_t>(child));
        it = demotion_map_.find(reinterpret_cast<uint64_t>(node));
      } else {
        it->second.insert(reinterpret_cast<uint64_t>(child));
      }
      if (it->second.size() == child_count) {
        // SIDLE_RECORD(WORKER_DEBUG, "[find_first_local_ancestor] the internode %p can be demoted, depth: %d\n", node, node->sidle_meta.depth);
        this->migrate_internode(node, node_mem_type::remote, 0);
        demotion_map_.erase(it);
      }
      return;
    }
    find_first_local_ancestor(node->parent, node);
  }

  /// @brief record the internodes that may need to be demoted
  // the internode is the first local node for all of its children
  /// key: the target node's address 
  /// value: the cold remote children (second)
  // if first == second, the internode can be demoted
  /// @note this is a special optimization for the art, since all art leaf node
  // are initialized as remote node
  std::unordered_map<uint64_t, std::unordered_set<uint64_t>> demotion_map_;
  tree_op_t tree_op_;
  threadinfo* ti_{nullptr};
};

/// @brief the background worker to halve all nodes' access time periodically
/// @note here all operation is lock-free, I think race problem for updating
/// node is not a big problem
/// @tparam T is the leaf node type, P is the internal node type, H is the tree type
template <typename T, typename P, typename H, typename N, typename... Args>
class art_cooler : public art_worker_base {
public:
  using base = art_worker_base;
  using histogram_ptr_t = std::shared_ptr<sidle_histogram>;
  using tree_op_t = tree_op<T, P, H, N, Args...>;

  art_cooler() = default;
  art_cooler(std::chrono::milliseconds interval, H* tree,
        histogram_ptr_t histogram, tree_op_t& tree_ops): 
        base(interval), table_(tree), histogram_(histogram), 
        tree_op_(tree_ops) {
    if (tree_op_.type_ == tree_type::masstree) {
      ti_ = threadinfo::make(threadinfo::TI_MIGRATION, -1);
    }
  }
  ~art_cooler() = default;

  void run_job() override {
    while (base::is_running_) {
      std::this_thread::sleep_for(base::interval_);

      // notify the trigger that the art_cooler is running
      histogram_->notify_cooling();
      if constexpr (sizeof...(Args) > 0) {
        tree_op_.traverse_func_(table_, 
        // art::adaptive_radix_tree_traverse_mt(table_, 
        [this](P* parent, T* cur) {
          this->cool_down_cb(parent, cur);
        }, ti_);
      } else {
        tree_op_.traverse_func_(table_, 
        // art::adaptive_radix_tree_traverse_mt(table_, 
        [this](P* parent, T* cur) {
          this->cool_down_cb(parent, cur);
        });
      }
      // update the histogram info
      histogram_->adjust_for_cooling();
    }
  }

private:
  void cool_down_cb(P* /* parent */, T* cur) {
    SIDLE_CHECK(cur != nullptr,
      "[DEBUG] [cool_down_cb] the leaf node shouldn't be nullptr");
    cur->sidle_meta.access_time = (cur->sidle_meta.access_time >> 1);
  }

  H* table_;
  histogram_ptr_t histogram_;
  tree_op_t tree_op_;
  threadinfo *ti_{nullptr};
};

/// @brief adjust the threshold according to the watermark, and schedule other
// workers
class art_threshold_adjuster : public art_worker_base {
public:
  using base = art_worker_base;
  using histogram_ptr_t = std::shared_ptr<sidle_histogram>;
  using strategy_t = sidle_strategy;
  using threshold_t = sidle_threshold;

  art_threshold_adjuster() = default;
  art_threshold_adjuster(std::chrono::milliseconds interval, histogram_ptr_t histogram) : 
    base(interval), histogram_(histogram), strategy_(&strategy_manager) {}

  void run_job() override {
    static threshold_t* threshold_manager = strategy_->get_threshold_manager();

    auto handle_local_mem_shortage = [&](mem_usage_status status) {
      // 1. stop and block the promotion executor
      base::can_promote_.store(false, std::memory_order_relaxed);
      // 2. decrease the tolerance for cold to accelerate the migration
      threshold_manager->adjust_local_allocation_threshold(true);
      threshold_manager->adjust_demotion_depth_threshold(true);
#ifdef WORKER_DEBUG
      uint8_t cur_demotion_threshold = threshold_manager->get_demotion_depth_threshold();
      uint8_t cur_allocation_threshold = threshold_manager->get_local_allocation_layer_threshold();
      SIDLE_RECORD(WORKER_DEBUG, "[art_threshold_adjuster] the demotion depth threshold is %d, the local allocation threshold is %d\n",
        cur_demotion_threshold, cur_allocation_threshold);
#endif
      int times = 0;
      while (times < default_threshold_adjust_times) {
        // 3. interrupt the demotion executor and migration trigger's execution
        base::can_trigger_.store(false, std::memory_order_relaxed);
        base::can_demote_.store(false, std::memory_order_relaxed);
        ++times;
        // 4. decrease the hot threshold and cold threshold to allow more nodes
        // to be identified as cold nodes and less nodes to be identified as 
        // hot nodes
        threshold_manager->adjust_hotness_watermark(false, false);
        histogram_->adjust_threshold();
        
        // 5. assert the demotion executor is not running and the migration trigger
        // is not running 
        while (base::is_demoting_.load() && base::is_running_) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // 6. notify the migration trigger to continue execution
        base::can_trigger_.store(true, std::memory_order_relaxed);
        base::can_demote_.store(true, std::memory_order_relaxed);

        // 7. wake up the migration trigger to start the migration and demotion
        // immediately
        {
          std::lock_guard<std::mutex> lock(base::trigger_wakeup_mtx_);
          base::is_triggering_migration_.store(true);
          base::trigger_wakeup_cv_.notify_all();
        }

        // 8. wait for the demotion executor to finish
        {
          std::unique_lock<std::mutex> lock(base::demotion_complete_mtx_);
          base::demoting_complete_.store(false);
          auto now = std::chrono::system_clock::now();
          // NOTE： for masstree, the internal is 2000
          if (!base::demotion_complete_cv_.wait_until(
              lock, now + std::chrono::milliseconds(10000),
              []() { return base::demoting_complete_.load();})) {}

        }

        // 9. check the memory usage rate again, if the memory is still tight,
        // continue to adjust the threshold
        status = strategy_->check_memory_usage();
        if (status != mem_usage_status::tight) {
          SIDLE_RECORD(WORKER_DEBUG, "[art_threshold_adjuster] the memory usage status from tight return to %d\n", status);
          break;
        }
      }

      threshold_manager->adjust_local_allocation_threshold(false);
      threshold_manager->adjust_demotion_depth_threshold(false);
      threshold_manager->adjust_hotness_watermark(false, false);
      
      // restore the execution of the promotion executor
      base::can_promote_.store(true, std::memory_order_relaxed);
    };

    auto handle_local_mem_sufficient = [&](mem_usage_status status) {
      threshold_manager->adjust_local_allocation_threshold(false);
      threshold_manager->adjust_demotion_depth_threshold(false);
      threshold_manager->adjust_hotness_watermark(false, true);
    };

    while (base::is_running_) {
      {
        std::unique_lock<std::mutex> lock(base::adjuster_wakeup_mtx_);
        if (!base::adjuster_wakeup_cv_.wait_until(
            lock, std::chrono::system_clock::now() + base::interval_,
            []() { return base::need_adjust_threshold_.load(); })) {
          base::need_adjust_threshold_.store(true);
        }
      }

      mem_usage_status status = strategy_->check_memory_usage();
      SIDLE_RECORD(WORKER_DEBUG, "[art_threshold_adjuster] wakeup, the memory usage status is %d\n", status);
      switch (status) {
      case mem_usage_status::tight:
        handle_local_mem_shortage(status);
        break;
      case mem_usage_status::sufficient:
        handle_local_mem_sufficient(status);
        break;
      case mem_usage_status::normal:
        threshold_manager->adjust_hotness_watermark(false, false);
        break;
      default:
        throw std::runtime_error("[art_threshold_adjuster] invalid memory usage status");
      }
      base::need_adjust_threshold_.store(false);
    }
  }

private:
  histogram_ptr_t histogram_;
  strategy_t* strategy_;
};
} // migration

#endif // SIDLE_WORKER_HH