#ifndef SIDLE_STRUCT_HH
#define SIDLE_STRUCT_HH

#include "sidle_meta.hh"

#include <atomic>
#include <condition_variable>
#include <cstdint>


namespace sidle {
struct node_metadata {
  node_mem_type type : 2;
  uint8_t depth : 6;

  node_metadata(node_mem_type type = node_mem_type::unknown, uint8_t depth = 0): type(type), depth(depth) {}

  node_mem_type get_type() const {
    return type;
  }
};

#pragma pack(push, 1)
struct leaf_metadata {
  node_metadata metadata;
  uint16_t access_time{0};

  leaf_metadata(node_mem_type type = node_mem_type::unknown, uint8_t depth = 0, uint16_t access_time = 0): metadata(type, depth), access_time(access_time) {}

  node_mem_type get_type() const {
      return metadata.get_type();
  }
};    
#pragma pack(pop)

class sidle_strategy;

class art_worker_base {
public:
    art_worker_base() : interval_(std::chrono::milliseconds(200)) {
        is_running_.store(true, std::memory_order_relaxed);
    }
    explicit art_worker_base(std::chrono::milliseconds interval): interval_(interval) {
        is_running_.store(true, std::memory_order_relaxed);
    }
    virtual ~art_worker_base() {}
    virtual void run_job() = 0;
    void stop() {
        is_running_ = false;
    }

protected:
    static migration_queue queue_;
    /// @brief the time interval between running jobs
    const std::chrono::milliseconds interval_;
    static std::atomic<bool> need_demotion_;
    static std::atomic<bool> need_promotion_;
    static std::atomic<bool> can_trigger_;
    static std::atomic<bool> can_promote_;
    static std::atomic<bool> can_demote_;
    static std::atomic<bool> is_demoting_;
    static std::atomic<bool> is_triggering_migration_;
    static std::atomic<bool> need_adjust_threshold_;
    static std::atomic<bool> demoting_complete_;
    /// @brief the condition variable to notify the migration trigger
    static std::condition_variable trigger_wakeup_cv_;
    /// @brief the condition variable to notify the promotion executor
    static std::condition_variable promotion_trigger_cv_;
    /// @brief the condition variable to notify the demotion executor
    static std::condition_variable demotion_trigger_cv_;
    /// @brief the condition variable to notify the threshold adjuster that the demotion is complete
    static std::condition_variable demotion_complete_cv_;
    /// @brief the condition variable to notify the threshold adjuster
    static std::condition_variable adjuster_wakeup_cv_;
    static std::mutex trigger_wakeup_mtx_;
    static std::mutex demotion_mtx_;
    static std::mutex promotion_mtx_;
    static std::mutex demotion_complete_mtx_;
    static std::mutex adjuster_wakeup_mtx_;
    static std::atomic<bool> is_running_;

    friend class sidle_strategy;
};

}   // namespace sidle
#endif