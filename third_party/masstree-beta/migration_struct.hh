// #ifndef MIGRATION_WORKER_BASE_HH
// #define MIGRATION_WORKER_BASE_HH


// #include <unistd.h>

// #include <atomic>
// #include <chrono>
// #include <condition_variable>
// #include <iostream>
// #include <mutex>
// #include <thread>
// #include <vector>

// #include "btree_leaflink.hh"
// #include "kvthread.hh"
// #include "masstree.hh"
// #include "masstree_scan.hh"
// #include "query_masstree.hh"
// #include "sidle_meta.hh"


// namespace sidle {

// /// @brief node_metadata is record metadata for access tracking
// struct node_metadata {
//     node_mem_type type;      
//     uint8_t depth;
//     node_metadata(node_mem_type type = node_mem_type::unknown, uint8_t depth = 0): type(type), depth(depth) {}

//     node_mem_type get_type() const {
//         return type;
//     }
// };

// struct leaf_metadata {
//     node_metadata metadata;
//     uint16_t access_time;

//     leaf_metadata(node_mem_type type = node_mem_type::unknown, uint8_t depth = 0, uint16_t access_time = 0): metadata(type, depth), access_time(access_time) {}

//     node_mem_type get_type() const {
//         return metadata.get_type();
//     }
// };

// using node_metadata_ptr = std::shared_ptr<node_metadata>;
// using node_metadata_pair = std::pair<uint64_t, node_metadata_ptr>;

// template <typename P, typename T> class naive_strategy;

// /// @brief the background job worker for migration
// // P is migration parameter, T is masstree node parameter
// template<typename P, typename T>
// class worker_base {
// public:
//     worker_base() : interval_(std::chrono::milliseconds(200)) {
//         is_running_.store(true, std::memory_order_relaxed);
//     }
//     explicit worker_base(std::chrono::milliseconds interval): interval_(interval) {
//         is_running_.store(true, std::memory_order_relaxed);
//     }
//     virtual ~worker_base() {}
//     virtual void run_job() = 0;
//     void stop() {
//         is_running_ = false;
//     }

// protected:
//     static migration_queue queue_;
//     /// @brief the time interval between running jobs
//     const std::chrono::milliseconds interval_;
//     static std::atomic<bool> need_demotion_;
//     static std::atomic<bool> need_promotion_;
//     static std::atomic<bool> can_trigger_;
//     static std::atomic<bool> can_promote_;
//     static std::atomic<bool> can_demote_;
//     static std::atomic<bool> is_demoting_;
//     static std::atomic<bool> is_triggering_migration_;
//     static std::atomic<bool> need_adjust_threshold_;
//     static std::atomic<bool> demoting_complete_;
//     /// @brief the condition variable to notify the migration trigger
//     static std::condition_variable trigger_wakeup_cv_;
//     /// @brief the condition variable to notify the promotion executor
//     static std::condition_variable promotion_trigger_cv_;
//     /// @brief the condition variable to notify the demotion executor
//     static std::condition_variable demotion_trigger_cv_;
//     /// @brief the condition variable to notify the threshold adjuster that the demotion is complete
//     static std::condition_variable demotion_complete_cv_;
//     /// @brief the condition variable to notify the threshold adjuster
//     static std::condition_variable adjuster_wakeup_cv_;
//     static std::mutex trigger_wakeup_mtx_;
//     static std::mutex demotion_mtx_;
//     static std::mutex promotion_mtx_;
//     static std::mutex demotion_complete_mtx_;
//     static std::mutex adjuster_wakeup_mtx_;
//     static std::atomic<bool> is_running_;

//     using base_type =  Masstree::node_base<T>;
//     using leaf_type = Masstree::leaf<T>;
//     using internode_type = Masstree::internode<T>;

//     /// @brief get parent node address and metadat according to child address info, 
//     /// @note here not acquire any lock
//     node_metadata_pair get_parent_info(uint64_t child_address) {
//         base_type* cur_node = reinterpret_cast<base_type*>(child_address);
//         if (cur_node->deleted()) {
//             return std::make_pair(0, nullptr);
//         }
//         auto parent_node = cur_node->parent();
//         if (!parent_node || parent_node->deleted()) {
//             return std::make_pair(0, nullptr);
//         }
//         return std::make_pair(reinterpret_cast<uint64_t>(parent_node), parent_node->sidle_meta);
//     }

//     auto cast_node(base_type* node) {
//         if (node->isleaf()) {
//             return static_cast<leaf_type*>(node);
//         } else {
//             return static_cast<internode_type*>(node);
//         }
//     }

//     // if current node is local node, its parent cannot be remote node
//     bool check_memory_layout_invariant(node_metadata_ptr parent_metadata, node_metadata_ptr current_metadata) {
//         if (current_metadata->type == node_mem_type::local && parent_metadata->type == node_mem_type::remote) {
//             // SPDLOG_WARN("the node is a local node, its parent node is a remote node");
//             return false;
//         }
//         return true;
//     }

//     friend class naive_strategy<P, T>;
// };

// template<typename P, typename T>
// migration_queue worker_base<P, T>::queue_;
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::need_demotion_(false);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::need_promotion_(false);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::can_trigger_(true);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::can_promote_(true);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::can_demote_(true);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::is_demoting_(false);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::is_triggering_migration_(false);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::need_adjust_threshold_(false);
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::demoting_complete_(false);
// template<typename P, typename T>
// std::condition_variable worker_base<P, T>::trigger_wakeup_cv_;
// template<typename P, typename T>
// std::condition_variable worker_base<P, T>::promotion_trigger_cv_;
// template<typename P, typename T>
// std::condition_variable worker_base<P, T>::demotion_trigger_cv_;
// template<typename P, typename T>
// std::condition_variable worker_base<P, T>::demotion_complete_cv_;
// template<typename P, typename T>
// std::condition_variable worker_base<P, T>::adjuster_wakeup_cv_;
// template<typename P, typename T>
// std::mutex worker_base<P, T>::trigger_wakeup_mtx_;
// template<typename P, typename T>
// std::mutex worker_base<P, T>::demotion_mtx_;
// template<typename P, typename T>
// std::mutex worker_base<P, T>::promotion_mtx_;
// template<typename P, typename T>
// std::mutex worker_base<P, T>::demotion_complete_mtx_;
// template<typename P, typename T>
// std::mutex worker_base<P, T>::adjuster_wakeup_mtx_;
// template<typename P, typename T>
// std::atomic<bool> worker_base<P, T>::is_running_(false);

// } // namespace sidle


// #endif // MIGRATION_WORKER_BASE_HH