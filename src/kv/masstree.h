#include <cstdlib>
#include <memory>
#include <set>
#include <vector>

#include "masstree.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_sidle.hh"
#include "masstree_tcursor.hh"
// #include "migration_worker.hh"
#include "query_masstree.hh"
#include "sidle_frontend.hh"
#include "sidle_worker.hh"

#include "helper.h"

#ifdef CAL_NODE_HOTNESS
#include <fstream>
#include <vector>
#endif

#if !defined(MASSTREE_H)
#define MASSTREE_H

typedef Masstree::default_table mass_tree_t;
using worker_base_ptr_t = std::shared_ptr<sidle::art_worker_base>;
using histogram_ptr_t = std::shared_ptr<sidle::sidle_histogram>;
using mass_leaf_t = Masstree::leaf<Masstree::default_query_table_params>;
using mass_internode_t = Masstree::internode<Masstree::default_query_table_params>;
using mass_node_t = Masstree::node_base<Masstree::default_query_table_params>;
using masstree_t = Masstree::basic_table<Masstree::default_query_table_params>;
using mass_migration_trigger_t = sidle::art_migration_trigger<mass_leaf_t, mass_internode_t, masstree_t, mass_node_t, threadinfo*>;
using mass_promotion_executor_t = sidle::art_promotion_executor<mass_leaf_t, mass_internode_t, masstree_t, mass_node_t, threadinfo*>;
using mass_demotion_executor_t = sidle::art_demotion_executor<mass_leaf_t, mass_internode_t, masstree_t, mass_node_t, threadinfo*>;
using mass_cooler_t = sidle::art_cooler<mass_leaf_t, mass_internode_t, masstree_t, mass_node_t, threadinfo*>;
using mass_threshold_adjuster_t = sidle::art_threshold_adjuster;
using mass_tree_op_t = sidle::tree_op<mass_leaf_t, mass_internode_t, masstree_t, mass_node_t, threadinfo*>;


struct MigrationJob {
  sidle::worker_type type;
  std::thread thread;
};

template <typename K, typename V>
class MasstreeKV {
  static const size_t key_size = sizeof(K);
  static const size_t val_size = sizeof(V);

 public:
  typedef typename mass_tree_t::leaf_type leaf_type;
  typedef typename mass_tree_t::node_type node_type;
  MasstreeKV(threadinfo *main_ti, const int cxl_percentage, const uint64_t max_local_memory_usage);
  ~MasstreeKV();
  bool get(const K &k, V &v, threadinfo *ti, query<row_type> &q,
           const uint32_t worker_id);
  bool insert(const K &k, const V &v, threadinfo *ti, query<row_type> &q,
              const uint32_t worker_id);
  bool remove(const K &k, threadinfo *ti, query<row_type> &q,
              const uint32_t worker_id);
  bool lower_bound(K &k, V &v, threadinfo *ti, query<row_type> &q,
                   const uint32_t worker_id);
  size_t scan(const K &k_start, size_t n, std::vector<std::pair<K, V>> &result,
              threadinfo *ti, query<row_type> &q, const uint32_t worker_id);
  size_t range_scan(const K &k_start, const K &k_end,
                    std::vector<std::pair<K, V>> &result, threadinfo *ti,
                    query<row_type> &q, const uint32_t worker_id);
  void worker_enter(const uint32_t worker_id);
  void worker_exit(const uint32_t worker_id);
  void stop();

  /// @brief init the background workers to do the job related to migration
  /// @note there is one worker for triggering migration, one worker for executing promotion, one worker for executing demotion and one worker for cooling 
  void init_migration_worker(int bg_worker_start_tid, int basic_worker_wakeup_interval, 
    int cooler_wakeup_interval, int threshold_adjuster_wakeup_interval, uint64_t hot_percentage_lower_bound);

  /// @brief function for tpc-c
  void start_bg();
  void terminate_bg();

 private:
  mass_tree_t mass_tree;
  std::vector<worker_base_ptr_t> background_workers;
  std::vector<std::thread> background_jobs;
  threadinfo *main_ti;
};

template <typename K, typename V>
MasstreeKV<K, V>::MasstreeKV(threadinfo *main_ti, const int cxl_percentage, const uint64_t max_local_memory_usage) : main_ti(main_ti) {
  sidle::strategy_manager = sidle::sidle_strategy(max_local_memory_usage, cxl_percentage, false);
  printf("[DEBUG] max_local_memory_usage: %lu\n", max_local_memory_usage);
  node_type::strategy_manager = &sidle::strategy_manager;
  mass_tree.initialize(*main_ti, cxl_percentage);
}

template <typename K, typename V>
void MasstreeKV<K, V>::stop() {
  for (auto &worker : background_workers) {
    if (worker != nullptr) {
      worker->stop();
    }
  }
  // wait for a while to let the background workers finish their job
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  for (auto &job : background_jobs) {
    job.join();
  }
  background_jobs.clear();
  background_workers.clear();
}

template <typename K, typename V>
MasstreeKV<K, V>::~MasstreeKV() {
#ifdef CAL_NODE_HOTNESS
  std::unordered_map<uint64_t, std::vector<uint16_t>> hotness;
  std::string hotness_file_name("masstree_hotness.csv");
  std::ofstream out(hotness_file_name);
  if (!out.is_open()) {
    std::cerr << "Failed to open hotness file: " << hotness_file_name << std::endl;
    return;
  }
  auto node_hotness = node_type::hotness_map_;
  for (auto & [addr, access_times] : node_hotness) {
    uint64_t page_id = addr >> 12;
    hotness[page_id].emplace_back(access_times);
  }
  std::vector<std::vector<uint16_t>> hotness_summary;
  hotness.reserve(hotness.size());
  for (auto& [page_id, access_times] : hotness) {
    uint64_t sum = std::accumulate(access_times.begin(), access_times.end(), 0);
    access_times.insert(access_times.begin(), sum);
    hotness_summary.emplace_back(access_times);
  }
  std::sort(hotness_summary.begin(), hotness_summary.end(),
            [](const std::vector<uint16_t>& a, const std::vector<uint16_t>& b) {
              return a[0] > b[0];
            });
  for (const auto& access_times : hotness_summary) {
    for (const auto& access_time : access_times) {
      out << access_time << ",";
    }
    out << std::endl;
  }
  out.close();
#endif
  stop();
}

template <typename K, typename V>
bool MasstreeKV<K, V>::get(const K &k, V &v, threadinfo *ti, query<row_type> &q,
                           const uint32_t worker_id) {
  K str_k = k.to_str_key();
  Str key((char *)&str_k, key_size);
  Str val_str;
  bool got = q.run_get1(mass_tree.table(), key, 0, val_str, *ti);
  if (got) {
    try {
      v = *((V *)val_str.s);
    } catch (const std::exception &e) {
      std::cerr << "Exception in MasstreeKV::get: " << e.what() << std::endl;
    }
    return true;
  }
  return false;
}

template <typename K, typename V>
bool MasstreeKV<K, V>::insert(const K &k, const V &v, threadinfo *ti,
                              query<row_type> &q, const uint32_t worker_id) {
  K str_k = k.to_str_key();
  Str key((char *)&str_k, key_size);
  Str val((char *)&v, val_size);
 result_t res = q.run_replace(mass_tree.table(), key, val, *ti);
  assert(res == Inserted || res == Updated);
  return res == Inserted || res == Updated;
}

template <typename K, typename V>
bool MasstreeKV<K, V>::remove(const K &k, threadinfo *ti, query<row_type> &q,
                              const uint32_t worker_id) {
  K str_k = k.to_str_key();
  Str key((char *)&str_k, key_size);
  q.run_remove(mass_tree.table(), key, *ti);
  return true;
}

template <typename K, typename V>
bool MasstreeKV<K, V>::lower_bound(K &k, V &v, threadinfo *ti,
                                   query<row_type> &q,
                                   const uint32_t worker_id) {
  throw std::runtime_error("MasstreeKV::lower_bound not implemented");
  return false;
}

template <typename K, typename V>
size_t MasstreeKV<K, V>::scan(const K &k_start, size_t n,
                              std::vector<std::pair<K, V>> &result,
                              threadinfo *ti, query<row_type> &q,
                              const uint32_t worker_id) {
  K str_k = k_start.to_str_key();
  Str first_key((char *)&str_k, key_size);
  lcdf::Json req = lcdf::Json::array(0, 0, first_key, n);
  q.run_scan(mass_tree.table(), req, *ti);
  assert(req.size() >= 2);

  for (int i = 2; i < req.size(); i += 2) {
    result.emplace_back(((K *)req[i].as_s().data())->to_normal_key(),
                        *(V *)req[i + 1].as_s().data());
  }
  return result.size();
}

template <typename K, typename V>
size_t MasstreeKV<K, V>::range_scan(const K &k_start, const K &k_end,
                                    std::vector<std::pair<K, V>> &result,
                                    threadinfo *ti, query<row_type> &q,
                                    const uint32_t worker_id) {
  throw std::runtime_error("MasstreeKV::range_scan not implemented");
  return 0;
}

template <typename K, typename V>
void MasstreeKV<K, V>::worker_enter(const uint32_t worker_id) {}

template <typename K, typename V>
void MasstreeKV<K, V>::worker_exit(const uint32_t worker_id) {}

template <typename K, typename V>
void MasstreeKV<K, V>::init_migration_worker(int bg_worker_start_tid, int basic_worker_wakeup_interval, int cooler_wakeup_interval, int threshold_adjuster_wakeup_interval, uint64_t hot_percentage_lower_bound) {
  // prepare the histogram
  sidle::sidle_threshold* thresholds = sidle::strategy_manager.get_threshold_manager();
  thresholds->set_hotness_watermarks(hot_percentage_lower_bound);
  histogram_ptr_t histogram = std::make_shared<sidle::sidle_histogram>(thresholds);
  background_workers.clear();
  int worker_count = 5;
  mass_tree_op_t masstree_ops = mass_tree_op_t {
    .traverse_func_ = Masstree::masstree_leaf_traverse<Masstree::default_query_table_params, threadinfo*>,
    .leaf_migration_ = Masstree::masstree_leaf_migration<Masstree::default_query_table_params, threadinfo*>,
    .internode_migration_ = Masstree::masstree_internode_migration<Masstree::default_query_table_params, threadinfo*>,
    .unlock_ = Masstree::masstree_unlock<Masstree::default_query_table_params>,
    .node_traverse_func_ = Masstree::masstree_internode_traverse<Masstree::default_query_table_params>,
    .get_leaf_ = Masstree::masstree_get_leaf<Masstree::default_query_table_params>,
    .is_leaf_ = Masstree::masstree_is_leaf<Masstree::default_query_table_params>,
    .type_ = sidle::tree_type::masstree,
  };
#ifdef WATERMARK_RECORD
#ifdef WATERMARK_TEST
  worker_count = 6;
#endif
#endif
  printf("[DEBUG] basic_worker_wakeup_interval: %d, cooler_wakeup_interval: %d, threshold_adjuster_wakeup_interval: %d\n", basic_worker_wakeup_interval, cooler_wakeup_interval, threshold_adjuster_wakeup_interval);
  background_workers.resize(worker_count);
  background_workers[0] = std::make_shared<mass_migration_trigger_t>(
  std::chrono::milliseconds(basic_worker_wakeup_interval), &mass_tree.table(), histogram, masstree_ops);
  background_workers[1] = std::make_shared<mass_promotion_executor_t>(
    thresholds, std::chrono::milliseconds(basic_worker_wakeup_interval), masstree_ops);
  background_workers[2] = std::make_shared<mass_demotion_executor_t>(
    thresholds, std::chrono::milliseconds(basic_worker_wakeup_interval), masstree_ops);
  background_workers[3] = std::make_shared<mass_cooler_t>(
    std::chrono::milliseconds(cooler_wakeup_interval), &mass_tree.table(), histogram, masstree_ops);
  background_workers[4] = std::make_shared<mass_threshold_adjuster_t>(
    std::chrono::milliseconds(threshold_adjuster_wakeup_interval), histogram);

#ifdef WATERMARK_RECORD
#ifdef WATERMARK_TEST
  background_workers[5] = std::make_shared<sidle::default_memory_usage_detector>(std::chrono::milliseconds(100), strategy);
#endif
#endif

  background_jobs.reserve(5);
  for (int i = 0; i < worker_count; ++i) {
    if (background_workers[i] == nullptr) {
      continue;
    }
    background_jobs.emplace_back(std::thread([&, i, bg_worker_start_tid]() {
      try {
        printf("background worker %d start, tid: %d\n", i, bg_worker_start_tid + i);
        bind_to_physical_cores(bg_worker_start_tid + i);
        background_workers[i]->run_job();
      } catch (const std::exception& e) {
        // handle the exception
        fprintf(stderr, "Exception in background worker %d: %s\n", i, e.what());
      }
    }));
  }
  // fprintf(stderr, "[DEBUG] end init background workers\n");
}

template <typename K, typename V>
void MasstreeKV<K, V>::start_bg() {}

template <typename K, typename V>
void MasstreeKV<K, V>::terminate_bg() {}

#endif  // MASSTREE_H
