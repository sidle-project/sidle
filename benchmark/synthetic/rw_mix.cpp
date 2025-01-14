#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "helper.h"
#include "micro_common.h"

#ifdef RO_LATENCY
std::vector<uint64_t> latency_cycles;
#endif
struct alignas(CACHELINE_SIZE) FGParam {
  void *table;
  uint64_t throughput;
  uint32_t thread_id;
  uint64_t read_cycles;
  uint64_t read_cnt;
  uint64_t insert_cycles;
  uint64_t insert_cnt;
  uint64_t remove_cycles;
  uint64_t remove_cnt;
  uint64_t inplace_cycles;
  uint64_t inplace_cnt;
};

template <class tab_t>
void debug_info(tab_t *table, size_t group_i) {}

template <class tab_t, bool stat_latency, size_t sample_rate>
void *run_fg(void *param) {
  FGParam &thread_param = *(FGParam *)param;
  uint32_t thread_id = thread_param.thread_id;
  tab_t *table = (tab_t *)thread_param.table;
  thread_param.throughput = 0;
  bind_to_physical_cores(thread_id);

  FastRandom r(thread_id);
  threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  query<row_type> q;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);

  size_t exist_key_n_per_thread = all_keys.size() / fg_n;
  size_t exist_key_start = thread_id * exist_key_n_per_thread;
  size_t exist_key_end = (thread_id + 1) * exist_key_n_per_thread;

  std::vector<index_key_t> query_keys(all_keys.begin() + exist_key_start,
                                      all_keys.begin() + exist_key_end);
  // std::vector<index_key_t> insert_keys;
  // std::vector<index_key_t> delete_keys;

  INVARIANT(read_ratio == 1 || non_exist_keys.size() != 0);
  // if (read_ratio != 1) {
  size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
  size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
         non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;

  query_keys.insert(query_keys.end(),
                    non_exist_keys.begin() + non_exist_key_start,
                    non_exist_keys.begin() + non_exist_key_end);
  // insert_keys.insert(insert_keys.end(), query_keys.begin(),
  // query_keys.end()); delete_keys.insert(delete_keys.end(),
  // query_keys.begin(), query_keys.end());

  // unsigned seed =
  // std::chrono::system_clock::now().time_since_epoch().count();
  // std::shuffle(insert_keys.begin(), insert_keys.end(),
  //              std::default_random_engine(seed));
  // seed = std::chrono::system_clock::now().time_since_epoch().count();
  // std::shuffle(delete_keys.begin(), delete_keys.end(),
  //              std::default_random_engine(seed));
  // }

  COUT_THIS("worker " << thread_id << " ready!");
  fg_prologue(table, thread_id);
  COUT_THIS("-------Worker" << thread_id << " Start Warmup");
  ready_threads++;
  uint64_t t1 = util_rdtsc();

  // warm up
  size_t query_i = 0, insert_i = query_keys.size() / 2, delete_i = 0,
         update_i = 0;

  while (!running) {
    double d = ratio_dis(gen);
    if (d <= read_ratio) {  // get
      Value v{3333};
      volatile bool b =
          table->get(query_keys[(query_i + delete_i) % query_keys.size()], v,
                     ti, q, thread_id);
      INVARIANT((read_ratio == 1 && b) || read_ratio < 1);
      query_i++;
      if (unlikely(query_i == query_keys.size() / 2)) {
        query_i = 0;
      }
    } else if (d <= read_ratio + inplace_ratio) {  // inpace update
      Value v{12345};
      bool res =
          table->insert(query_keys[(update_i + delete_i) % query_keys.size()],
                        v, ti, q, thread_id);
      UNUSED(res);
      update_i++;
      if (unlikely(update_i == query_keys.size() / 2)) {
        update_i = 0;
      }
    } else if (d <= read_ratio + inplace_ratio + insert_ratio +
                        delete_ratio) {  // insert + remove
      Value v{12345};
      bool res = table->insert(query_keys[insert_i], v, ti, q, thread_id);
      UNUSED(res);
      res = table->remove(query_keys[delete_i], ti, q, thread_id);
      UNUSED(res);
      insert_i++;
      delete_i++;
      if (unlikely(insert_i == query_keys.size())) {
        insert_i = 0;
      }
      if (unlikely(delete_i == query_keys.size())) {
        delete_i = 0;
      }
    } else {  // scan
      // std::vector<std::pair<index_key_t, Value>> res;
      // bool res = table->scan(k, 100, res, ti, q, 0);
    }

    if (util_rdtsc() - t1 > 10 * CPU_CYCLE_PER_SEC) {
      t1 = util_rdtsc();
      debug_info(table, 0);
    }
  }

  COUT_THIS("-------Worker" << thread_id << " Finish Warmup");
#ifdef IPQ_PERF
  sleep(5);
#endif

  uint64_t cnt = 1;
  while (running) {
    double d = ratio_dis(gen);
    if (d <= read_ratio) {  // get
      Value v{3333};
      if (stat_latency && thread_param.throughput % sample_rate == 0) {
        uint64_t t1 = util_rdtsc();
#ifdef RO_LATENCY
        volatile bool b =
            table->get(query_keys[(query_i + delete_i) % query_keys.size()], v,
                       ti, q, 1234);
#else
        volatile bool b =
            table->get(query_keys[(query_i + delete_i) % query_keys.size()], v,
                       ti, q, thread_id);
#endif
        cnt += (b && v.val > 5000000) ? 1 : 0;
#ifdef RO_LATENCY
        latency_cycles.push_back(util_rdtsc() - t1);
#else
        thread_param.read_cycles += (util_rdtsc() - t1);
        thread_param.read_cnt++;
#endif

      } else {
#ifdef IPQ_PERF
        rdtscp();
#endif
        volatile bool b =
            table->get(query_keys[(query_i + delete_i) % query_keys.size()], v,
                       ti, q, thread_id);
#ifdef IPQ_PERF
        rdtscp();
#endif
        cnt += (b && v.val > 5000000) ? 1 : 0;
      }

      query_i++;
      if (unlikely(query_i == query_keys.size() / 2)) {
        query_i = 0;
      }

      thread_param.throughput++;
    } else if (d <= read_ratio + insert_ratio) {  // inpace update
      Value v{12345};
      if (stat_latency && thread_param.throughput % sample_rate == 0) {
        uint64_t t1 = util_rdtsc();
        bool res =
            table->insert(query_keys[(update_i + delete_i) % query_keys.size()],
                          v, ti, q, thread_id);
        thread_param.inplace_cycles += (util_rdtsc() - t1);
        thread_param.inplace_cnt++;
        UNUSED(res);
      } else {
        bool res =
            table->insert(query_keys[(update_i + delete_i) % query_keys.size()],
                          v, ti, q, thread_id);
        UNUSED(res);
      }
      update_i++;
      if (unlikely(update_i == query_keys.size() / 2)) {
        update_i = 0;
      }

      thread_param.throughput++;
    } else if (d <= read_ratio + inplace_ratio + insert_ratio +
                        delete_ratio) {  // remove + insert
      Value v{12345};
      if (stat_latency && thread_param.throughput % sample_rate == 0) {
        uint64_t t1 = util_rdtsc();
        volatile bool res =
            table->insert(query_keys[insert_i], v, ti, q, thread_id);
        uint64_t t2 = util_rdtsc();
        res = table->remove(query_keys[delete_i], ti, q, thread_id);
        UNUSED(res);
        thread_param.remove_cycles += (util_rdtsc() - t2);
        thread_param.remove_cnt++;
        thread_param.insert_cycles += (t2 - t1);
        thread_param.insert_cnt++;
      } else {
        volatile bool res =
            table->insert(query_keys[insert_i], v, ti, q, thread_id);
        res = table->remove(query_keys[delete_i], ti, q, thread_id);
        UNUSED(res);
      }
      insert_i++;
      delete_i++;
      if (unlikely(insert_i == query_keys.size())) {
        insert_i = 0;
      }
      if (unlikely(delete_i == query_keys.size())) {
        delete_i = 0;
      }

      thread_param.throughput += 2;
    } else {  // scan
      std::vector<std::pair<index_key_t, Value>> res;
      table->scan(query_keys[query_i], 100, res, ti, q, 0);
      query_i++;
      if (unlikely(query_i == query_keys.size())) {
        query_i = 0;
      }

      thread_param.throughput++;
    }
  }

  COUT_VAR(cnt);
  fg_epilogue(table, thread_id);

  pthread_exit(nullptr);
}

template <class tab_t>
void run_benchmark(tab_t *table, size_t sec, threadinfo *main_ti) {
  pthread_t threads[fg_n];
  FGParam fg_params[fg_n];
  // check if parameters are cacheline aligned
  for (size_t i = 0; i < fg_n; i++) {
    if ((uint64_t)(&(fg_params[i])) % CACHELINE_SIZE != 0) {
      COUT_N_EXIT("wrong parameter address: " << &(fg_params[i]));
    }
  }

  // 1. prepare keys
  if (hot_data_ratio != 0) {
    generate_skew_workload(hot_data_start, hot_data_ratio, hot_query_ratio,
                           all_keys);
    if (read_ratio < 1) {
      generate_skew_workload(hot_data_start, hot_data_ratio, hot_query_ratio,
                             non_exist_keys);
    }
    COUT_THIS("done generate skewed workload using hot range");
  } else if (zipfian_theta != -1) {
    generate_skew_workload(zipfian_theta, all_keys);
    if (read_ratio < 1) {
      generate_skew_workload(zipfian_theta, non_exist_keys);
    }
    COUT_THIS("done generate skewed workload using zipfian. zipf_theta="
              << zipfian_theta);
  } else {
    std::shuffle(all_keys.begin(), all_keys.end(),
                 std::default_random_engine());
    COUT_THIS("done shuffling all_keys");
    std::shuffle(non_exist_keys.begin(), non_exist_keys.end(),
                 std::default_random_engine());
    COUT_THIS("done shuffling non_exist_keys");
  }
  timeit();

  // 2. create foreground thread
  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].read_cycles = 0;
    fg_params[worker_i].read_cnt = 0;
    fg_params[worker_i].insert_cycles = 0;
    fg_params[worker_i].insert_cnt = 0;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].remove_cycles = 0;
    fg_params[worker_i].remove_cnt = 0;
    fg_params[worker_i].inplace_cycles = 0;
    fg_params[worker_i].inplace_cnt = 0;
    int ret;
    if (stat_op_latency) {
      if (sample_lat_rate == 100000) {
        ret = pthread_create(&threads[worker_i], nullptr,
                             run_fg<tab_t, true, 100000>,
                             (void *)&fg_params[worker_i]);
      } else {
        ret = pthread_create(&threads[worker_i], nullptr,
                             run_fg<tab_t, true, 10000>,
                             (void *)&fg_params[worker_i]);
      }
    } else {
      ret = pthread_create(&threads[worker_i], nullptr,
                           run_fg<tab_t, false, 10000>,
                           (void *)&fg_params[worker_i]);
    }
    if (ret) {
      COUT_N_EXIT("Error:" << ret);
    }
  }

  // 3. wait for all threads to get ready
  COUT_THIS("prepare data ...");
  while (ready_threads < fg_n) sleep(1);

  // 4. warm up
  COUT_THIS("warm up " << warmup_time << "s...");
  sleep(warmup_time);
  running = true;
  timeit();

#ifdef IPQ_PERF
  sleep(5);
#endif

  // 5. runtime info (e.g. tput)
  std::vector<size_t> tput_history(fg_n, 0);
  std::vector<std::pair<size_t, size_t>> lat_read_history(fg_n);
  std::vector<std::pair<size_t, size_t>> lat_insert_history(fg_n);
  std::vector<std::pair<size_t, size_t>> lat_remove_history(fg_n);
  std::vector<std::pair<size_t, size_t>> lat_inplace_history(fg_n);
  size_t current_sec = 0;
  while (current_sec < sec) {
    sleep(1);
    uint64_t tput = 0;
    for (size_t i = 0; i < fg_n; i++) {
      tput += fg_params[i].throughput - tput_history[i];
      tput_history[i] = fg_params[i].throughput;
    }

    uint64_t read_cnt = 0;
    uint64_t read_cycles = 0;
    for (size_t i = 0; i < fg_n; i++) {
      read_cycles += fg_params[i].read_cycles - lat_read_history[i].first;
      read_cnt += fg_params[i].read_cnt - lat_read_history[i].second;
      lat_read_history[i].first = fg_params[i].read_cycles;
      lat_read_history[i].second = fg_params[i].read_cnt;
    }
    uint64_t insert_cnt = 0;
    uint64_t insert_cycles = 0;
    for (size_t i = 0; i < fg_n; i++) {
      insert_cycles += fg_params[i].insert_cycles - lat_insert_history[i].first;
      insert_cnt += fg_params[i].insert_cnt - lat_insert_history[i].second;
      lat_insert_history[i].first = fg_params[i].insert_cycles;
      lat_insert_history[i].second = fg_params[i].insert_cnt;
    }
    uint64_t remove_cnt = 0;
    uint64_t remove_cycles = 0;
    for (size_t i = 0; i < fg_n; i++) {
      remove_cycles += fg_params[i].remove_cycles - lat_remove_history[i].first;
      remove_cnt += fg_params[i].remove_cnt - lat_remove_history[i].second;
      lat_remove_history[i].first = fg_params[i].remove_cycles;
      lat_remove_history[i].second = fg_params[i].remove_cnt;
    }
    uint64_t inplace_cnt = 0;
    uint64_t inplace_cycles = 0;
    for (size_t i = 0; i < fg_n; i++) {
      inplace_cycles +=
          fg_params[i].inplace_cycles - lat_inplace_history[i].first;
      inplace_cnt += fg_params[i].inplace_cnt - lat_inplace_history[i].second;
      lat_inplace_history[i].first = fg_params[i].inplace_cycles;
      lat_inplace_history[i].second = fg_params[i].inplace_cnt;
    }

    COUT_THIS(">>> sec " << current_sec << " throughput: " << tput);
    if (read_cnt > 0) {
      COUT_THIS(">>> sec " << current_sec << " read_lat : "
                           << (read_cycles / read_cnt) / CPU_CYCLE_PER_NANOSEC);
    }
    if (insert_cnt > 0) {
      COUT_THIS(">>> sec " << current_sec << " insert_lat : "
                           << (insert_cycles / insert_cnt) /
                                  CPU_CYCLE_PER_NANOSEC);
    }
    if (remove_cnt > 0) {
      COUT_THIS(">>> sec " << current_sec << " remove_lat : "
                           << (remove_cycles / remove_cnt) /
                                  CPU_CYCLE_PER_NANOSEC);
    }
    if (inplace_cnt > 0) {
      COUT_THIS(">>> sec " << current_sec << " inplace_lat : "
                           << (inplace_cycles / inplace_cnt) /
                                  CPU_CYCLE_PER_NANOSEC);
    }
    ++current_sec;
  }

  // 6. thread join and exit
  running = false;
  timeit();
  void *status;
  for (size_t i = 0; i < fg_n; i++) {
    int rc = pthread_join(threads[i], &status);
    if (rc) {
      COUT_N_EXIT("Error:unable to join," << rc);
    }
  }

  // 7. benchmark result output
  size_t throughput = 0, all_read_cnt = 0, all_read_cycles = 0,
         all_insert_cycles = 0, all_insert_cnt = 0, all_remove_cycles = 0,
         all_remove_cnt = 0;
  for (auto &p : fg_params) {
    throughput += p.throughput;
    all_read_cnt += p.read_cnt;
    all_read_cycles += p.read_cycles;
    all_insert_cnt += p.insert_cnt;
    all_insert_cycles += p.insert_cycles;
    all_remove_cnt += p.remove_cnt;
    all_remove_cycles += p.remove_cycles;
  }

  COUT_THIS("[Overall] RunTime(s): " << runtime);
  if (all_read_cnt > 0) {
    COUT_THIS("[Overall] ReadLatency(ns):" << (all_read_cycles / all_read_cnt) /
                                                  CPU_CYCLE_PER_NANOSEC);
  }
  // if (all_insert_cnt > 0) {
  //   COUT_THIS("[Overall] InsertLatency(ns):"
  //             << (all_insert_cycles / all_insert_cnt) /
  //             CPU_CYCLE_PER_NANOSEC);
  // }
  // if (all_remove_cnt > 0) {
  //   COUT_THIS("[Overall] RemoveLatency(ns):"
  //             << (all_remove_cycles / all_remove_cnt) /
  //             CPU_CYCLE_PER_NANOSEC);
  // }

#ifdef RO_LATENCY
  COUT_VAR(latency_cycles.size());
  COUT_VAR(error_info.size());
  if (target == "learned") {
    for (size_t i = 0; i < latency_cycles.size(); ++i) {
      double lat = latency_cycles[i] / CPU_CYCLE_PER_NANOSEC;
      double eb = std::log2(error_info[i]);
      COUT_THIS(target << ":" << file_name << ":" << lat << ":" << error_info[i]
                       << ":" << eb);
    }
  } else if (target == "btree") {
    for (size_t i = 0; i < latency_cycles.size(); ++i) {
      double lat = latency_cycles[i] / CPU_CYCLE_PER_NANOSEC;
      COUT_THIS(target << ":" << file_name << ":" << lat);
    }
  }
#endif

  COUT_THIS("[Overall] Throughput(op/s): " << throughput / sec);

#ifdef RO_DEBUG
  if (xindex::get_n > 0) {
    double avg_predict_ns =
        xindex::predict_cycles / (xindex::get_n * CPU_CYCLE_PER_NANOSEC);
    double avg_search_ns =
        xindex::search_cycles / (xindex::get_n * CPU_CYCLE_PER_NANOSEC);
    double avg_locate_group_ns =
        xindex::locate_group_cycles / (xindex::get_n * CPU_CYCLE_PER_NANOSEC);
    COUT_VAR(avg_predict_ns);
    COUT_VAR(avg_search_ns);
    COUT_VAR(avg_locate_group_ns);
  }
#endif
}

int main(int argc, char **argv) {
#ifdef PURE_RO
  COUT_THIS("[Overall] MICRO: rw_mix_pure_ro");
#else
  COUT_THIS("[Overall] MICRO: rw_mix");
#endif

#ifdef RO_LATENCY
  stat_op_latency = true;
  latency_cycles.reserve(5000000);
  error_info.reserve(5000000);
#endif

  parse_args(argc, argv);
  // INVARIANT(read_ratio + insert_ratio + delete_ratio + scan_ratio +
  //               inplace_ratio ==
  //           1);
  double ratio_sum = read_ratio + insert_ratio + delete_ratio + scan_ratio +
                     read_modify_write_ratio + inplace_ratio;
  INVARIANT(ratio_sum > 0.9999 && ratio_sum < 1.0001);  // avoid precision lost

  FastRandom r(3333);
  prepare_data(file_name, r);
  timeit();

  start_micro();
  timeit();
}
