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

#include "generator.h"
#include "helper.h"
#include "micro_common.h"

// #define TEST_DEBUG

struct alignas(CACHELINE_SIZE) FGParam {
  void *table;
  uint32_t thread_id;
  uint64_t throughput;
  uint64_t read_cycles;
  uint64_t read_tput;
  uint64_t insert_cycles;
  uint64_t insert_tput;
#ifdef ANALYZE_LATENCY
  std::vector<uint64_t> read_latencies;
  std::vector<uint64_t> insert_latencies;
#endif
};

template <class tab_t, bool stat_latency>
void *run_fg(void *param) {
  FGParam &thread_param = *(FGParam *)param;
  uint32_t thread_id = thread_param.thread_id;
  tab_t *table = (tab_t *)thread_param.table;
  bind_to_physical_cores(thread_id);

  threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  query<row_type> q;

  FastRandom r(thread_id);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);

  size_t exist_key_n_per_thread = all_keys.size() / fg_n;
  size_t exist_key_start = thread_id * exist_key_n_per_thread;
  size_t exist_key_end = (thread_id + 1) * exist_key_n_per_thread;

  size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
  size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
         non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;



  std::vector<index_key_t> op_keys(all_keys.begin() + exist_key_start,
                                   all_keys.begin() + exist_key_end);
  std::vector<index_key_t> candidate_keys(non_exist_keys.begin() + non_exist_key_start,
                                          non_exist_keys.begin() + non_exist_key_end);                                

  ScrambledZipfianGenerator key_generator(0, op_keys.size() - 1);

  fg_prologue(table, thread_id);
  ready_threads++;
  size_t query_i = 0, insert_i = 0;

#ifdef TEST_DEBUG
  uint64_t hot = 0;
  uint64_t counter = 0;
#endif

  uint64_t cnt = 1;
  size_t op_i = 0;
  volatile bool b = false;
  while (!running) {
    Value v{3333};
    double d = ratio_dis(gen);
    if (d <= read_ratio) {
      b = table->get(op_keys[query_i], v,
                     ti, q, thread_id); 
      ++query_i;                          
    } else {
      v.val = rand_int64(r, 123, 123456789);
      b = table->insert(candidate_keys[insert_i], v,
                        ti, q, thread_id);
      ++insert_i;
    }
    if (unlikely(query_i == op_keys.size())) {
      query_i = 0;
    }
    if (unlikely(insert_i == candidate_keys.size())) {
      insert_i = 0;
    }
  }
  COUT_THIS("-------Worker" << thread_id << " Finish Warmup");

  cnt = 1;
  uint64_t t1 = 0, t2 = 0;
  while (running) {
    double d = ratio_dis(gen);
    cnt += (d > 0.9) ? 1 : 0;

    if (op_i % 100 == 0) {
        epochinc();
        ti->rcu_quiesce();
    }
      
    Value v{3333};
    if (d <= read_ratio) {
#ifdef ANALYZE_LATENCY
      if (record_op_latency) {
        t1 = util_rdtsc();
        b = table->get(op_keys[query_i], v,
                      ti, q, thread_id); 
        t2 = util_rdtsc();
        thread_param.read_cycles += (t2 - t1);
        thread_param.read_tput++;
        thread_param.read_latencies.emplace_back(t2 - t1);
      } else {
        b = table->get(op_keys[query_i], v, ti, q, thread_id);
      }
#else
      b = table->get(op_keys[query_i], v, ti, q, thread_id);
#endif
#ifdef TEST_DEBUG
      if (op_keys[query_i] >= hot_key_lower_bound && op_keys[query_i] <= hot_key_upper_bound) {
        ++hot;
      }
      ++counter;
#endif
      cnt += (b && v.val > 333) ? 1 : 0;
      ++query_i;                        
    } else {
      v.val = rand_int64(r, 123, 123456789);
#ifdef ANALYZE_LATENCY
      if (record_op_latency) {
        t1 = util_rdtsc();
        b = table->insert(candidate_keys[insert_i], v, ti, q, thread_id);
        t2 = util_rdtsc();
        thread_param.insert_cycles += (t2 - t1);
        thread_param.insert_tput++;
        thread_param.insert_latencies.emplace_back(t2 - t1);
      } else {
        b = table->insert(candidate_keys[insert_i], v, ti, q, thread_id);
      }
#else
      b = table->insert(candidate_keys[insert_i], v, ti, q, thread_id);
#endif
#ifdef TEST_DEBUG
      if (candidate_keys[insert_i] >= hot_key_lower_bound && candidate_keys[insert_i] <= hot_key_upper_bound) {
        ++hot;
      }
      ++counter;
#endif
      ++insert_i;
    }
    op_i++;
    if (unlikely(op_i == op_keys.size())) {
      op_i = 0;
    }
    if (unlikely(query_i == op_keys.size())) {
      query_i = 0;
    }
    if (unlikely(insert_i == candidate_keys.size())) {
      insert_i = 0;
    }
    thread_param.throughput++;
  }

  COUT_VAR(cnt);
#ifdef TEST_DEBUG
  COUT_VAR(hot);
  COUT_VAR(counter);
#endif
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

  // 1. prepare data
  all_keys = skewed_keys[0];
  non_exist_keys = skewed_non_exist_keys[0];
  std::shuffle(all_keys.begin(), all_keys.end(), std::mt19937(std::random_device()()));
  std::shuffle(non_exist_keys.begin(), non_exist_keys.end(), std::mt19937(std::random_device()()));

  // 2. create foreground thread
  // set signal for masstree
  bzero((void *)timeout, sizeof(timeout));
  signal(SIGALRM, test_timeout);
  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].read_cycles = 0;
    fg_params[worker_i].read_tput = 0;
    fg_params[worker_i].insert_cycles = 0;
    fg_params[worker_i].insert_tput = 0;
    int ret;
    if (stat_op_latency) {
      ret = pthread_create(&threads[worker_i], nullptr, run_fg<tab_t, true>,
                           (void *)&fg_params[worker_i]);

    } else {
      ret = pthread_create(&threads[worker_i], nullptr, run_fg<tab_t, false>,
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

  // 5. runtime info(e.g.tput)
  std::vector<size_t> tput_history(fg_n, 0);
  size_t current_sec = 0;
  while (current_sec < sec) {
    sleep(1);
    uint64_t tput = 0;
    for (size_t i = 0; i < fg_n; i++) {
      tput += fg_params[i].throughput - tput_history[i];
      tput_history[i] = fg_params[i].throughput;
    }
    COUT_THIS(">>> sec " << current_sec << " throughput: " << tput);
    ++current_sec;
#ifdef ANALYZE_LATENCY
    if (current_sec == 20) {
      record_op_latency = true;
    } else if (current_sec == 30) {
      record_op_latency = false;
    }
#endif
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
  size_t throughput = 0, all_read_tput = 0, all_read_cycles = 0,
         all_insert_cycles = 0, all_insert_tput = 0;
  for (auto &p : fg_params) {
    throughput += p.throughput;
    all_read_tput += p.read_tput;
    all_read_cycles += p.read_cycles;
    all_insert_tput += p.insert_tput;
    all_insert_cycles += p.insert_cycles;
  }

  COUT_THIS("[Overall] RunTime(s): " << runtime);
  COUT_THIS("[Overall] Throughput(op/s): " << throughput / sec);
  if (all_read_tput > 0) {
    COUT_THIS("[Overall] ReadAvgLatency(ns): "
              << all_read_cycles / (all_read_tput * CPU_CYCLE_PER_NANOSEC));
  }
  if (all_insert_tput > 0) {
    COUT_THIS("[Overall] InsertAvgLatency(ns): "
              << all_insert_cycles / (all_insert_tput * CPU_CYCLE_PER_NANOSEC));
  }
  if (all_read_tput > 0 || all_insert_tput > 0) {
    COUT_THIS("[Overall] AvgLatency(ns): "
              << (all_read_cycles + all_insert_cycles) /
                     ((all_read_tput + all_insert_tput) * CPU_CYCLE_PER_NANOSEC));
  }
#ifdef ANALYZE_LATENCY
  std::vector<uint64_t> all_read_latencies, all_insert_latencies;
  for (auto &p : fg_params) {
    all_read_latencies.insert(all_read_latencies.end(),
                              p.read_latencies.begin(), p.read_latencies.end());
    all_insert_latencies.insert(all_insert_latencies.end(),
                                p.insert_latencies.begin(), p.insert_latencies.end());
  }
  std::sort(all_read_latencies.begin(), all_read_latencies.end());
  std::sort(all_insert_latencies.begin(), all_insert_latencies.end());
  COUT_THIS("[Overall] ReadLatencyP50(ns): "
            << all_read_latencies[all_read_latencies.size() / 2] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] ReadLatencyP90(ns): "
            << all_read_latencies[all_read_latencies.size() * 9 / 10] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] ReadLatencyP99(ns): "
            << all_read_latencies[all_read_latencies.size() * 99 / 100] / CPU_CYCLE_PER_NANOSEC);
  if (all_insert_tput > 0) {
    COUT_THIS("[Overall] InsertLatencyP50(ns): "
            << all_insert_latencies[all_insert_latencies.size() / 2] / CPU_CYCLE_PER_NANOSEC);
    COUT_THIS("[Overall] InsertLatencyP90(ns): "
            << all_insert_latencies[all_insert_latencies.size() * 9 / 10] / CPU_CYCLE_PER_NANOSEC);
    COUT_THIS("[Overall] InsertLatencyP99(ns): "
            << all_insert_latencies[all_insert_latencies.size() * 99 / 100] / CPU_CYCLE_PER_NANOSEC);
  }
#endif
}

int main(int argc, char **argv) {
#ifdef ORIGIN_YCSB
  COUT_THIS("[Overall] MICRO: rw_ycsb origin");
#else
  COUT_THIS("[Overall] MICRO: skew parition");
#endif
  parse_args(argc, argv);
  INVARIANT(read_ratio + insert_ratio == 1);

  FastRandom r(3333);
  prepare_data(file_name, r, true);
  INVARIANT((operation_count * 2 * insert_ratio) <= (int64_t)all_keys.size());


  // 1. prepare new keys (non-existing key)
  // generate skewed workload
  for (auto &arr : skewed_keys) {
    arr.resize(all_keys.size());
  }
  assert(hot_data_ratio != 0);
  generate_skew_workload(hot_data_start, hot_data_ratio, hot_query_ratio, all_keys, skewed_keys);

  if (read_ratio < 1) {
    for (auto &arr : skewed_non_exist_keys) {
      arr.resize(non_exist_keys.size());
    }
    generate_skew_workload(hot_data_start, hot_data_ratio, hot_query_ratio, non_exist_keys, skewed_non_exist_keys);
  }

  timeit();
  start_micro();
  timeit();
}