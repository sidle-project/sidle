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

size_t scan_len_min = 1, scan_len_max = 100;
std::atomic<size_t> done_scan_threads(0);

struct alignas(CACHELINE_SIZE) FGParam {
  void *table;
  uint32_t thread_id;
  uint32_t throughput;
  uint32_t insert_cycles;
  uint32_t insert_tput;
  uint32_t scan_cycles;
  uint32_t scan_tput;
#ifdef ANALYZE_LATENCY
  std::vector<uint64_t> insert_latencies;
  std::vector<uint64_t> scan_latencies;
#endif
};

/*
 * REQUIRE: thread_id starts from 0 and increases monotonically
 */
template <class tab_t>
void *run_fg(void *param) {
  FGParam &thread_param = *(FGParam *)param;
  tab_t *table = (tab_t *)thread_param.table;
  uint32_t thread_id = thread_param.thread_id;
  bind_to_physical_cores(thread_id);

  threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
  query<row_type> q;
  FastRandom r(thread_id);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> scan_len_generator(scan_len_min,
                                                     scan_len_max);
  std::uniform_real_distribution<> ratio_dis(0, 1);

  size_t exist_key_n_per_thread = all_keys.size() / fg_n;
  size_t exist_key_start = thread_id * exist_key_n_per_thread,
         exist_key_end = (thread_id + 1) * exist_key_n_per_thread;
  size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
  size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
         non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;

  std::vector<index_key_t> op_keys(all_keys.begin() + exist_key_start,
                                   all_keys.begin() + exist_key_end);
  // we can control # of non-exist key to control the size of opertaions
  op_keys.insert(op_keys.end(), non_exist_keys.begin() + non_exist_key_start,
                 non_exist_keys.begin() + non_exist_key_end);

#ifdef ORIGIN_YCSB
  for (size_t i = 1; i < op_keys.size(); ++i) {
    INVARIANT(op_keys[i].key == op_keys[i - 1].key + 1);
  }
#endif

  ScrambledZipfianGenerator key_generator(0, op_keys.size() - 1);

  size_t o_i = exist_key_n_per_thread;
  std::vector<operation_t> operations;
  while (o_i < op_keys.size()) {
    if (o_i % 50 == 0) {
        epochinc();
        ti->rcu_quiesce();
    }
    double d = ratio_dis(gen);
    if (d <= insert_ratio) {
      operations.emplace_back(Op_Type::insert, op_keys[o_i]);
      o_i++;
      if (o_i % (op_keys.size() / 10) == 0) {
        COUT_THIS("generate operations. o_i=" << o_i << ", op_keys.size()="
                                              << op_keys.size());
      }
    } else if (d <= scan_ratio + insert_ratio) {
      size_t k_i = (size_t)key_generator.nextValue();
      while (k_i >= o_i) {
        k_i = (size_t)key_generator.nextValue();
      }
      INVARIANT(k_i >= 0 && k_i < o_i);
      operations.emplace_back(Op_Type::scan, op_keys[k_i]);
    }
  }
  COUT_VAR(operations.size());

  ready_threads++;
  fg_prologue(table, thread_id);

  // only use scan operation to warmup
  uint64_t cnt = 0;
  while (!running) {
    size_t k_i = (size_t)key_generator.nextValue();
    INVARIANT(k_i >= 0 && k_i < op_keys.size());
    size_t scan_len = scan_len_generator(gen);
    std::vector<std::pair<index_key_t, Value>> data_res;
    bool res = table->scan(op_keys[k_i], scan_len, data_res, ti, q, thread_id);
    for (size_t i = 0; i < data_res.size(); i++) {
      cnt += (data_res[i].second.val > 1000) ? 1 : 0;
    }
    UNUSED(res);
  }
  COUT_THIS("------- Finish Warmup");

  double avg_request_scan_len = 0, avg_res_scan_len = 0;

  size_t op_i = 0;
  size_t scan_len = 0;
  bool res = false;
  uint64_t t1 = 0, t2 = 0;
  while (running) {
    if (op_i % 50 == 0) {
      epochinc();
      ti->rcu_quiesce();
    }
    operation_t &op = operations[op_i];
    Value v{};
    std::vector<std::pair<index_key_t, Value>> data_res;
    switch (op.first) {
      case Op_Type::insert:
#ifdef ANALYZE_LATENCY
        if (record_op_latency) {
          v.val = rand_int64(r, 123, 123456789);
          t1 = util_rdtsc();
          res = table->insert(op.second, v, ti, q, thread_id);
          t2 = util_rdtsc();
          cnt += (res && v.val > 1000) ? 1 : 0;
          thread_param.insert_cycles += (t2 - t1);
          thread_param.insert_tput++;
          thread_param.insert_latencies.push_back(t2 - t1);
        } else {
          v.val = rand_int64(r, 123, 123456789);
          res = table->insert(op.second, v, ti, q, thread_id);
          cnt += (res && v.val > 1000) ? 1 : 0;
        }
#else
        v.val = rand_int64(r, 123, 123456789);
        res = table->insert(op.second, v, ti, q, thread_id);
        cnt += (res && v.val > 1000) ? 1 : 0;
#endif
        break;
      case Op_Type::scan:
#ifdef ANALYZE_LATENCY
        if (stat_op_latency) {
          scan_len = scan_len_generator(gen);
          avg_request_scan_len += scan_len;
          t1 = util_rdtsc();
          res = table->scan(op.second, scan_len, data_res, ti, q, thread_id);
          t2 = util_rdtsc();
          for (size_t i = 0; i < data_res.size(); i++) {
            cnt += (res && data_res[i].second.val > 1000) ? 1 : 0;
          }
          avg_res_scan_len += data_res.size();
          thread_param.scan_cycles += (t2 - t1);
          thread_param.scan_tput++;
          thread_param.scan_latencies.push_back(t2 - t1);
        } else {
          scan_len = scan_len_generator(gen);
          avg_request_scan_len += scan_len;
          res = table->scan(op.second, scan_len, data_res, ti, q, thread_id);
          for (size_t i = 0; i < data_res.size(); i++) {
            cnt += (res && data_res[i].second.val > 1000) ? 1 : 0;
          }
          avg_res_scan_len += data_res.size();
        }
#else 
        scan_len = scan_len_generator(gen);
        avg_request_scan_len += scan_len;
        res = table->scan(op.second, scan_len, data_res, ti, q, thread_id);
        for (size_t i = 0; i < data_res.size(); i++) {
          cnt += (res && data_res[i].second.val > 1000) ? 1 : 0;
        }
        avg_res_scan_len += data_res.size();
#endif
        break;
      default:
        COUT_N_EXIT("bad op type!!!");
        break;
    }
    thread_param.throughput++;
    op_i++;
    if (unlikely(op_i == operations.size())) {
      COUT_THIS("[Overall] have done all the opertaions!!! size="
                << operations.size());
      done_scan_threads++;
      break;
    }
  }

  avg_request_scan_len /= thread_param.scan_tput;
  avg_res_scan_len /= thread_param.scan_tput;
  COUT_VAR(cnt);
  COUT_VAR(avg_request_scan_len);
  COUT_VAR(avg_res_scan_len);

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

  // 1. prepare new keys (non-existing key)
#ifndef ORIGIN_YCSB
  FastRandom r(9999);
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(all_keys.begin(), all_keys.end(),
               std::default_random_engine(seed));
  COUT_THIS("done shuffling all_keys");
  std::shuffle(non_exist_keys.begin(), non_exist_keys.end(),
               std::default_random_engine(seed));
  COUT_THIS("done shuffling non_exist_keys");
#endif

  // 2. create foreground thread
  running = false;
  size_t thread_i = 0;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++, thread_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].thread_id = thread_i;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].insert_cycles = 0;
    fg_params[worker_i].insert_tput = 0;
    fg_params[worker_i].scan_cycles = 0;
    fg_params[worker_i].scan_tput = 0;
    int ret = pthread_create(&threads[worker_i], nullptr, run_fg<tab_t>,
                             (void *)&fg_params[worker_i]);
    if (ret) {
      COUT_N_EXIT("Error:" << ret);
    }
  }

  // 3. wait for all threads to get ready
  COUT_THIS("prepare threads ...");
  while (ready_threads < fg_n) sleep(1);
  COUT_THIS("finish prepare threads.")

  // 4. warm up
  COUT_THIS("warm up " << warmup_time << "s...");
  sleep(warmup_time);
  COUT_THIS("finish warmup.");
  running = true;
  timeit();

  // 5. runtime info (e.g. tput)
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
    // a hack here: if all threads have done scan, then return, and reset
    // runtime in order to calulate accurate throughput
    if (done_scan_threads >= fg_n) {
      runtime = current_sec;
      sec = current_sec;
      break;
    }
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
  size_t throughput = 0, all_scan_tput = 0, all_scan_cycles = 0,
         all_insert_cycles = 0, all_insert_tput = 0;
  for (auto &p : fg_params) {
    throughput += p.throughput;
    all_scan_tput += p.scan_tput;
    all_scan_cycles += p.scan_cycles;
    all_insert_tput += p.insert_tput;
    all_insert_cycles += p.insert_cycles;
  }

  COUT_THIS("[Overall] Runtime(s): " << runtime);
  COUT_THIS("[Overall] Throughput(op/s): " << throughput / sec);
#ifdef ANALYZE_LATENCY
  if (all_insert_tput > 0) {
    COUT_THIS("[Overall] InsertAvgLatency(ns): "
            << all_insert_cycles / (all_insert_tput * CPU_CYCLE_PER_NANOSEC));
  }
  if (all_scan_tput > 0) {
    COUT_THIS("[Overall] ScanAvgLatency(ns): "
            << all_scan_cycles / (all_scan_tput * CPU_CYCLE_PER_NANOSEC));
  }
  if (all_insert_tput > 0 && all_scan_tput > 0) {
    COUT_THIS("[Overall] AvgLatency(ns): " << (all_insert_cycles + all_scan_cycles) /
            ((all_insert_tput + all_scan_tput) * CPU_CYCLE_PER_NANOSEC));
  }
  std::vector<uint64_t> all_insert_latencies, all_scan_latencies;
  for (auto &p : fg_params) {
    all_insert_latencies.insert(all_insert_latencies.end(),
                                p.insert_latencies.begin(),
                                p.insert_latencies.end());
    all_scan_latencies.insert(all_scan_latencies.end(),
                              p.scan_latencies.begin(),
                              p.scan_latencies.end());
  }
  std::sort(all_insert_latencies.begin(), all_insert_latencies.end());
  std::sort(all_scan_latencies.begin(), all_scan_latencies.end());
  COUT_THIS("[Overall] InsertLatencyP50(ns): "
          << all_insert_latencies[all_insert_latencies.size() / 2] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] InsertLatencyP90(ns): "
            << all_insert_latencies[all_insert_latencies.size() * 9 / 10] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] InsertLatencyP99(ns): "
            << all_insert_latencies[all_insert_latencies.size() * 99 / 100] / CPU_CYCLE_PER_NANOSEC); 
  COUT_THIS("[Overall] ScanLatencyP50(ns): "
          << all_scan_latencies[all_scan_latencies.size() / 2] / CPU_CYCLE_PER_NANOSEC);  
  COUT_THIS("[Overall] ScanLatencyP90(ns): "
            << all_scan_latencies[all_scan_latencies.size() * 9 / 10] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] ScanLatencyP99(ns): "
            << all_scan_latencies[all_scan_latencies.size() * 99 / 100] / CPU_CYCLE_PER_NANOSEC); 
#endif
}

int main(int argc, char **argv) {
#ifdef ORIGIN_YCSB
  COUT_THIS("[Overall] MACRO: short_range origin");
#else
  COUT_THIS("[Overall] MACRO: short_range");
#endif
  // COUT_THIS("[short range] params: ");
  // for (int i = 0; i < argc; ++i) {
  //   COUT_THIS(argv[i] << " ");
  // }
  read_ratio = 0;
  scan_ratio = 0.95;
  insert_ratio = 0.05;
  parse_args(argc, argv);
  INVARIANT(insert_ratio + scan_ratio == 1);

#ifdef ORIGIN_YCSB
  size_t k_n_per_seg = table_size / fg_n, key = 1;
  for (size_t i = 0; i < fg_n; ++i) {
    for (size_t j = 0; j < k_n_per_seg; ++j) {
      all_keys.push_back(key++);
    }
    for (size_t j = 0; j < k_n_per_seg; ++j) {
      non_exist_keys.push_back(key++);
    }
  }
  COUT_VAR(all_keys.size());
  COUT_VAR(non_exist_keys.size());
#else
  FastRandom r(3333);
  prepare_data(file_name, r);
  INVARIANT((operation_count * 2 * insert_ratio) <= (int64_t)all_keys.size());
  timeit();
#endif

  start_micro();
  timeit();
}
