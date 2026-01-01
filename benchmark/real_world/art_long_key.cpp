#define STR_KEY
#define STR_256
#define MAX_KEY_LEN 256

#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>

#include "../synthetic/generator.h"
#include "helper.h"
#include "../synthetic/micro_common.h"

// #define TEST_DEBUG

std::vector<index_key_t> all_trace_keys;

struct alignas(CACHELINE_SIZE) FGParam {
  void *table;
  uint32_t thread_id;
  uint64_t throughput;
  uint64_t read_cycles;
  uint64_t read_tput;
  uint64_t insert_cycles;
  uint64_t insert_tput;
  std::vector<operation_t> ops;
#ifdef ANALYZE_LATENCY
  std::vector<uint64_t> read_latencies;
  std::vector<uint64_t> insert_latencies;
#endif 
};

void generate_workload_local(std::string &fname,
                       std::vector<operation_t> &operations) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);
  for (const auto& key : all_trace_keys) {
    double d = ratio_dis(gen);
    Op_Type op_type = (d <= read_ratio) ? Op_Type::read : Op_Type::insert;
    operations.emplace_back(op_type, key);
  }
}


// NOTE(zhr): 执行真正的CRUD相关操作
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

  fg_prologue(table, thread_id);
  ready_threads++;

#ifdef TEST_DEBUG
  uint64_t hot = 0;
  uint64_t counter = 0;
#endif

  uint64_t cnt = 1;
  size_t op_i = 0;
  volatile bool b = false;
  while (!running) {
    Value v{3333};
    switch (thread_param.ops[op_i].first) {
      case Op_Type::read:
        b = table->get(thread_param.ops[op_i].second, v,
                      ti, q, thread_id);
        break;
      case Op_Type::insert:
        v.val = rand_int64(r, 123, 123456789);
        b = table->insert(thread_param.ops[op_i].second, v,
                          ti, q, thread_id);
        break;
      case Op_Type::remove:
        b = table->remove(thread_param.ops[op_i].second,
                          ti, q, thread_id);
        break;
      default:
        break;
    }
    ++op_i;
    if (unlikely(op_i == thread_param.ops.size())) {
      op_i = 0;
    }
  }
  COUT_THIS("-------Worker" << thread_id << " Finish Warmup");

  cnt = 1;
  uint64_t t1 = 0, t2 = 0;
  while (running) {
    double d = ratio_dis(gen);
    cnt += (d > 0.9) ? 1 : 0;

    if (op_i % 50 == 0) {
        epochinc();
        ti->rcu_quiesce();
    }
      
    Value v{3333};
    switch (thread_param.ops[op_i].first) {
      case Op_Type::read:
#ifdef ANALYZE_LATENCY
        if (record_op_latency) {
          t1 = util_rdtsc();
          b = table->get(thread_param.ops[op_i].second, v,
                        ti, q, thread_id);
          t2 = util_rdtsc();
          thread_param.read_cycles += (t2 - t1);
          thread_param.read_tput++;
          thread_param.read_latencies.push_back(t2 - t1);
        } else {
          b = table->get(thread_param.ops[op_i].second, v,
                        ti, q, thread_id);
        }
#else
        b = table->get(thread_param.ops[op_i].second, v,
                      ti, q, thread_id);
#endif
        break;
      case Op_Type::insert:
        v.val = rand_int64(r, 123, 123456789);
#ifdef ANALYZE_LATENCY
        if (record_op_latency) {
          t1 = util_rdtsc();
          b = table->insert(thread_param.ops[op_i].second, v,
                            ti, q, thread_id);
          t2 = util_rdtsc();
          thread_param.insert_cycles += (t2 - t1);
          thread_param.insert_tput++;
          thread_param.insert_latencies.push_back(t2 - t1);
        } else {
          b = table->insert(thread_param.ops[op_i].second, v,
                            ti, q, thread_id);
        }
#else
        b = table->insert(thread_param.ops[op_i].second, v,
                          ti, q, thread_id);
#endif
        break;
      case Op_Type::remove:
        b = table->remove(thread_param.ops[op_i].second,
                          ti, q, thread_id);
        break;
      default:
        break;
    }
#ifdef TEST_DEBUG
      if (op_keys[op_i] >= hot_key_lower_bound && op_keys[op_i] <= hot_key_upper_bound) {
        ++hot;
      }
      ++counter;
#endif
    op_i++;
    if (unlikely(op_i == thread_param.ops.size())) {
      // COUT_THIS("refresh");
      op_i = 0;
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

// NOTE(zhr): 感觉相当于守护线程，产生多个线程，并对于运行时间进程控制
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
  std::vector<operation_t> operations;
  generate_workload_local(trace_file_name, operations);

  // 2. create foreground thread
  // fprintf(stderr, "[DEBUG] create foreground thread\n");
  // set signal for masstree
  bzero((void *)timeout, sizeof(timeout));
  signal(SIGALRM, test_timeout);
  running = false;
  size_t exist_key_n_per_thread = 0, exist_key_start = 0;
  size_t operation_size = operations.size();
  COUT_VAR(operation_size);
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].read_cycles = 0;
    fg_params[worker_i].read_tput = 0;
    fg_params[worker_i].insert_cycles = 0;
    fg_params[worker_i].insert_tput = 0;
    exist_key_n_per_thread = operation_size / fg_n;
    exist_key_start = worker_i * exist_key_n_per_thread;
    COUT_THIS("[DEBUG] exist_key_n_per_thread: " << exist_key_n_per_thread
                                                 << " exist_key_start: " << exist_key_start);
    fg_params[worker_i].ops = std::vector<operation_t>(
        operations.begin() + exist_key_start,
        operations.begin() + exist_key_start + exist_key_n_per_thread);
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
    if (current_sec == 20 || (current_sec == 2 && read_ratio == 0.5)) {
      record_op_latency = true;
    } else if (current_sec == 30) {
      record_op_latency = false;
    }
#endif
  }

  // 6. thread join and exit
  // running = false;
  // timeit();
  // void *status;
  // for (size_t i = 0; i < fg_n; i++) {
  //   int rc = pthread_join(threads[i], &status);
  //   if (rc) {
  //     COUT_N_EXIT("Error:unable to join," << rc);
  //   }
  // }

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
  COUT_THIS("[Overall] all_read_tput: " << all_read_tput);
  COUT_THIS("[Overall] all_read_cycles: " << all_read_cycles);
  COUT_THIS("[Overall] all_insert_tput: " << all_insert_tput);
  COUT_THIS("[Overall] all_insert_cycles: " << all_insert_cycles);

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
                                p.insert_latencies.begin(),
                                p.insert_latencies.end());
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

void prepare_data_local(std::string &fname) {
  std::fstream fs(fname.c_str(), std::fstream::in);
  std::string line;
  all_trace_keys.reserve(table_size);
  int id = 0;
  std::unordered_set<std::string> inserted_str;
  while (std::getline(fs, line)) {
    // std::cout << "[DEBUG] line length: " << line.size() << std::endl;
    if (line.size() > MAX_KEY_LEN) {
      line = line.substr(0, MAX_KEY_LEN);
    }
    ++id; 
    if (inserted_str.count(line)) {
      // std::cout << "[DEBUG] dup line: " << line << std::endl;
      continue;
    } else {
      inserted_str.insert(line);
    }
    all_trace_keys.emplace_back(index_key_t(line.c_str()));
    if (all_trace_keys.size() > table_size) {
      break;
    }
  }
  fs.close();
  // 均匀抽样 read_ratio * all_trace_keys.size() 个 key 放入 all_keys
  size_t sample_size = static_cast<size_t>(read_ratio * all_trace_keys.size());
  size_t m = all_trace_keys.size() / sample_size;
  for (size_t i = 0; i < all_trace_keys.size(); i += m) {
    all_keys.emplace_back(all_trace_keys[i]);
  }
  COUT_VAR(all_keys.size());
}

/// @example ./art_long_key --target art --runtime 30 --fg 28 --file /data/zhr/download/url/key_data/extracted_urls.txt  --cxl-percentage 80 --max-local-memory-usage 400 --table-size 20000000
int main(int argc, char **argv) {
  COUT_THIS("[Overall] ART long key");
  target = "art";
  parse_args(argc, argv);
  INVARIANT(read_ratio + insert_ratio == 1);

  FastRandom r(3333);
  prepare_data_local(file_name);

  timeit();
  start_micro();
  timeit();
}
