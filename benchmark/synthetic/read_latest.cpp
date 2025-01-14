

#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <set>
#include <string>

#include "generator.h"
#include "helper.h"
#include "micro_common.h"

#ifdef ORIGIN_YCSB
std::atomic<size_t> done_threads(0);
#endif

struct alignas(CACHELINE_SIZE) FGParam {
  void *table;
  uint32_t thread_id;
  uint64_t read_cycles;
  uint64_t read_tput;
  uint64_t insert_cycles;
  uint64_t throughput;
  uint64_t op_cnt;
  uint64_t insert_tput;
#ifdef ANALYZE_LATENCY
  std::vector<uint64_t> read_latencies;
  std::vector<uint64_t> insert_latencies;
#endif
};

template <class tab_t>
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
  size_t exist_key_start = thread_id * exist_key_n_per_thread,
         exist_key_end = (thread_id + 1) * exist_key_n_per_thread;
  size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
  size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
         non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;

  std::vector<index_key_t> op_keys(all_keys.begin() + exist_key_start,
                                   all_keys.begin() + exist_key_end);
  op_keys.insert(op_keys.end(), non_exist_keys.begin() + non_exist_key_start,
                 non_exist_keys.begin() + non_exist_key_end);
#ifdef ORIGIN_YCSB
  if (target == "art") {
    std::sort(op_keys.begin(), op_keys.end());
  } else {
    for (size_t i = 1; i < op_keys.size(); ++i) {
      INVARIANT(op_keys[i].key == op_keys[i - 1].key + 1);
    }
  }
#endif

  // prepare data & operations
  size_t delete_i = 0, insert_i = exist_key_n_per_thread;
  ZipfianGenerator zipf_gen(1, exist_key_n_per_thread - 1);

  std::vector<operation_t> operations;
  while (insert_i < op_keys.size()) {
    double d = ratio_dis(gen);
    if (d <= read_ratio) { // read_ratio = 0.9
      size_t offset = zipf_gen.nextValue(exist_key_n_per_thread);
      assert(insert_i >= offset);
      size_t query_i = insert_i - offset;
      operations.emplace_back(Op_Type::read, op_keys[query_i]);
    } else if (d <= read_ratio + insert_ratio) { // 0.95
      operations.emplace_back(Op_Type::insert, op_keys[insert_i]);
      insert_i++;
    } else {  // 1.0
      operations.emplace_back(Op_Type::remove, op_keys[delete_i]);
      delete_i++;
      assert(delete_i < insert_i);
    }
  }

  fg_prologue(table, thread_id);
  ready_threads++;

  size_t query_i = 0;
  while (!running) {
    Value v{3333};
    volatile bool b = table->get(op_keys[query_i], v, ti, q, thread_id);
    UNUSED(b);
    query_i++;
    if (unlikely(query_i == op_keys.size())) {
      query_i = 0;
    }
  }
  COUT_THIS("------- Finish Warmup");

  uint64_t cnt = 0;
  size_t op_i = 0;
  volatile bool b = false;
  while (running) {
    operation_t &op = operations[op_i];
    if (op_i % 50 == 0) {
        epochinc();
        ti->rcu_quiesce();
    }
    Value v{3333};
    double d = ratio_dis(gen);
    cnt += (d > 0.9) ? 1 : 0;
    uint64_t t1 = 0, t2 = 0;
    switch (op.first) {
      case Op_Type::read:
#ifdef ANALYZE_LATENCY
      if (record_op_latency) {
        t1 = util_rdtsc();
        b = table->get(op.second, v, ti, q, thread_id);
        t2 = util_rdtsc();
        cnt += (b && v.val > 333) ? 1 : 0;
        thread_param.read_cycles += (t2 - t1);
        thread_param.read_tput++;
        thread_param.read_latencies.push_back(t2 - t1);
      } else {
        b = table->get(op.second, v, ti, q, thread_id);
        cnt += (b && v.val > 333) ? 1 : 0;
      } 
#else 
        b = table->get(op.second, v, ti, q, thread_id);
        cnt += (v.val > 333) ? 1 : 0;
#endif
        break;
      case Op_Type::insert:
#ifdef ANALYZE_LATENCY
        if (record_op_latency) {
          v.val = rand_int64(r, 123, 1234567890);
          t1 = util_rdtsc();
          b = table->insert(op.second, v, ti, q, thread_id);
          t2 = util_rdtsc();
          thread_param.insert_cycles += (t2 - t1);
          thread_param.insert_tput++;
          thread_param.insert_latencies.push_back(t2 - t1);
        } else {
           v.val = rand_int64(r, 123, 1234567890);
          b = table->insert(op.second, v, ti, q, thread_id);
        }
#else 
        v.val = rand_int64(r, 123, 1234567890);
        b = table->insert(op.second, v, ti, q, thread_id);
#endif
        
        break;
      case Op_Type::remove:
        b = table->remove(op.second, ti, q, thread_id);
        break;
      default:
        COUT_N_EXIT("bad op type!!!");
        break;
    }
    op_i++;
    thread_param.throughput++;
    if (unlikely(op_i == operations.size())) {
      op_i = 0;
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

  // 1. generate new keys and shuffle all_keys
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
  timeit();

  // 2. create foreground thread
  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].read_cycles = 0;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].read_tput = 0;
    fg_params[worker_i].insert_tput = 0;
    fg_params[worker_i].insert_cycles = 0;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].op_cnt = operation_count;
    int ret = pthread_create(&threads[worker_i], nullptr, run_fg<tab_t>,
                             (void *)&fg_params[worker_i]);
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
#ifdef ORIGIN_YCSB
    // a hack here: if all threads have done scan, then return, and reset
    // runtime in order to calulate accurate throughput
    if (done_threads >= fg_n) {
      runtime = current_sec;
      sec = current_sec;
      break;
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
  size_t throughput = 0, all_read_throughput = 0, all_read_cycles = 0, all_insert_cycles = 0, all_insert_throughput = 0;
  for (auto &j : fg_params) {
    throughput += j.throughput;
    all_read_throughput += j.read_tput;
    all_read_cycles += j.read_cycles;
    all_insert_throughput += j.insert_tput;
    all_insert_cycles += j.insert_cycles;
  }
  COUT_THIS("[Overall] Runtime(s): " << runtime);
  COUT_THIS("[Overall] Throughput(op/s): " << throughput / sec);

#ifdef ANALYZE_LATENCY
  COUT_THIS("[Overall] ReadAvgLatency(ns): "
            << (all_read_cycles / all_read_throughput) / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] InsertAvgLatency(ns): "
            << (all_insert_cycles / all_insert_throughput) / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] AvgLatency(ns): "
            << (all_read_cycles + all_insert_cycles) / throughput / CPU_CYCLE_PER_NANOSEC);
  std::vector<uint64_t> all_read_latencies, all_insert_latencies;
  for (auto &j : fg_params) {
    all_read_latencies.insert(all_read_latencies.end(), j.read_latencies.begin(),
                              j.read_latencies.end());
    all_insert_latencies.insert(all_insert_latencies.end(), j.insert_latencies.begin(),
                                j.insert_latencies.end());
  }
  std::sort(all_read_latencies.begin(), all_read_latencies.end());
  std::sort(all_insert_latencies.begin(), all_insert_latencies.end());
  COUT_THIS("[Overall] ReadLatencyP50(ns): " << all_read_latencies[all_read_latencies.size() / 2] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] ReadLatencyP90(ns): "
            << all_read_latencies[all_read_latencies.size() * 9 / 10] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] ReadLatencyP99(ns): "
            << all_read_latencies[all_read_latencies.size() * 99 / 100] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] InsertLatencyP50(ns): "
            << all_insert_latencies[all_insert_latencies.size() / 2] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] InsertLatencyP90(ns): "
            << all_insert_latencies[all_insert_latencies.size() * 9 / 10] / CPU_CYCLE_PER_NANOSEC);
  COUT_THIS("[Overall] InsertLatencyP99(ns): "
            << all_insert_latencies[all_insert_latencies.size() * 99 / 100] / CPU_CYCLE_PER_NANOSEC);
#endif
}

int main(int argc, char **argv) {
#ifdef ORIGIN_YCSB
  COUT_THIS("[Overall] MACRO: read-latest origin");
#else
  COUT_THIS("[Overall] MACRO: read-latest");
#endif
  read_ratio = 0.9;
  insert_ratio = 0.05;
  delete_ratio = 0.05;
  // COUT_THIS("[read_latest] params: ");
  // for (int i = 0; i < argc; ++i) {
  //   COUT_THIS(argv[i] << " ");
  // }
  parse_args(argc, argv);
  INVARIANT(read_ratio < 1 && insert_ratio < 1 && delete_ratio < 1);
  INVARIANT(read_ratio + insert_ratio + delete_ratio == 1);

#ifdef ORIGIN_YCSB
  read_ratio = 0.95;
  insert_ratio = 0.05;
  delete_ratio = 0;
  INVARIANT(delete_ratio == 0);
  size_t k_n_per_seg = table_size / fg_n, key = 1;
  if (target == "art") {
    uint64_t step = UINT64_MAX / (table_size * 2);
    for (size_t i = 0; i < fg_n; ++i) {
      for (size_t j = 0; j < k_n_per_seg; ++j) {
        all_keys.push_back(key);
        key += step;
      }
      for (size_t j = 0; j < k_n_per_seg; ++j) {
        non_exist_keys.push_back(key);
        key += step;
      }
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(all_keys.begin(), all_keys.end(), g);
    std::shuffle(non_exist_keys.begin(), non_exist_keys.end(), g);
  } else {
    for (size_t i = 0; i < fg_n; ++i) {
      for (size_t j = 0; j < k_n_per_seg; ++j) {
        all_keys.push_back(key++);
      }
      for (size_t j = 0; j < k_n_per_seg; ++j) {
        non_exist_keys.push_back(key++);
      }
    }
  }
  COUT_VAR(all_keys.size());
  COUT_VAR(non_exist_keys.size());
#else
  FastRandom r(3333);
  prepare_data(file_name, r);
#endif
  timeit();

  start_micro();
  timeit();
}
