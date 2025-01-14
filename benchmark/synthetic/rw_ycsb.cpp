#include <algorithm>
#include <atomic>
#include <getopt.h>
#include <map>
#include <memory>
#include <set>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>

#include "generator.h"
#include "helper.h"
#include "micro_common.h"

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

template <class tab_t, bool stat_latency> void *run_fg(void *param) {
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

  std::vector<index_key_t> op_keys(all_keys.begin() + exist_key_start,
                                   all_keys.begin() + exist_key_end);

  ScrambledZipfianGenerator key_generator(0, op_keys.size() - 1);

  std::vector<operation_t> operations;
  while (operations.size() < 5 * op_keys.size()) {
    double d = ratio_dis(gen);
    size_t k_i = (size_t)key_generator.nextValue();
    INVARIANT(k_i >= 0 && k_i < all_keys.size());
    if (d <= read_ratio) {
      operations.emplace_back(Op_Type::read, op_keys[k_i]);
    } else {
      operations.emplace_back(Op_Type::insert, op_keys[k_i]);
    }
  }

  // COUT_THIS("[DEBUG] Worker" << thread_id << " finish perpare operations");
  fg_prologue(table, thread_id);
  ready_threads++;

  uint64_t cnt = 1;
  size_t op_i = 0;
  volatile bool b = false;
  while (!running) {
    operation_t &op = operations[op_i];
    Value v{3333};
    switch (op.first) {
    case Op_Type::read:
      b = table->get(op.second, v, ti, q, thread_id);
      cnt += (b && v.val > 333) ? 1 : 0;
      break;
    case Op_Type::insert:
      v.val = rand_int64(r, 123, 1234567890);
      b = table->insert(op.second, v, ti, q, thread_id);
      break;
    default:
      COUT_N_EXIT("bad op type!!!");
      break;
    }
    op_i++;
    if (op_i == operations.size()) {
      op_i = 0;
    }
  }
  COUT_THIS("-------Worker" << thread_id << " Finish Warmup");

  cnt = 1;
  op_i = 0;
  uint64_t t1 = 0, t2 = 0;
  while (running) {
    double d = ratio_dis(gen);
    cnt += (d > 0.9) ? 1 : 0;
    operation_t &op = operations[op_i];

    if (op_i % 50 == 0) {
      epochinc();
      ti->rcu_quiesce();
    }
    Value v{3333};
    switch (op.first) {
    case Op_Type::read:
#ifdef ANALYZE_LATENCY
      if (record_op_latency) {
        t1 = util_rdtsc();
        b = table->get(op.second, v, ti, q, thread_id);
        cnt += (b && v.val > 333) ? 1 : 0;
        t2 = util_rdtsc();
        thread_param.read_cycles += (t2 - t1);
        thread_param.read_tput++;
        thread_param.read_latencies.emplace_back(t2 - t1);
      } else {
        b = table->get(op.second, v, ti, q, thread_id);
        cnt += (b && v.val > 333) ? 1 : 0;
      }
#else
      b = table->get(op.second, v, ti, q, thread_id);
      cnt += (b && v.val > 333) ? 1 : 0;
#endif
      break;
    case Op_Type::insert:
#ifdef ANALYZE_LATENCY
      if (record_op_latency) {
        v.val = rand_int64(r, 123, 123456789);
        t1 = util_rdtsc();
        b = table->insert(op.second, v, ti, q, thread_id);
        t2 = util_rdtsc();
        thread_param.insert_cycles += (t2 - t1);
        thread_param.insert_tput++;
        thread_param.insert_latencies.emplace_back(t2 - t1);
      } else {
        v.val = rand_int64(r, 123, 123456789);
        b = table->insert(op.second, v, ti, q, thread_id);
      }
#else
      v.val = rand_int64(r, 123, 123456789);
      b = table->insert(op.second, v, ti, q, thread_id);
#endif
      break;
    default:
      COUT_N_EXIT("bad op type!!!");
      break;
    }
    op_i++;
    if (unlikely(op_i == operations.size())) {
      op_i = 0;
    }
    thread_param.throughput++;
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

  // 1. prepare new keys (non-existing key)
#ifndef ORIGIN_YCSB
  FastRandom r(9999);
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(all_keys.begin(), all_keys.end(),
               std::default_random_engine(seed));
  COUT_THIS("done shuffling all_keys");
#endif

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
  while (ready_threads < fg_n) {
    sleep(1);
  }

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
    COUT_THIS(
        "[Overall] AvgLatency(ns): "
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

int main(int argc, char **argv) {
#ifdef ORIGIN_YCSB
  COUT_THIS("[Overall] MACRO: rw_ycsb origin");
#else
  COUT_THIS("[Overall] MACRO: rw_ycsb");
#endif
  parse_args(argc, argv);
  INVARIANT(read_ratio + insert_ratio == 1);

#ifdef ORIGIN_YCSB
  if (target == "art") {
    uint64_t step = UINT64_MAX / table_size;
    uint64_t cur_lower_key = 0;
    uint64_t cur_upper_key = UINT64_MAX;
    for (size_t i = 0; i < table_size; ++i) {
      all_keys.push_back(cur_lower_key);
      cur_lower_key += step;
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(all_keys.begin(), all_keys.end(), g);
  } else {
    for (size_t i = 0; i < table_size; ++i) {
      all_keys.push_back(i + 1);
    }
  }
  COUT_VAR(all_keys.size());
  COUT_VAR(non_exist_keys.size());
#else
  FastRandom r(3333);
  prepare_data(file_name, r);
  INVARIANT((operation_count * 2 * insert_ratio) <= (int64_t)all_keys.size());
#endif
  timeit();
  start_micro();
  timeit();
}