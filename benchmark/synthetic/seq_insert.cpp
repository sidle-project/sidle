

#include <getopt.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <set>
#include <string>

#include "helper.h"
#include "micro_common.h"

struct alignas(CACHELINE_SIZE) FGParam {
  void *table;
  uint32_t thread_id;
  uint64_t throughput;
  uint64_t insert_cycles;
  uint64_t insert_tput;
  uint64_t remove_cycles;
  uint64_t remove_tput;
};

template <class tab_t, bool stat_latency>
void *run_fg(void *param) {
  FGParam &thread_param = *(FGParam *)param;
  uint32_t thread_id = thread_param.thread_id;
  tab_t *table = (tab_t *)thread_param.table;
  bind_to_physical_cores(thread_id);

  query<row_type> q;
  threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> ratio_dis(0, 1);

  size_t exist_key_n_per_thread = all_keys.size() / fg_n;
  size_t exist_key_start = thread_id * exist_key_n_per_thread,
         exist_key_end = (thread_id + 1) * exist_key_n_per_thread;
  size_t non_exist_key_n_per_thread = non_exist_keys.size() / fg_n;
  size_t non_exist_key_start = thread_id * non_exist_key_n_per_thread,
         non_exist_key_end = (thread_id + 1) * non_exist_key_n_per_thread;

  std::vector<index_key_t> query_keys(all_keys.begin() + exist_key_start,
                                      all_keys.begin() + exist_key_end);
  query_keys.insert(query_keys.end(),
                    non_exist_keys.begin() + non_exist_key_start,
                    non_exist_keys.begin() + non_exist_key_end);

  std::vector<index_key_t> op_keys(query_keys.begin(), query_keys.end());

  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(query_keys.begin(), query_keys.end(),
               std::default_random_engine(seed));

#ifdef WEAK_RACE
  // op_keys is used to insert & delete, so it must be sorted
  for (size_t i = 1; i < exist_key_n_per_thread; ++i) {
    INVARIANT(op_keys[i - 1] <= op_keys[i]);
  }
  for (size_t i = exist_key_n_per_thread + 1; i < op_keys.size(); ++i) {
    INVARIANT(op_keys[i - 1] <= op_keys[i]);
  }
  size_t query_i = 0, delete_i = 0, insert_i = exist_key_n_per_thread;
#else
  std::sort(op_keys.begin(), op_keys.end());
  size_t query_i = 0, insert_i = 0, delete_i = exist_key_n_per_thread;
#endif

  INVARIANT((fg_n == 1 && op_keys.size() == 2 * exist_key_n_per_thread) ||
            fg_n > 1);

  fg_prologue(table, thread_id);
  ready_threads++;

  uint64_t cnt = 1;

  while (!running) {
    double d = ratio_dis(gen);
    if (d <= read_ratio) {
      Value v{3333};
      volatile bool b = table->get(query_keys[query_i], v, ti, q, thread_id);
      cnt += (b && v.val > 5000000) ? 1 : 0;
      if (unlikely(query_i == query_keys.size())) {
        query_i = 0;
      }
    } else if (d <= read_ratio + insert_ratio) {
      Value v{12345};
      bool res = table->insert(op_keys[insert_i], v, ti, q, thread_id);
      UNUSED(res);
      insert_i++;
      if (unlikely(insert_i == op_keys.size())) {
        insert_i = 0;
      }
    } else {
      bool res = table->remove(op_keys[delete_i], ti, q, thread_id);
      UNUSED(res);
      delete_i++;
      if (unlikely(delete_i == op_keys.size())) {
        delete_i = 0;
      }
    }
  }
  COUT_THIS("------- Finish Warmup");

  cnt = 1;
  while (running) {
    double d = ratio_dis(gen);
    if (d <= read_ratio) {
      Value v{3333};
      bool b = table->get(query_keys[query_i], v, ti, q, thread_id);
      cnt += (b && v.val > 5000000) ? 1 : 0;
      if (unlikely(query_i == query_keys.size())) {
        query_i = 0;
      }
    } else if (d <= read_ratio + insert_ratio) {
      Value v{12345};
      if (stat_latency) {
        uint64_t t = util_rdtsc();
        bool res = table->insert(op_keys[insert_i], v, ti, q, thread_id);
        UNUSED(res);
        thread_param.insert_cycles += (util_rdtsc() - t);
        thread_param.insert_tput++;
      } else {
        bool res = table->insert(op_keys[insert_i], v, ti, q, thread_id);
        UNUSED(res);
      }
      insert_i++;
      if (unlikely(insert_i == op_keys.size())) {
        insert_i = 0;
      }
    } else {
      if (stat_latency) {
        uint64_t t = util_rdtsc();
        bool res = table->remove(op_keys[delete_i], ti, q, thread_id);
        UNUSED(res);
        thread_param.remove_cycles += (util_rdtsc() - t);
        thread_param.remove_tput++;
      } else {
        bool res = table->remove(op_keys[delete_i], ti, q, thread_id);
        UNUSED(res);
      }
      delete_i++;
      if (unlikely(delete_i == op_keys.size())) {
        delete_i = 0;
      }
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
  FastRandom r(9999);
#ifdef WEAK_RACE
  // sort the first half(existing keys) and the second half(non-existing key)
  std::sort(all_keys.begin(), all_keys.end());
  std::sort(non_exist_keys.begin(), non_exist_keys.end());

#else
  // here we shuffle all the keys to create contention
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(all_keys.begin(), all_keys.end(),
               std::default_random_engine(seed));
  COUT_THIS("done shuffling all_keys");
  std::shuffle(non_exist_keys.begin(), non_exist_keys.end(),
               std::default_random_engine(seed));
  COUT_THIS("done shuffling non_exist_keys");
#endif

  // 1. create foreground thread
  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    fg_params[worker_i].table = table;
    fg_params[worker_i].throughput = 0;
    fg_params[worker_i].thread_id = worker_i;
    fg_params[worker_i].insert_cycles = 0;
    fg_params[worker_i].insert_tput = 0;
    fg_params[worker_i].remove_cycles = 0;
    fg_params[worker_i].remove_tput = 0;
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
  size_t throughput = 0, all_remove_tput = 0, all_remove_cycles = 0,
         all_insert_cycles = 0, all_insert_tput = 0;
  for (auto &p : fg_params) {
    throughput += p.throughput;
    all_remove_tput += p.remove_tput;
    all_remove_cycles += p.remove_cycles;
    all_insert_tput += p.insert_tput;
    all_insert_cycles += p.insert_cycles;
  }

  COUT_THIS("[Overall] RunTime(s): " << runtime);
  if (all_insert_tput > 0) {
    COUT_THIS("[Overall] InsertLatency(ns):"
              << (all_insert_cycles / all_insert_tput) / CPU_CYCLE_PER_NANOSEC);
  }
  if (all_remove_tput > 0) {
    COUT_THIS("[Overall] RemoveLatency(ns):"
              << (all_remove_cycles / all_remove_tput) / CPU_CYCLE_PER_NANOSEC);
  }
  COUT_THIS("[Overall] Throughput(op/s): " << throughput / sec);
}

int main(int argc, char **argv) {
  COUT_THIS("[Overall] MICRO: sequential insert");
  read_ratio = 0.5;
  insert_ratio = 0.25;
  delete_ratio = 0.25;
  parse_args(argc, argv);
  INVARIANT(read_ratio < 1 && insert_ratio < 1 && delete_ratio < 1);
  INVARIANT(read_ratio + insert_ratio + delete_ratio == 1);

  FastRandom r(3333);
  prepare_data(file_name, r);
  timeit();

  start_micro();
  timeit();
}
