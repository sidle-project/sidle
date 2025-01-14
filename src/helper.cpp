#include <cassert>
#include <chrono>
#include <iostream>

#include "helper.h"

void timeit() {
  using namespace std::chrono;
  static bool inited = false;
  static steady_clock::time_point last_time_point;

  if (inited) {
    duration<double> time_span =
        duration_cast<duration<double>>(steady_clock::now() - last_time_point);
    COUT_THIS("-----" << time_span.count() << " seconds passed");
    last_time_point = steady_clock::now();
  } else {
    last_time_point = steady_clock::now();
    inited = true;
  }
}

void print_matrix(double *data, size_t row_n, size_t col_n) {
  for (size_t row_i = 0; row_i < row_n; row_i++) {
    for (size_t col_i = 0; col_i < col_n; col_i++) {
      std::cout << data[row_i * col_n + col_i] << "\t";
    }
    COUT_THIS(" ");
  }
}

void print_matrix(const uint8_t *data, const size_t row_n, const size_t col_n) {
  for (size_t row_i = 0; row_i < row_n; row_i++) {
    for (size_t col_i = 0; col_i < col_n; col_i++) {
      std::cout << (int)data[row_i * col_n + col_i] << "\t";
    }
    COUT_THIS(" ");
  }
}

// used to bind foreground thread
// if # of foreground thread is larger than phisical cores
int bind_to_physical_cores(int t_id) {
  const int physical_core_n = 48;
  INVARIANT(t_id < physical_core_n);
  int mapping[physical_core_n] = {
      0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22,  // NUMA0
      1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23,
      24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46,
      25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47,
  };
  int y = mapping[t_id];  // core 0-23 are different phisical cores
  fprintf(stdout, "thread: %d binding %d\n", t_id, y);
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(y, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  return 0;
}


// used to bind background thread
int bind_to_physical_cores_reverse(int t_id) {
  INVARIANT(t_id < 64);
  COUT_N_EXIT("this function is wrong!");
  return bind_to_physical_cores(11 - t_id);
}
