#include <csignal>
#include <cstdint>
#include <fcntl.h>
#include <string>
#include <tuple>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>

#include "kv/masstree.h"
#include "kv/art_wrapper.h"

#include "generator.h"
#include "micro_base.h"

#if !defined(MICRO_COMMON_H)
#define MICRO_COMMON_H

// #define ANALYZE_LATENCY

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
volatile bool recovering = false;
kvepoch_t global_log_epoch = 0;
kvtimestamp_t initial_timestamp;
volatile bool timeout[2] = {false, false};
double duration[2] = {10, 0};

void epochinc() {
  globalepoch += 2;
  active_epoch = threadinfo::min_active_epoch();
}

inline void xalarm(double d) {
  double ip, fp = modf(d, &ip);
  struct itimerval x;
  timerclear(&x.it_interval);
  x.it_value.tv_sec = (long)ip;
  x.it_value.tv_usec = (long)(fp * 1000000);
  setitimer(ITIMER_REAL, &x, 0);
}

/* running local tests */
void test_timeout(int) {
  size_t n;
  for (n = 0; n < arraysize(timeout) && timeout[n]; ++n)
    /* do nothing */;
  if (n < arraysize(timeout)) {
    timeout[n] = true;
    if (n + 1 < arraysize(timeout) && duration[n + 1])
      xalarm(duration[n + 1]);
  }
}

/**********************************************************************
 * common configs
 *********************************************************************/
double read_ratio = 1;
double insert_ratio = 0;
double delete_ratio = 0;
double scan_ratio = 0;
double inplace_ratio = 0;
double read_modify_write_ratio = 0;
int operation_count = 0;
std::string file_name;
std::string trace_file_name;
std::vector<std::string> other_files;
std::vector<int> dynamic_intervals;
std::string target = "xindex";
size_t table_size = 1000;
size_t warmup_time = 0;
size_t runtime = 10;
size_t fg_n = 1;
size_t bg_n = 0;
double zipfian_theta = -1;

size_t li_sec_stg_mod_n = 10000;
size_t sample_lat_rate = 10000;

size_t hot_data_start = 0, hot_data_ratio = 0, hot_query_ratio = 0;
bool stat_op_latency = true;
bool record_op_latency = false;

volatile bool running = false;
std::atomic<size_t> ready_threads(0);
int cxl_percentage = 0;
int hot_range_count = 1;
int hot_range_interval = 60;
int basic_worker_wakeup_interval = 500;
int cooler_wakeup_interval = 2000;
int threshold_adjuster_wakeup_interval = 100;
uint64_t max_local_memory_usage = 0;
uint64_t hot_percentage_upper_bound = 10;
uint64_t cold_percentage_upper_bound = 90;
uint64_t hot_percentage_lower_bound = 5;
uint64_t cold_percentage_lower_bound = 80;
size_t test_data_set_size = 20000000;

/**********************************************************************
 * parse command line args
 *********************************************************************/
inline void parse_args(int argc, char **argv) {
  struct option long_options[] = {
      // basic configs
      {"read", required_argument, 0, 'a'},
      {"insert", required_argument, 0, 'b'},
      {"delete", required_argument, 0, 'c'},
      {"scan", required_argument, 0, 'd'},
      {"read-modify-write", required_argument, 0, 'e'},
      {"target", required_argument, 0, 'f'},
      {"file", required_argument, 0, 'g'},
      {"table-size", required_argument, 0, 'h'},
      {"runtime", required_argument, 0, 'i'},
      {"warmup", required_argument, 0, 'j'},
      {"fg", required_argument, 0, 'k'},
      {"bg", required_argument, 0, 'l'},
      {"op-count", required_argument, 0, 'm'},
      // configs related to specific target
      {"second-file", required_argument, 0, 'o'},
      // configs related to skewed access
      {"zipf-dist-delta", required_argument, 0, 'u'},
      {"hot-data-ratio", required_argument, 0, 'v'},
      {"hot-query-ratio", required_argument, 0, 'w'},
      {"hot-data-start", required_argument, 0, 'x'},
      // show opertaion latency or not
      {"latency", no_argument, 0, 'y'},
      {"hot-range-count", required_argument, 0, 'z'},
      {"basic-worker-wakeup-interval", required_argument, 0, 'A'},
      {"cooler-wakeup-interval", required_argument, 0, 'B'},
      {"threshold-adjuster-wakeup-interval", required_argument, 0, 'C'},
      {"max-local-memory-usage", required_argument, 0, 'D'},
      {"hot-percentage-upper-bound", required_argument, 0, 'E'},
      {"cold-percentage-upper-bound", required_argument, 0, 'F'},
      {"hot-percentage-lower-bound", required_argument, 0, 'G'},
      {"cold-percentage-lower-bound", required_argument, 0, 'H'},
      // cxl config
      {"cxl-percentage", required_argument, 0, 'I'},
      {0, 0, 0, 0}};
  std::string ops =
      "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:E:F:H:I";
  int option_index = 0;

  while (1) {
    int c = getopt_long(argc, argv, ops.c_str(), long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;
    case 'a':
      read_ratio = strtod(optarg, NULL);
      INVARIANT(read_ratio >= 0 && read_ratio <= 1);
      break;
    case 'b':
      insert_ratio = strtod(optarg, NULL);
      INVARIANT(insert_ratio >= 0 && insert_ratio <= 1);
      break;
    case 'c':
      delete_ratio = strtod(optarg, NULL);
      INVARIANT(delete_ratio >= 0 && delete_ratio <= 1);
      break;
    case 'd':
      scan_ratio = strtod(optarg, NULL);
      INVARIANT(scan_ratio >= 0 && scan_ratio <= 1);
      break;
    case 'e':
      read_modify_write_ratio = strtod(optarg, NULL);
      INVARIANT(read_modify_write_ratio >= 0 && read_modify_write_ratio <= 1);
      break;
    case 'f':
      target = std::string(optarg);
      break;
    case 'g':
      file_name = std::string(optarg);
      break;
    case 'h':
      table_size = strtoul(optarg, NULL, 10);
      INVARIANT(table_size > 0);
      break;
    case 'i':
      runtime = strtoul(optarg, NULL, 10);
      INVARIANT(runtime > 0);
      break;
    case 'j':
      warmup_time = strtoul(optarg, NULL, 10);
      // INVARIANT(warmup_time > 0);
      break;
    case 'k':
      fg_n = strtoul(optarg, NULL, 10);
      INVARIANT(fg_n > 0);
      break;
    case 'l':
      bg_n = strtoul(optarg, NULL, 10);
      break;
    case 'm':
      operation_count = strtol(optarg, NULL, 10);
      INVARIANT(operation_count > 0);
      break;
    case 'n':
      li_sec_stg_mod_n = strtol(optarg, NULL, 10);
      INVARIANT(li_sec_stg_mod_n > 0);
      break;
    case 'o':
      trace_file_name = std::string(optarg);
      other_files.push_back(trace_file_name);
      INVARIANT(!trace_file_name.empty());
      break;
    case 'u':
      zipfian_theta = strtod(optarg, NULL);
      INVARIANT(zipfian_theta < 1 && zipfian_theta > -1);
      break;
    case 'v':
      hot_data_ratio = strtol(optarg, NULL, 10);
      INVARIANT(hot_data_ratio <= 100);
      break;
    case 'w':
      hot_query_ratio = strtol(optarg, NULL, 10);
      INVARIANT(hot_query_ratio <= 100);
      break;
    case 'x':
      hot_data_start = strtol(optarg, NULL, 10);
      INVARIANT(hot_data_start <= 100);
      break;
    case 'y':
      stat_op_latency = true;
      break;
    case 'z':
      hot_range_count = strtol(optarg, NULL, 10);
      INVARIANT(hot_range_count > 0);
      break;
    case 'A':
      basic_worker_wakeup_interval = strtol(optarg, NULL, 10);
      INVARIANT(basic_worker_wakeup_interval > 0);
      break;
    case 'B':
      cooler_wakeup_interval = strtol(optarg, NULL, 10);
      INVARIANT(cooler_wakeup_interval > 0);
      break;
    case 'C':
      threshold_adjuster_wakeup_interval = strtol(optarg, NULL, 10);
      INVARIANT(threshold_adjuster_wakeup_interval > 0);
      break;
    case 'D':
      max_local_memory_usage = strtol(optarg, NULL, 10);
      INVARIANT(max_local_memory_usage > 0);
      break;
    case 'E':
      hot_percentage_upper_bound = strtol(optarg, NULL, 10);
      INVARIANT(hot_percentage_upper_bound <= 100);
      break;
    case 'F':
      cold_percentage_upper_bound = strtol(optarg, NULL, 10);
      INVARIANT(cold_percentage_upper_bound <= 100);
      break;
    case 'G':
      hot_percentage_lower_bound = strtol(optarg, NULL, 10);
      INVARIANT(hot_percentage_lower_bound <= 100 &&
                hot_percentage_lower_bound >= 0);
      break;
    case 'H':
      cold_percentage_lower_bound = strtol(optarg, NULL, 10);
      INVARIANT(cold_percentage_lower_bound <= 100 &&
                cold_percentage_lower_bound >= 0);
      break;
    case 'I':
      cxl_percentage = strtol(optarg, NULL, 10);
      INVARIANT(cxl_percentage <= 100);
      break;
    default:
      abort();
    }
  }

  COUT_THIS("Read:Insert:Delete:Scan:RMW:Inplace = "
            << read_ratio << ":" << insert_ratio << ":" << delete_ratio << ":"
            << scan_ratio << ":" << read_modify_write_ratio << ":"
            << inplace_ratio)
  double ratio_sum = read_ratio + insert_ratio + delete_ratio + scan_ratio +
                     read_modify_write_ratio + inplace_ratio;
  INVARIANT(ratio_sum > 0.9999 && ratio_sum < 1.0001); // avoid precision lost
  // INVARIANT(bg_n <= 1);
  INVARIANT(fg_n + bg_n <= 64);
  COUT_VAR(target);
  COUT_VAR(file_name);
  COUT_VAR(runtime);
  COUT_VAR(warmup_time);
  COUT_VAR(fg_n);
  COUT_VAR(bg_n);
  COUT_VAR(operation_count);
  COUT_VAR(hot_data_start);
  COUT_VAR(hot_data_ratio);
  COUT_VAR(hot_query_ratio);
  COUT_VAR(hot_range_count);
  COUT_VAR(zipfian_theta);
  COUT_VAR(stat_op_latency);
  COUT_VAR(sample_lat_rate);
}

/**********************************************************************
 * table definition
 *********************************************************************/

typedef double weight_t;

#ifdef STR_KEY

#if defined(STR_8)
typedef StrBagKey<8, weight_t> index_key_t;
#elif defined(STR_16)
typedef StrBagKey<16, weight_t> index_key_t;
#elif defined(STR_20)
typedef StrBagKey<20, weight_t> index_key_t;
#elif defined(STR_24)
typedef StrBagKey<24, weight_t> index_key_t;
#elif defined(STR_32)
typedef StrBagKey<32, weight_t> index_key_t;
#elif defined(STR_40)
typedef StrBagKey<40, weight_t> index_key_t;
#elif defined(STR_64)
typedef StrBagKey<64, weight_t> index_key_t;
#elif defined(STR_128)
typedef StrBagKey<128, weight_t> index_key_t;
#elif defined(STR_140)
typedef StrBagKey<140, weight_t> index_key_t;
#elif defined(STR_256)
typedef StrBagKey<256, weight_t> index_key_t;
#elif defined(STR_512)
typedef StrBagKey<512, weight_t> index_key_t;
#else
typedef StrBagKey<8, weight_t> index_key_t;
#endif

#else
typedef Key<weight_t> index_key_t;
#endif



typedef MasstreeKV<index_key_t, Value> tab_mt_t;
typedef ARTKV<index_key_t, Value> tab_art_t;
enum class Op_Type : uint16_t { read, insert, remove, read_modify_write, scan };
typedef std::pair<Op_Type, index_key_t> operation_t;
typedef std::tuple<Op_Type, index_key_t, uint64_t> operation_rocksdb_t;

tab_mt_t *tab_mt;
tab_art_t *tab_art;

Value default_val{1};
std::vector<index_key_t> all_keys;
std::vector<index_key_t> non_exist_keys;
std::vector<std::vector<index_key_t>> skewed_keys(hot_range_count);
std::vector<std::vector<index_key_t>> skewed_non_exist_keys(hot_range_count);

/**********************************************************************
 * data loaders
 *********************************************************************/

index_key_t rand_key(FastRandom &r) {
#ifdef STR_KEY
  index_key_t k;
  for (size_t i = 0; i < sizeof(index_key_t); ++i) {
    k.buf[i] = (uint8_t)rand_int64(r, 0, 255);
  }
  return k;
#else
  return index_key_t(rand_int64(r, 1, std::numeric_limits<int64_t>::max()));
#endif
}

// read from file
void read_keys_from_file(std::string &fname, std::vector<index_key_t> &e_keys) {
  std::fstream fs(fname.c_str(), std::fstream::in);
  assert(fs.is_open());
  std::vector<index_key_t> temp_keys;
#ifdef STR_KEY
  std::string temp_str;
  while (getline(fs, temp_str)) {
    INVARIANT(temp_str.size() <= sizeof(index_key_t));
    temp_keys.push_back(index_key_t(temp_str));
  }
#else
  int64_t num;
  while (fs >> num) {
    INVARIANT(num >= 0);
    temp_keys.push_back(index_key_t((uint64_t)num));
  }
#endif

  e_keys.insert(e_keys.begin(), temp_keys.begin(), temp_keys.end());
  fs.close();
}

// read from file, the fisrt half is used to initialize the KV,
// the second half is used as non-exist keys
void read_keys_from_file(std::string &fname, std::vector<index_key_t> &e_keys,
                         std::vector<index_key_t> &ne_keys) {
  std::fstream fs(fname.c_str(), std::fstream::in);
  assert(fs.is_open());
  std::vector<index_key_t> temp_keys;
#ifdef STR_KEY
  std::string temp_str;
  while (getline(fs, temp_str)) {
    INVARIANT(temp_str.size() <= sizeof(index_key_t));
    temp_keys.push_back(index_key_t(temp_str));
  }
#else
  int64_t num;
#ifdef TABLESIZE_RO
  INVARIANT(read_ratio == 1);
  uint64_t cnt = 0;
#endif
  while (fs >> num) {
    INVARIANT(num >= 0);
    temp_keys.push_back(index_key_t((uint64_t)num));
#ifdef TABLESIZE_RO
    cnt++;
    if (cnt == table_size) {
      COUT_THIS("Read from file got " << table_size << " keys");
      break;
    }
#endif
  }
#endif

#ifdef PURE_RO
  e_keys.insert(e_keys.begin(), temp_keys.begin(), temp_keys.end());
#else
  std::shuffle(temp_keys.begin(), temp_keys.end(),
               std::default_random_engine());
  e_keys.insert(e_keys.begin(), temp_keys.begin(),
                temp_keys.begin() + temp_keys.size() / 2);
  ne_keys.insert(ne_keys.begin(), temp_keys.begin() + temp_keys.size() / 2,
                 temp_keys.end());
#endif
  fs.close();
}

uint64_t rand_seed(FastRandom &r) {
  return rand_int64(r, 1, std::numeric_limits<int64_t>::max());
}

// prepare exist and non-exist-keys
void prepare_data(std::string &fname, FastRandom &r, bool is_ordered = false) {
#ifdef STR_KEY
  COUT_THIS("Prepare Str Key!"
            << ", str_len=" << sizeof(index_key_t));
#else
  COUT_THIS("Prepare Int Key!");
#endif
  all_keys.clear();
  uint64_t seed = rand_seed(r);
  if (fname.empty()) {
    all_keys.reserve(table_size);
    non_exist_keys.reserve(table_size);
    if (target == "art") {
      seed = 0;
      uint64_t step = UINT64_MAX / (table_size * 2);
      for (size_t i = 0; i < table_size; ++i) {
        all_keys.push_back(index_key_t(seed));
        seed += step * 2;
      }
#ifndef PURE_RO
      seed = step;
      for (size_t i = 0; i < table_size; ++i) {
        non_exist_keys.push_back(index_key_t(seed));
        seed += step * 2;
      }
#endif 
    } else {
      for (size_t i = 0; i < table_size; ++i) {
        if (is_ordered) {
          all_keys.push_back(index_key_t(seed + i * 2));
        } else {
          all_keys.push_back(rand_key(r));
        }
      }
#ifndef PURE_RO
      for (size_t i = 0; i < table_size; ++i) {
        if (is_ordered) {
          non_exist_keys.push_back(index_key_t(seed + i * 2 + 1));
        } else {
          non_exist_keys.push_back(rand_key(r));
        }
      }
#endif
    }
  } else {
    read_keys_from_file(fname, all_keys, non_exist_keys);
    table_size = all_keys.size();
  }
  COUT_VAR(all_keys.size());
  COUT_VAR(non_exist_keys.size());
}

std::vector<std::string> split(const std::string &str, char delimiter) {
  std::vector<std::string> tokens;
  std::string token;
  size_t start = 0, end = 0;
  size_t str_length = str.length();
  while ((end = str.find(delimiter, start)) != std::string::npos) {
    token = str.substr(start, end - start);
    tokens.push_back(token);
    start = end + 1;
  }
  // Add the last token
  if (start != str_length) {
    tokens.push_back(str.substr(start));
  }
  return tokens;
}


void prepare_data(std::string &fname) {
  std::fstream fs(fname.c_str(), std::fstream::in);
  std::string line;
  std::getline(fs, line);
  uint64_t step = std::strtoull(line.c_str(), NULL, 10);
  uint64_t cur_key = 0;
  if (target == "art") {
    step = UINT64_MAX / test_data_set_size;
    for (uint64_t i = 0; i < test_data_set_size; ++i) {
      all_keys.emplace_back(index_key_t(cur_key));
      cur_key += step;
    }
  } else {
    uint64_t cur_key = 0;
    while (std::getline(fs, line)) {
      uint64_t key_from_trace = std::strtoull(line.c_str(), NULL, 10);
      while (key_from_trace > cur_key) {
        all_keys.emplace_back(index_key_t(cur_key));
        cur_key += step;
      }
      // if (key_from_trace != cur_key) {
      //   all_keys.emplace_back(index_key_t(key_from_trace));
      // }
      all_keys.emplace_back(index_key_t(cur_key));
    }
  }
  
  fs.close();
  if (target == "art") {
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(all_keys.begin(), all_keys.end(), g);
  }
}

template <class key_t>
void generate_skew_workload(double theta, std::vector<key_t> &keys) {
  INVARIANT(theta < 1 && theta > -1);
  std::sort(keys.begin(), keys.end());
  std::vector<key_t> temp_keys(keys.begin(), keys.end());

  ZipfianGenerator generator(0, keys.size() - 1, abs(theta));
  for (uint64_t i = 0; i < keys.size(); i++) {
    uint64_t idx = generator.nextValue(keys.size());
    if (theta >= 0) {
      keys[i] = temp_keys[idx];
      INVARIANT(keys[i] == temp_keys[idx]);
    } else {
      keys[i] = temp_keys[keys.size() - 1 - idx];
      INVARIANT(keys[i] == temp_keys[keys.size() - 1 - idx]);
    }
  }
}

void generate_workload(std::string &fname,
                       std::vector<operation_t> &operations) {
  std::fstream fs(fname.c_str(), std::fstream::in);
  std::string line;
  uint64_t key;
  while (std::getline(fs, line)) {
    char type = line[0];
    auto sec_comma = line.find(',', 2);
    // COUT_THIS(line);
    key = std::stoull(line.substr(2, sec_comma - 2));
    if (type == '1') {
      operations.emplace_back(Op_Type::insert, index_key_t(key));
    } else {
      operations.emplace_back(Op_Type::read, index_key_t(key));
    }
  }
}

void generate_workload(std::string &fname, std::vector<operation_rocksdb_t> &operations) {
  std::fstream fs(fname.c_str(), std::fstream::in);
  std::string line;
  uint64_t key;
  uint64_t length;
  size_t space_pos = 0;
  while (std::getline(fs, line)) {
    char type = line[0];
    if (type != '0' && type != '1' && type != '2') {
      continue;
    }
    switch (type) {
      case '0': // get
        operations.emplace_back(Op_Type::read, index_key_t(key), 0);
        break;
      case '1': // put
        operations.emplace_back(Op_Type::insert, index_key_t(key), 0);
        break;
      case '2': // scan
        space_pos = line.find(' ', 2);
        if (space_pos != std::string::npos) {
          length = std::stoull(line.substr(space_pos + 1));
          operations.emplace_back(Op_Type::scan, index_key_t(key), length);
        }
        break;
      default:
        std::cerr << "invalid operation type" << std::endl;
    }
  }
}

// hot_query_ratio falls into [hot_data_start, hot_data_start + hot_data_ratio]
template <class key_t>
void generate_skew_workload(size_t hot_data_start, size_t hot_data_ratio,
                            size_t hot_query_ratio, std::vector<key_t> &keys,
                            std::vector<std::vector<key_t>> &targets) {
  COUT_THIS("------- Begin Generate Skew Workload.");

  std::sort(keys.begin(), keys.end());
  for (int k = 0; k < hot_range_count; ++k) {
    size_t real_range = keys.size() / 100.0 * hot_data_ratio;
    size_t real_start = keys.size() / 100.0 * hot_data_start + real_range * k;
    if (real_start > keys.size() - real_range) {
      real_start = keys.size() - real_range;
    }
    COUT_VAR(real_range);
    COUT_VAR(real_start);

    auto total_start = keys.begin();
    auto total_end = keys.end();
    auto workload_start = keys.begin() + real_start;
    auto workload_end = keys.begin() + real_start + real_range;

    // in temp_keys, hot range is [0, real_range), otherwise cold keys
    std::vector<key_t> temp_keys(keys.begin(), keys.end());
    std::copy(workload_start, workload_end, temp_keys.begin());
    std::copy(total_start, workload_start, temp_keys.begin() + real_range);
    std::copy(workload_end, total_end,
              temp_keys.begin() + real_start + real_range);

    size_t num_hot = real_range;
    size_t num_cold = keys.size() - num_hot;
    COUT_VAR(num_cold);
    COUT_VAR(num_hot);

    std::default_random_engine gen1(0xdeadbeef);
    std::default_random_engine gen2(0xbeefdead);
    std::uniform_int_distribution<uint64_t> uniform(0, 99);
    std::uniform_int_distribution<uint64_t> uniform_hot_index(0, num_hot - 1);
    std::uniform_int_distribution<uint64_t> uniform_cold_index(
        num_hot, temp_keys.size() - 1);

    uint64_t hot_key_n = 0, cold_key_n = 0;
    for (uint64_t i = 0; i < keys.size(); i++) {
      uint64_t index;
      if (uniform(gen1) < hot_query_ratio) {
        hot_key_n++;
        index = uniform_hot_index(gen2);
      } else {
        cold_key_n++;
        index = uniform_cold_index(gen2);
      }
      targets[k][i] = temp_keys[index];
      INVARIANT(targets[k][i] == temp_keys[index]);
    }
  }
}

// hot_query_ratio falls into [hot_data_start, hot_data_start + hot_data_ratio]
template <class key_t>
void generate_skew_workload(size_t hot_data_start, size_t hot_data_ratio,
                            size_t hot_query_ratio, std::vector<key_t> &keys) {
  COUT_THIS("------- Begin Generate Skew Workload.");

  std::sort(keys.begin(), keys.end());
  size_t real_range = keys.size() / 100.0 * hot_data_ratio;
  size_t real_start = keys.size() / 100.0 * hot_data_start;

  COUT_VAR(real_range);
  COUT_VAR(real_start);

  auto total_start = keys.begin();
  auto total_end = keys.end();
  auto workload_start = keys.begin() + real_start;
  auto workload_end = keys.begin() + real_start + real_range;

  // in temp_keys, hot range is [0, real_range), otherwise cold keys
  std::vector<key_t> temp_keys(keys.begin(), keys.end());
  std::copy(workload_start, workload_end, temp_keys.begin());
  std::copy(total_start, workload_start, temp_keys.begin() + real_range);
  std::copy(workload_end, total_end,
            temp_keys.begin() + real_start + real_range);

  size_t num_hot = real_range;
  size_t num_cold = keys.size() - num_hot;

  COUT_VAR(num_cold);
  COUT_VAR(num_hot);

  std::default_random_engine gen1(0xdeadbeef);
  std::default_random_engine gen2(0xbeefdead);
  std::uniform_int_distribution<uint64_t> uniform(0, 99);
  std::uniform_int_distribution<uint64_t> uniform_hot_index(0, num_hot - 1);
  std::uniform_int_distribution<uint64_t> uniform_cold_index(
      num_hot, temp_keys.size() - 1);

  uint64_t hot_key_n = 0, cold_key_n = 0;
  for (uint64_t i = 0; i < keys.size(); i++) {
    uint64_t index;
    if (uniform(gen1) < hot_query_ratio) {
      hot_key_n++;
      index = uniform_hot_index(gen2);
    } else {
      cold_key_n++;
      index = uniform_cold_index(gen2);
    }
    keys[i] = temp_keys[index];
    INVARIANT(keys[i] == temp_keys[index]);
  }

  COUT_VAR(cold_key_n);
  COUT_VAR(hot_key_n);
  COUT_THIS("------- End Generate Skew Workload");
  timeit();
}

#ifndef STR_KEY
void skew_pattern_generator(std::string &fname, std::string &outfname,
                            size_t hot_data_start, size_t hot_data_ratio,
                            size_t hot_query_ratio) {
  std::fstream infs(fname.c_str(), std::fstream::in);
  INVARIANT(infs.is_open());
  std::vector<index_key_t> keys;

  int64_t num;
  while (infs >> num) {
    INVARIANT(num >= 0);
    keys.push_back(index_key_t((uint64_t)num));
  }
  infs.close();

  generate_skew_workload(hot_data_start, hot_data_ratio, hot_query_ratio, keys,
                         skewed_keys);

  std::fstream outfs;
  outfs.open(outfname.c_str(), std::fstream::out);
  INVARIANT(outfs.is_open());

  uint64_t cnt = 0;
  for (auto &n : keys) {
    if (cnt == keys.size() - 1) {
      outfs << n.key;
    } else {
      outfs << n.key << "\n";
    }
    cnt++;
  }
  outfs.close();
}
#endif

/**********************************************************************
 * prepare tables
 *********************************************************************/
inline void prepare_masstree(tab_mt_t *&table, threadinfo *main_ti,
                             query<row_type> &q) {
  INVARIANT(table == nullptr);
  table = new tab_mt_t(main_ti, cxl_percentage, max_local_memory_usage);
  for (auto iter = all_keys.begin(); iter != all_keys.end(); iter++) {
    bool res = table->insert(*iter, default_val, main_ti, q, 0);
    assert(res);
    UNUSED(res);
  }
  // begin background support
  table->init_migration_worker(
      fg_n, basic_worker_wakeup_interval, cooler_wakeup_interval,
      threshold_adjuster_wakeup_interval, hot_percentage_lower_bound);
}


inline void prepare_art(tab_art_t *&table) {
  INVARIANT(table == nullptr);
  query<row_type> q;  // not used
  table = new tab_art_t(cxl_percentage, max_local_memory_usage);
  uint64_t id = 0;
  for (auto iter = all_keys.begin(); iter != all_keys.end(); iter++) {
    if (id % 10000000 == 0) {
      printf("insert %ld\n", id);
    }
    ++id;
    bool res = table->insert(*iter, default_val, nullptr, q, 0);
    INVARIANT(res);
    UNUSED(res);
  }
  table->init_migration_worker(
    fg_n, basic_worker_wakeup_interval, cooler_wakeup_interval,
    threshold_adjuster_wakeup_interval);
}

/**********************************************************************
 * table prologue & epilogue
 *********************************************************************/

template <class tab_t> void fg_prologue(tab_t *table, uint32_t thread_id) {}
template <class tab_t> void fg_epilogue(tab_t *table, uint32_t thread_id) {}

/**********************************************************************
 * common interface
 *********************************************************************/

template <class tab_t> void *run_fg(void *param);

template <class tab_t>
void run_benchmark(tab_t *table, size_t sec, threadinfo *main_ti);

void start_micro() {
  threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  query<row_type> q;

  if (target == "masstree") {
    prepare_masstree(tab_mt, main_ti, q);
    timeit();
    run_benchmark(tab_mt, runtime, main_ti);
  } else if (target == "art") {
    prepare_art(tab_art);
    timeit();
    run_benchmark(tab_art, runtime, main_ti);
  } else {
    COUT_N_EXIT("the fuck?");
  }
  
  if (tab_mt != nullptr) {
    delete tab_mt;
  }
  if (tab_art != nullptr) {
    delete tab_art;
  }
}
#endif // MICRO_COMMON_H
