#include <sys/types.h>
#include <atomic>
#include <cassert>
#include <cstring>
#include <string>

#include "helper.h"

#ifndef TPCC_H
#define TPCC_H

#define PACKED __attribute__((packed))
#define ALWAYS_INLINE __attribute__((always_inline))

/******************************************************************************
 * Benchmark configs
 *****************************************************************************/

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
kvepoch_t global_log_epoch = 0;
kvtimestamp_t initial_timestamp;

void epochinc() {
  globalepoch += 2;
  active_epoch = threadinfo::min_active_epoch();
}

size_t warmup = 1;
size_t runtime = 30;
size_t fg_n = 1;
size_t bg_n = 1;
size_t model_n = 10000;
int32_t operation_id = -1;
bool stat_latency = false;
volatile bool running = false;
// for tpcc, warehouse, district and item are in local memory
// configuration for cxl_percentage=80, masstree
uint64_t warehouse_max_local_memory_usage = 100;
uint64_t district_max_local_memory_usage = 100;
uint64_t customer_max_local_memory_usage = 14;
uint64_t history_max_local_memory_usage = 19;
uint64_t item_max_local_memory_usage = 100;
uint64_t stock_max_local_memory_usage = 47;
uint64_t order_max_local_memory_usage = 19;
uint64_t new_order_max_local_memory_usage = 7;
uint64_t order_line_max_local_memory_usage = 252;
// configuration for cxl_percentage=80, art
// uint64_t warehouse_max_local_memory_usage = 100;
// uint64_t district_max_local_memory_usage = 100;
// uint64_t customer_max_local_memory_usage = 60;
// uint64_t history_max_local_memory_usage = 60;
// uint64_t item_max_local_memory_usage = 100;
// uint64_t stock_max_local_memory_usage = 120;
// uint64_t order_max_local_memory_usage = 60;
// uint64_t new_order_max_local_memory_usage = 20;
// uint64_t order_line_max_local_memory_usage = 450;

size_t warehouse_num = 1;
size_t num_district_per_warehouse = 10;
const size_t num_item = 100000;
const size_t num_customer_per_district = 3000;
static int g_new_order_remote_item_pct = 1;
static int g_uniform_item_dist = 0;
int cxl_percentage = 0;

std::array<int, 5> ratio = {45 /*new_order*/, 43 /*payment*/, 4 /*delivery*/,
                            4 /*order_status*/, 4 /*stock_level*/};

// RO:read-only, IU: inplace-update, I:insert, NOP: no-operation, S: scan
uint32_t xindex_table_bg_infos[] = {0, /*Warehouse IU*/
                                    0, /*District IU*/
                                    0, /*Customer IU*/
                                    0, /*History I*/
                                    0, /*Item RO*/
                                    0, /*Stock IU*/
                                    0, /*StockData NOP*/
                                    1, /*Order I*/
                                    1, /*NewOrder I*/
                                    1, /*OrderLine I*/
                                    0, /*CustomerIndex NOP*/
                                    0 /*OrderIndex NOP*/};
uint32_t baseline_table_bg_infos[] = {1, /*Warehouse*/
                                      1, /*District*/
                                      1, /*Customer*/
                                      1, /*History*/
                                      0, /*Item*/
                                      1, /*Stock*/
                                      0, /*StockData*/
                                      1, /*Order*/
                                      1, /*NewOrder*/
                                      1, /*OrderLine*/
                                      0, /*CustomerIndex*/
                                      0 /*OrderIndex*/};

/******************************************************************************
 * utils
 *****************************************************************************/
void parse_args(int argc, char **argv) {
  while (1) {
    static struct option long_options[] = {
        {"new-order", required_argument, 0, 'a'},
        {"paymen", required_argument, 0, 'b'},
        {"delivery", required_argument, 0, 'c'},
        {"order-status", required_argument, 0, 'd'},
        {"stock", required_argument, 0, 'e'},
        {"runtime", required_argument, 0, 'f'},
        {"fg", required_argument, 0, 'g'},
        {"bg", required_argument, 0,
         'h'},  // if we set bgn=0, then no background thread will start, but if
                // we set bgn>0, we will ignore it
        {"model-n", required_argument, 0, 'i'},
        {"warehouse-num", required_argument, 0, 'j'},
        {"warmup", required_argument, 0, 'k'},
        {"district", required_argument, 0, 'l'},
        {"op-id", required_argument, 0, 'm'},
        {"stat-lat", required_argument, 0, 'n'},
        {"cxl-percentage", required_argument, 0, 'o'},
        {0, 0, 0, 0}};
    int option_index = 0;
    int c =
        getopt_long(argc, argv, "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:", long_options,
                    &option_index);
    if (c == -1) break;
    switch (c) {
      case 0:
        if (long_options[option_index].flag != 0) break;
        abort();
        break;
      case 'a':
        ratio[0] = strtod(optarg, NULL);
        break;
      case 'b':
        ratio[1] = strtod(optarg, NULL);
        break;
      case 'c':
        ratio[2] = strtod(optarg, NULL);
        break;
      case 'd':
        ratio[3] = strtod(optarg, NULL);
        break;
      case 'e':
        ratio[4] = strtod(optarg, NULL);
        break;
      case 'f':
        runtime = strtoul(optarg, NULL, 10);
        assert(runtime > 0);
        break;
      case 'g':
        fg_n = strtoul(optarg, NULL, 10);
        assert(fg_n > 0);
        break;
      case 'h':
        bg_n = strtoul(optarg, NULL, 10);
        // assert(bg_n > 0);
        break;
      case 'i':
        model_n = strtod(optarg, NULL);
        assert(model_n > 0);
        break;
      case 'j':
        warehouse_num = strtoul(optarg, NULL, 10);
        assert(warehouse_num > 0 && warehouse_num < 255);
        break;
      case 'k':
        warmup = strtoul(optarg, NULL, 10);
        break;
      case 'l':
        num_district_per_warehouse = strtoul(optarg, NULL, 10);
        assert(num_district_per_warehouse > 0 &&
               num_district_per_warehouse < 256);
        break;
      case 'm':
        operation_id = strtoul(optarg, NULL, 10);
        assert(operation_id > 0);
        break;
      case 'n':
        stat_latency = strtoul(optarg, NULL, 10) > 0;
        break;
      case 'o':
        cxl_percentage = strtol(optarg, NULL, 10);
        INVARIANT(cxl_percentage <= 100 && cxl_percentage >= 0);
        break;
      case '?':
        /* getopt_long already printed an error message. */
        exit(1);

      default:
        abort();
    }
  }
}

template <size_t N, char FILL_CHAR = ' '>
class varibale_str {
 public:
  uint8_t buf[N];

  inline void assign(const char *s) {
    size_t n = strlen(s);
    memcpy(buf, s, n < N ? n : N);
    if (N > n) {
      memset(&buf[n], FILL_CHAR, N - n);
    }
  }

  inline void assign(const char *s, size_t n) {
    memcpy(buf, s, n < N ? n : N);
    if (N > n) {
      memset(&buf[n], FILL_CHAR, N - n);
    }
  }

  inline void assign(const std::string &s) { assign(s.data()); }

  inline bool operator==(const varibale_str &other) const {
    return memcmp(buf, other.buf, N) == 0;
  }

  inline bool operator!=(const varibale_str &other) const {
    return !operator==(other);
  }

  std::string to_string() { return std::string(buf, buf + N); }
};

/******************************************************************************
 * KV structs
 *****************************************************************************/

struct W_key {
  uint64_t w_id;
  typedef std::array<double, 1> model_key_t;
  static constexpr size_t model_key_size() { return 1; }
  W_key() : w_id(0) {}
  W_key(uint64_t i) : w_id(i) {}

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = w_id;
    return model_key;
  }

  static W_key max() {
    W_key max_k(0);
    memset(&max_k, 255, sizeof(W_key));
    return max_k;
  }
  static W_key min() {
    W_key min_k(0);
    memset(&min_k, 0, sizeof(W_key));
    return min_k;
  }

  W_key to_str_key() const {
    if (little_endian()) {
      W_key reverse(0);
      for (size_t byte_i = 0; byte_i < sizeof(W_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(W_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }

  W_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const W_key &l, const W_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const W_key &l, const W_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const W_key &l, const W_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const W_key &l, const W_key &r) { return !(l == r); }
  friend bool operator>=(const W_key &l, const W_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const W_key &l, const W_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct W_val {
  float w_ytd;
  float w_tax;
  varibale_str<10> w_name;
  varibale_str<20> w_street_1;
  varibale_str<20> w_street_2;
  varibale_str<20> w_city;
  varibale_str<2> w_state;
  varibale_str<9> w_zip;
  friend bool operator<(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) < 0;
  }
  friend bool operator>(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) > 0;
  }
  friend bool operator==(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) == 0;
  }
  friend bool operator!=(const W_val &l, const W_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(W_val)) != 0;
  }
} PACKED;

struct D_key {
  uint8_t dummy[5];
  uint8_t d_id;
  uint16_t d_w_id;

  D_key() : D_key(0, 0) {}
  D_key(uint16_t dw, uint8_t di) : d_id(di), d_w_id(dw) {
    for (size_t i = 0; i < 5; ++i) dummy[i] = 0;
  }

  typedef std::array<double, 2> model_key_t;
  static constexpr size_t model_key_size() { return 2; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = d_w_id;
    model_key[1] = d_id;
    return model_key;
  }

  static D_key max() {
    D_key max_k(0, 0);
    memset(&max_k, 255, sizeof(D_key));
    return max_k;
  }
  static D_key min() {
    D_key min_k(0, 0);
    memset(&min_k, 0, sizeof(D_key));
    return min_k;
  }

  D_key to_str_key() const {
    if (little_endian()) {
      D_key reverse(0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(D_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(D_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }

  D_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const D_key &l, const D_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const D_key &l, const D_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const D_key &l, const D_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const D_key &l, const D_key &r) { return !(l == r); }
  friend bool operator>=(const D_key &l, const D_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const D_key &l, const D_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct D_val {
  float d_ytd;
  float d_tax;
  uint32_t d_next_o_id;
  varibale_str<10> d_name;
  varibale_str<20> d_street1;
  varibale_str<20> d_street2;
  varibale_str<20> d_city;
  varibale_str<2> d_state;
  varibale_str<9> d_zip;
  friend bool operator<(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) < 0;
  }
  friend bool operator>(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) > 0;
  }
  friend bool operator==(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) == 0;
  }
  friend bool operator!=(const D_val &l, const D_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(D_val)) != 0;
  }
} PACKED;

struct CustomerNameIndexKey {
  uint16_t ci_w_id;
  uint8_t ci_d_id;
  varibale_str<16> ci_last;
  varibale_str<16> ci_first;

  static const size_t model_unit = 8;
  typedef std::array<double, 5> model_key_t;
  static constexpr size_t model_key_size() {
    return (sizeof(CustomerNameIndexKey) % model_unit) == 0
               ? (sizeof(CustomerNameIndexKey) / model_unit)
               : (sizeof(CustomerNameIndexKey) / model_unit + 1);
  }

  model_key_t to_model_key() const {
    model_key_t model_key;
    for (size_t k_i = 0; k_i < model_key_size() - 1; k_i++) {
      uint8_t temp[model_unit];
      for (size_t byte_i = 0; byte_i < model_unit; ++byte_i) {
        if (little_endian()) {
          temp[model_unit - 1 - byte_i] =
              ((uint8_t *)this)[k_i * model_unit + byte_i];
        } else {
          temp[byte_i] = ((uint8_t *)this)[k_i * model_unit + byte_i];
        }
      }
      model_key[k_i] = *(uint64_t *)&temp;
    }

    // handle trailing bytes
    size_t trailing_byte_n =
        sizeof(CustomerNameIndexKey) - (model_key_size() - 1) * model_unit;
    uint8_t temp[trailing_byte_n];
    for (size_t byte_i = 0; byte_i < trailing_byte_n; ++byte_i) {
      if (little_endian()) {
        temp[trailing_byte_n - 1 - byte_i] =
            ((uint8_t *)this)[(model_key_size() - 1) * model_unit + byte_i];
      } else {
        temp[byte_i] =
            ((uint8_t *)this)[(model_key_size() - 1) * model_unit + byte_i];
      }
    }
    if (trailing_byte_n == 1) {
      model_key[model_key_size() - 1] = *(uint8_t *)&temp;
    } else if (trailing_byte_n == 2) {
      model_key[model_key_size() - 1] = *(uint16_t *)&temp;
    } else if (trailing_byte_n == 4) {
      model_key[model_key_size() - 1] = *(uint32_t *)&temp;
    } else if (trailing_byte_n == 8) {
      model_key[model_key_size() - 1] = *(uint64_t *)&temp;
    } else {
      COUT_N_EXIT("the len of the key is not ok!! len="
                  << sizeof(CustomerNameIndexKey));
    }
    return model_key;
  }

  static CustomerNameIndexKey max() {
    CustomerNameIndexKey max_k;
    memset(&max_k, 255, sizeof(CustomerNameIndexKey));
    return max_k;
  }
  static CustomerNameIndexKey min() {
    CustomerNameIndexKey min_k;
    memset(&min_k, 0, sizeof(CustomerNameIndexKey));
    return min_k;
  }

  CustomerNameIndexKey to_str_key() const { return *this; }
  CustomerNameIndexKey to_normal_key() const { return to_str_key(); }

  friend bool operator<(const CustomerNameIndexKey &l,
                        const CustomerNameIndexKey &r) {
    return memcmp(&l, &r, sizeof(CustomerNameIndexKey)) < 0;
  }
  friend bool operator>(const CustomerNameIndexKey &l,
                        const CustomerNameIndexKey &r) {
    return memcmp(&l, &r, sizeof(CustomerNameIndexKey)) > 0;
  }
  friend bool operator==(const CustomerNameIndexKey &l,
                         const CustomerNameIndexKey &r) {
    return memcmp(&l, &r, sizeof(CustomerNameIndexKey)) == 0;
  }
  friend bool operator!=(const CustomerNameIndexKey &l,
                         const CustomerNameIndexKey &r) {
    return !(l == r);
  }
  friend bool operator>=(const CustomerNameIndexKey &l,
                         const CustomerNameIndexKey &r) {
    return memcmp(&l, &r, sizeof(CustomerNameIndexKey)) >= 0;
  }
  friend bool operator<=(const CustomerNameIndexKey &l,
                         const CustomerNameIndexKey &r) {
    return memcmp(&l, &r, sizeof(CustomerNameIndexKey)) <= 0;
  }
} PACKED;

struct CustomerNameIndexVal {
  uint32_t ci_c_id;
  friend bool operator<(const CustomerNameIndexVal &l,
                        const CustomerNameIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(CustomerNameIndexVal)) <
           0;
  }
  friend bool operator>(const CustomerNameIndexVal &l,
                        const CustomerNameIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(CustomerNameIndexVal)) >
           0;
  }
  friend bool operator==(const CustomerNameIndexVal &l,
                         const CustomerNameIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(CustomerNameIndexVal)) ==
           0;
  }
  friend bool operator!=(const CustomerNameIndexVal &l,
                         const CustomerNameIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(CustomerNameIndexVal)) !=
           0;
  }
} PACKED;

struct OrderCidIndexKey {
  uint32_t oc_o_id;
  uint16_t oc_c_id;
  uint8_t oc_d_id;
  uint8_t oc_w_id;
  OrderCidIndexKey() : OrderCidIndexKey(0, 0, 0, 0) {}
  OrderCidIndexKey(uint8_t oc_w_id, uint8_t oc_d_id, uint16_t oc_c_id,
                   uint32_t oc_o_id)
      : oc_o_id(oc_o_id),
        oc_c_id(oc_c_id),
        oc_d_id(oc_d_id),
        oc_w_id(oc_w_id) {}

  typedef std::array<double, 4> model_key_t;
  static constexpr size_t model_key_size() { return 4; }

  model_key_t to_model_key() const {
    COUT_N_EXIT("fix w_id uint8_t problem");
    model_key_t model_key;
    model_key[0] = oc_w_id;
    model_key[1] = oc_d_id;
    model_key[2] = oc_c_id;
    model_key[3] = oc_o_id;
    return model_key;
  }

  static OrderCidIndexKey max() {
    OrderCidIndexKey max_k(0, 0, 0, 0);
    memset(&max_k, 255, sizeof(OrderCidIndexKey));
    return max_k;
  }
  static OrderCidIndexKey min() {
    OrderCidIndexKey min_k(0, 0, 0, 0);
    memset(&min_k, 0, sizeof(OrderCidIndexKey));
    return min_k;
  }

  OrderCidIndexKey to_str_key() const {
    if (little_endian()) {
      OrderCidIndexKey reverse(0, 0, 0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(OrderCidIndexKey); byte_i++) {
        ((char *)&reverse)[byte_i] =
            ((char *)this)[sizeof(OrderCidIndexKey) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }

  OrderCidIndexKey to_normal_key() const { return to_str_key(); }

  friend bool operator<(const OrderCidIndexKey &l, const OrderCidIndexKey &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const OrderCidIndexKey &l, const OrderCidIndexKey &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const OrderCidIndexKey &l, const OrderCidIndexKey &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const OrderCidIndexKey &l, const OrderCidIndexKey &r) {
    return !(l == r);
  }
  friend bool operator>=(const OrderCidIndexKey &l, const OrderCidIndexKey &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const OrderCidIndexKey &l, const OrderCidIndexKey &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct OrderCidIndexVal {
  uint8_t oc_dummy;
  friend bool operator<(const OrderCidIndexVal &l, const OrderCidIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OrderCidIndexVal)) < 0;
  }
  friend bool operator>(const OrderCidIndexVal &l, const OrderCidIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OrderCidIndexVal)) > 0;
  }
  friend bool operator==(const OrderCidIndexVal &l, const OrderCidIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OrderCidIndexVal)) == 0;
  }
  friend bool operator!=(const OrderCidIndexVal &l, const OrderCidIndexVal &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OrderCidIndexVal)) != 0;
  }
} PACKED;

struct C_key {
  uint8_t dummy[3];
  uint16_t c_id;
  uint8_t c_d_id;
  uint16_t c_w_id;

  C_key() : C_key(0, 0, 0) {}
  C_key(uint16_t ci, uint8_t di, uint16_t wi)
      : c_id(ci), c_d_id(di), c_w_id(wi) {
    for (size_t i = 0; i < 3; ++i) dummy[i] = 0;
  }

  typedef std::array<double, 3> model_key_t;
  static constexpr size_t model_key_size() { return 3; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = c_w_id;
    model_key[1] = c_d_id;
    model_key[2] = c_id;
    return model_key;
  }

  static C_key max() {
    C_key max_k(0, 0, 0);
    memset(&max_k, 255, sizeof(C_key));
    return max_k;
  }
  static C_key min() {
    C_key min_k(0, 0, 0);
    memset(&min_k, 0, sizeof(C_key));
    return min_k;
  }

  C_key to_str_key() const {
    if (little_endian()) {
      C_key reverse(0, 0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(C_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(C_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }

  C_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const C_key &l, const C_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const C_key &l, const C_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const C_key &l, const C_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const C_key &l, const C_key &r) { return !(l == r); }
  friend bool operator>=(const C_key &l, const C_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const C_key &l, const C_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct C_val {
  float c_discount;
  float c_credit_lim;
  float c_balance;
  float c_ytd_payment;
  uint32_t c_payment_cnt;
  uint32_t c_delivery_cnt;
  uint32_t c_since;
  varibale_str<2> c_credit;
  varibale_str<16> c_last;
  varibale_str<16> c_first;
  varibale_str<20> c_street_1;
  varibale_str<20> c_street_2;
  varibale_str<20> c_city;
  varibale_str<2> c_state;
  varibale_str<9> c_zip;
  varibale_str<16> c_phone;
  varibale_str<2> c_middle;
  varibale_str<500> c_data;
  // uint32_t o_c_id;
  // uint32_t o_carrier_id;
  // int8_t o_ol_cnt;
  // bool o_all_local;
  // uint32_t o_entry_d;
  friend bool operator<(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) < 0;
  }
  friend bool operator>(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) > 0;
  }
  friend bool operator==(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) == 0;
  }
  friend bool operator!=(const C_val &l, const C_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(C_val)) != 0;
  }
} PACKED;

struct H_key {
  uint16_t h_c_id;
  uint8_t h_c_d_id;
  uint16_t h_c_w_id;
  uint32_t h_date;
  uint8_t h_d_id;
  uint16_t h_w_id;

  H_key() : H_key(0, 0, 0, 0, 0, 0) {}
  H_key(uint16_t wi, uint8_t di, uint16_t cwi, uint8_t cdi, uint16_t ci,
        uint32_t ts)
      : h_c_id(ci),
        h_c_d_id(cdi),
        h_c_w_id(cwi),
        h_date(ts),
        h_d_id(di),
        h_w_id(wi) {}

  typedef std::array<double, 6> model_key_t;
  static constexpr size_t model_key_size() { return 6; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = h_w_id;
    model_key[1] = h_d_id;
    model_key[2] = h_date;
    model_key[3] = h_c_w_id;
    model_key[4] = h_c_d_id;
    model_key[5] = h_c_id;
    return model_key;
  }

  static H_key max() {
    H_key max_k;
    memset(&max_k, 255, sizeof(H_key));
    return max_k;
  }
  static H_key min() {
    H_key min_k;
    memset(&min_k, 0, sizeof(H_key));
    return min_k;
  }

  H_key to_str_key() const {
    if (little_endian()) {
      H_key reverse;
      for (size_t byte_i = 0; byte_i < sizeof(H_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(H_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  H_key to_normal_key() const { return to_str_key(); }

  bool smaller(const H_key &r) const {
    model_key_t k1 = to_model_key();
    model_key_t k2 = r.to_model_key();
    for (size_t i = 0; i < model_key_size(); ++i) {
      if (k1[i] > k2[i]) return false;
      if (k1[i] < k2[i]) return true;
    }
    return false;
  }

  friend bool operator<(const H_key &l, const H_key &r) {
    // return *((uint64_t *)&l) < *((uint64_t *)&r);
    return l.smaller(r);
  }
  friend bool operator>(const H_key &l, const H_key &r) {
    // return *((uint64_t *)&l) > *((uint64_t *)&r);
    return r.smaller(l);
  }
  friend bool operator==(const H_key &l, const H_key &r) {
    // return *((uint64_t *)&l) == *((uint64_t *)&r);
    return !l.smaller(r) && !r.smaller(l);
  }
  friend bool operator!=(const H_key &l, const H_key &r) { return !(l == r); }
  friend bool operator>=(const H_key &l, const H_key &r) {
    // return *((uint64_t *)&l) >= *((uint64_t *)&r);
    return !l.smaller(r);
  }
  friend bool operator<=(const H_key &l, const H_key &r) {
    // return *((uint64_t *)&l) <= *((uint64_t *)&r);
    return !r.smaller(l);
  }
} PACKED;

struct H_val {
  float h_amount;
  varibale_str<24> h_data;
  friend bool operator<(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) < 0;
  }
  friend bool operator>(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) > 0;
  }
  friend bool operator==(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) == 0;
  }
  friend bool operator!=(const H_val &l, const H_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(H_val)) != 0;
  }
} PACKED;

struct NO_key {
  uint8_t dummy[1];
  uint32_t no_o_id;
  uint8_t no_d_id;
  uint16_t no_w_id;

  NO_key() : NO_key(0, 0, 0) {}
  NO_key(uint16_t wi, uint8_t di, uint32_t oi)
      : no_o_id(oi), no_d_id(di), no_w_id(wi) {
    for (size_t i = 0; i < 1; ++i) dummy[i] = 0;
  }

  typedef std::array<double, 3> model_key_t;
  static constexpr size_t model_key_size() { return 3; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = no_w_id;
    model_key[1] = no_d_id;
    model_key[2] = no_o_id;
    return model_key;
  }

  static NO_key max() {
    NO_key max_k(0, 0, 0);
    memset(&max_k, 255, sizeof(NO_key));
    return max_k;
  }
  static NO_key min() {
    NO_key min_k(0, 0, 0);
    memset(&min_k, 0, sizeof(NO_key));
    return min_k;
  }

  NO_key to_str_key() const {
    if (little_endian()) {
      NO_key reverse(0, 0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(NO_key); byte_i++) {
        ((char *)&reverse)[byte_i] =
            ((char *)this)[sizeof(NO_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  NO_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const NO_key &l, const NO_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const NO_key &l, const NO_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const NO_key &l, const NO_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const NO_key &l, const NO_key &r) { return !(l == r); }
  friend bool operator>=(const NO_key &l, const NO_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const NO_key &l, const NO_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct NO_val {
  varibale_str<2> no_dummy;
  friend bool operator<(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) < 0;
  }
  friend bool operator>(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) > 0;
  }
  friend bool operator==(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) == 0;
  }
  friend bool operator!=(const NO_val &l, const NO_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(NO_val)) != 0;
  }
} PACKED;

struct O_key {
  uint8_t dummy[1];
  uint32_t o_id;
  uint8_t o_d_id;
  uint16_t o_w_id;

  O_key() : O_key(0, 0, 0) {}
  O_key(uint16_t wi, uint8_t di, uint32_t oi)
      : o_id(oi), o_d_id(di), o_w_id(wi) {
    for (size_t i = 0; i < 1; ++i) dummy[i] = 0;
  }

  typedef std::array<double, 3> model_key_t;
  static constexpr size_t model_key_size() { return 3; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = o_w_id;
    model_key[1] = o_d_id;
    model_key[2] = o_id;
    return model_key;
  }

  static O_key max() {
    O_key max_k(0, 0, 0);
    memset(&max_k, 255, sizeof(O_key));
    return max_k;
  }
  static O_key min() {
    O_key min_k(0, 0, 0);
    memset(&min_k, 0, sizeof(O_key));
    return min_k;
  }

  O_key to_str_key() const {
    if (little_endian()) {
      O_key reverse(0, 0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(O_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(O_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  O_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const O_key &l, const O_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const O_key &l, const O_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const O_key &l, const O_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const O_key &l, const O_key &r) { return !(l == r); }
  friend bool operator>=(const O_key &l, const O_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const O_key &l, const O_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct O_val {
  uint32_t o_c_id;
  uint32_t o_carrier_id;
  uint8_t o_ol_cnt;
  bool o_all_local;
  uint32_t o_entry_d;
  friend bool operator<(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) < 0;
  }
  friend bool operator>(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) > 0;
  }
  friend bool operator==(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) == 0;
  }
  friend bool operator!=(const O_val &l, const O_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(O_val)) != 0;
  }
} PACKED;

struct OL_key {
  uint8_t ol_number;
  uint32_t ol_o_id;
  uint8_t ol_d_id;
  uint16_t ol_w_id;

  OL_key() : OL_key(0, 0, 0, 0) {}
  OL_key(uint16_t ol_w_id, uint8_t ol_d_id, uint32_t ol_o_id, uint8_t ol_number)
      : ol_number(ol_number),
        ol_o_id(ol_o_id),
        ol_d_id(ol_d_id),
        ol_w_id(ol_w_id) {}

  typedef std::array<double, 4> model_key_t;
  static constexpr size_t model_key_size() { return 4; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = ol_w_id;
    model_key[1] = ol_d_id;
    model_key[2] = ol_o_id;
    model_key[3] = ol_number;
    return model_key;
  }

  static OL_key max() {
    OL_key max_k(0, 0, 0, 0);
    memset(&max_k, 255, sizeof(OL_key));
    return max_k;
  }
  static OL_key min() {
    OL_key min_k(0, 0, 0, 0);
    memset(&min_k, 0, sizeof(OL_key));
    return min_k;
  }

  OL_key to_str_key() const {
    if (little_endian()) {
      OL_key reverse(0, 0, 0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(OL_key); byte_i++) {
        ((char *)&reverse)[byte_i] =
            ((char *)this)[sizeof(OL_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  OL_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const OL_key &l, const OL_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const OL_key &l, const OL_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const OL_key &l, const OL_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const OL_key &l, const OL_key &r) { return !(l == r); }
  friend bool operator>=(const OL_key &l, const OL_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const OL_key &l, const OL_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct OL_val {
  uint32_t ol_i_id;
  uint32_t ol_delivery_d;
  float ol_amount;
  uint32_t ol_supply_w_id;
  uint8_t ol_quantity;
  friend bool operator<(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) < 0;
  }
  friend bool operator>(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) > 0;
  }
  friend bool operator==(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) == 0;
  }
  friend bool operator!=(const OL_val &l, const OL_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(OL_val)) != 0;
  }
} PACKED;

struct I_key {
  uint64_t i_id;
  I_key() : I_key(0) {}
  I_key(uint64_t i) : i_id(i) {}

  typedef std::array<double, 1> model_key_t;
  static constexpr size_t model_key_size() { return 1; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = i_id;
    return model_key;
  }

  static I_key max() {
    I_key max_k(0);
    memset(&max_k, 255, sizeof(I_key));
    return max_k;
  }
  static I_key min() {
    I_key min_k(0);
    memset(&min_k, 0, sizeof(I_key));
    return min_k;
  }

  I_key to_str_key() const {
    if (little_endian()) {
      I_key reverse(0);
      for (size_t byte_i = 0; byte_i < sizeof(I_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(I_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  I_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const I_key &l, const I_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const I_key &l, const I_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const I_key &l, const I_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const I_key &l, const I_key &r) { return !(l == r); }
  friend bool operator>=(const I_key &l, const I_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const I_key &l, const I_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct I_val {
  float i_price;
  varibale_str<24> i_name;
  varibale_str<50> i_data;
  uint32_t i_im_id;
  friend bool operator<(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) < 0;
  }
  friend bool operator>(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) > 0;
  }
  friend bool operator==(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) == 0;
  }
  friend bool operator!=(const I_val &l, const I_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(I_val)) != 0;
  }
} PACKED;

struct S_key {
  uint8_t dummy[2];
  uint32_t s_i_id;
  uint16_t s_w_id;

  S_key() : S_key(0, 0) {}
  S_key(uint16_t wi, uint32_t ii) : s_i_id(ii), s_w_id(wi) {
    for (size_t i = 0; i < 2; ++i) dummy[i] = 0;
  }

  typedef std::array<double, 2> model_key_t;
  static constexpr size_t model_key_size() { return 2; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = s_w_id;
    model_key[1] = s_i_id;
    return model_key;
  }

  static S_key max() {
    S_key max_k(0, 0);
    memset(&max_k, 255, sizeof(S_key));
    return max_k;
  }
  static S_key min() {
    S_key min_k(0, 0);
    memset(&min_k, 0, sizeof(S_key));
    return min_k;
  }

  S_key to_str_key() const {
    if (little_endian()) {
      S_key reverse(0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(S_key); byte_i++) {
        ((char *)&reverse)[byte_i] = ((char *)this)[sizeof(S_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  S_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const S_key &l, const S_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const S_key &l, const S_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const S_key &l, const S_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const S_key &l, const S_key &r) { return !(l == r); }
  friend bool operator>=(const S_key &l, const S_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const S_key &l, const S_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
  friend std::ostream &operator<<(std::ostream &os, const S_key &key) {
    os << "key [" << key.s_w_id << ", " << key.s_i_id << "]";
    return os;
  }
} PACKED;

struct S_val {
  uint32_t s_quantity;
  float s_ytd;
  uint32_t s_order_cnt;
  uint32_t s_remote_cnt;
  friend bool operator<(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) < 0;
  }
  friend bool operator>(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) > 0;
  }
  friend bool operator==(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) == 0;
  }
  friend bool operator!=(const S_val &l, const S_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_val)) != 0;
  }
} PACKED;

struct S_Data_key {
  uint8_t dummy[2];
  uint32_t s_i_id;
  uint16_t s_w_id;

  S_Data_key() : S_Data_key(0, 0) {}
  S_Data_key(uint16_t wi, uint32_t ii) : s_i_id(ii), s_w_id(wi) {
    for (size_t i = 0; i < 2; ++i) dummy[i] = 0;
  }

  typedef std::array<double, 2> model_key_t;
  static constexpr size_t model_key_size() { return 2; }

  model_key_t to_model_key() const {
    model_key_t model_key;
    model_key[0] = s_w_id;
    model_key[1] = s_i_id;
    return model_key;
  }

  static S_Data_key max() {
    S_Data_key max_k(0, 0);
    memset(&max_k, 255, sizeof(S_Data_key));
    return max_k;
  }
  static S_Data_key min() {
    S_Data_key min_k(0, 0);
    memset(&min_k, 0, sizeof(S_Data_key));
    return min_k;
  }

  S_Data_key to_str_key() const {
    if (little_endian()) {
      S_Data_key reverse(0, 0);
      for (size_t byte_i = 0; byte_i < sizeof(S_Data_key); byte_i++) {
        ((char *)&reverse)[byte_i] =
            ((char *)this)[sizeof(S_Data_key) - byte_i - 1];
      }
      return reverse;
    }
    return *this;
  }
  S_Data_key to_normal_key() const { return to_str_key(); }

  friend bool operator<(const S_Data_key &l, const S_Data_key &r) {
    return *((uint64_t *)&l) < *((uint64_t *)&r);
  }
  friend bool operator>(const S_Data_key &l, const S_Data_key &r) {
    return *((uint64_t *)&l) > *((uint64_t *)&r);
  }
  friend bool operator==(const S_Data_key &l, const S_Data_key &r) {
    return *((uint64_t *)&l) == *((uint64_t *)&r);
  }
  friend bool operator!=(const S_Data_key &l, const S_Data_key &r) {
    return !(l == r);
  }
  friend bool operator>=(const S_Data_key &l, const S_Data_key &r) {
    return *((uint64_t *)&l) >= *((uint64_t *)&r);
  }
  friend bool operator<=(const S_Data_key &l, const S_Data_key &r) {
    return *((uint64_t *)&l) <= *((uint64_t *)&r);
  }
} PACKED;

struct S_Data_val {
  varibale_str<50> s_data;
  varibale_str<24> s_dist_01;
  varibale_str<24> s_dist_02;
  varibale_str<24> s_dist_03;
  varibale_str<24> s_dist_04;
  varibale_str<24> s_dist_05;
  varibale_str<24> s_dist_06;
  varibale_str<24> s_dist_07;
  varibale_str<24> s_dist_08;
  varibale_str<24> s_dist_09;
  varibale_str<24> s_dist_10;
  friend bool operator<(const S_Data_val &l, const S_Data_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_Data_val)) < 0;
  }
  friend bool operator>(const S_Data_val &l, const S_Data_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_Data_val)) > 0;
  }
  friend bool operator==(const S_Data_val &l, const S_Data_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_Data_val)) == 0;
  }
  friend bool operator!=(const S_Data_val &l, const S_Data_val &r) {
    return memcmp((uint8_t *)&l, (uint8_t *)&r, sizeof(S_Data_val)) != 0;
  }
} PACKED;

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class fast_random {
 public:
  fast_random(unsigned long seed) : seed(0) { set_seed0(seed); }

  inline unsigned long next() {
    return ((unsigned long)next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() { return next(32); }

  inline uint16_t next_u16() { return next(16); }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return ((next(26) << 27) + next(27)) / (double)(1L << 53);
  }

  inline char next_char() { return next(8) % 256; }

  inline char next_readable_char() {
    static const char readables[] =
        "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_char();
    return s;
  }

  inline std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_readable_char();
    return s;
  }

  inline unsigned long get_seed() { return seed; }

  inline void set_seed(unsigned long seed) { this->seed = seed; }

 private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return seed >> (48 - bits);
  }

  unsigned long seed;
};

// TCZ: rename these function in snake case
static inline std::string RandomStr(fast_random &r, uint len) {
  // this is a property of the oltpbench implementation...
  if (!len) return "";

  uint i = 0;
  std::string buf(len - 1, 0);
  while (i < (len - 1)) {
    const char c = r.next_char();
    // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
    // is a less restrictive filter than isalnum()
    if (!isalnum(c)) continue;
    buf[i++] = c;
  }
  return buf;
}

static inline uint32_t GetCurrentTimeMillis() {
  // struct timeval tv;
  // ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
  // return tv.tv_sec * 1000;

  // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
  // for now, we just give each core an increasing number

  static __thread uint32_t tl_hack = 3001;
  return tl_hack++;
}

static inline std::string RandomNStr(fast_random &r, uint len) {
  const char base = '0';
  std::string buf(len, 0);
  for (uint i = 0; i < len; i++) buf[i] = (char)(base + (r.next() % 10));
  return buf;
}

static inline ALWAYS_INLINE int RandomNumber(fast_random &r, int min, int max) {
  int res = (int)(r.next_uniform() * (max - min + 1) + min);
  assert(res >= min && res <= max);
  return res;
}

static inline ALWAYS_INLINE int NonUniformRandom(fast_random &r, int A, int C,
                                                 int min, int max) {
  return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) %
          (max - min + 1)) +
         min;
}

std::string NameTokens[] = {
    std::string("BAR"),  std::string("OUGHT"), std::string("ABLE"),
    std::string("PRI"),  std::string("PRES"),  std::string("ESE"),
    std::string("ANTI"), std::string("CALLY"), std::string("ATION"),
    std::string("EING"),
};

// all tokens are at most 5 chars long
const size_t CustomerLastNameMaxSize = 5 * 3;

static inline size_t GetCustomerLastName(uint8_t *buf, fast_random &r,
                                         int num) {
  const std::string &s0 = NameTokens[num / 100];
  const std::string &s1 = NameTokens[(num / 10) % 10];
  const std::string &s2 = NameTokens[num % 10];
  uint8_t *const begin = buf;
  const size_t s0_sz = s0.size();
  const size_t s1_sz = s1.size();
  const size_t s2_sz = s2.size();
  memcpy(buf, s0.data(), s0_sz);
  buf += s0_sz;
  memcpy(buf, s1.data(), s1_sz);
  buf += s1_sz;
  memcpy(buf, s2.data(), s2_sz);
  buf += s2_sz;
  return buf - begin;
}

static inline std::string GetCustomerLastName(fast_random &r, int num) {
  std::string ret;
  ret.resize(CustomerLastNameMaxSize);
  ret.resize(GetCustomerLastName((uint8_t *)&ret[0], r, num));
  return ret;
}

static inline ALWAYS_INLINE std::string GetNonUniformCustomerLastNameLoad(
    fast_random &r) {
  return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
}

static inline ALWAYS_INLINE size_t
GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r) {
  return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
}

static inline ALWAYS_INLINE unsigned PickWarehouseId(fast_random &r,
                                                     unsigned start,
                                                     unsigned end) {
  const unsigned diff = end - start;
  if (diff == 1) return start;
  return (r.next() % diff) + start;
}

static inline ALWAYS_INLINE int GetCustomerId(fast_random &r) {
  return NonUniformRandom(r, 1023, 259, 1, num_customer_per_district);
}

static inline ALWAYS_INLINE int GetItemId(fast_random &r) {
  return g_uniform_item_dist ? RandomNumber(r, 1, num_item)
                             : NonUniformRandom(r, 8191, 7911, 1, num_item);
}

struct alignas(CACHELINE_SIZE) WorkerThreadParam {
  uint64_t throughput;
  uint32_t w_id_start;
  uint32_t w_id_end;
  uint32_t worker_id;
  uint64_t new_order_cycles;
  uint64_t new_order_tput;
  uint64_t payment_cycles;
  uint64_t payment_tput;
  uint64_t delivery_cycles;
  uint64_t delivery_tput;
  uint64_t order_status_cycles;
  uint64_t order_status_tput;
  uint64_t stock_level_cycles;
  uint64_t stock_level_tput;
  unsigned long seed;

  // Latency statistics - forward declare LatencyStats
  void* new_order_latency_stats;
  void* payment_latency_stats;
  void* delivery_latency_stats;
  void* order_status_latency_stats;
  void* stock_level_latency_stats;
};

#endif  // TPCC_H