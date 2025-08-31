#include "kv/masstree.h"
#include "kv/art_wrapper.h"

#include <map>
#include <random>


#ifndef TPCC_LOADER
#define TPCC_LOADER

/******************************************************************************
 * global kv reference
 *****************************************************************************/

static_assert(sizeof(W_key) == 8);
static_assert(sizeof(D_key) == 8);
static_assert(sizeof(C_key) == 8);
// static_assert(sizeof(H_key) == 8);
static_assert(sizeof(I_key) == 8);
static_assert(sizeof(S_key) == 8);
static_assert(sizeof(S_Data_key) == 8);
static_assert(sizeof(O_key) == 8);
static_assert(sizeof(NO_key) == 8);
static_assert(sizeof(OL_key) == 8);
static_assert(sizeof(OrderCidIndexKey) == 8);

#define LR_T(K) LinearModel<K::model_key_size()>
#define RMI_T(K) RMI<LR_T(K), LR_T(K)>

#define KV_T(K, V, OPT) MasstreeKV<K, V>
#define KV_DATA_LOADER load_masstree_kv_data
// #define KV_T(K, V, OPT) ARTKV<K, V>
// #define KV_DATA_LOADER load_art_kv_data

KV_T(W_key, W_val, false) * table_warehouse;             // in-place update
KV_T(D_key, D_val, false) * table_district;              // in-place update
KV_T(C_key, C_val, false) * table_customer;              // in-place update
KV_T(H_key, H_val, true) * table_history;                // seq insert-only
KV_T(I_key, I_val, false) * table_item;                  // read-only
KV_T(S_key, S_val, false) * table_stock;                 // in-place update
KV_T(S_Data_key, S_Data_val, false) * table_stock_data;  // no operation...
KV_T(O_key, O_val, true) * table_order;                  // insert
KV_T(NO_key, NO_val, true) * table_new_order;            // insert
KV_T(OL_key, OL_val, true) * table_order_line;           // insert
KV_T(CustomerNameIndexKey, CustomerNameIndexVal, false) *
    table_customer_index;  // scan only
KV_T(OrderCidIndexKey, OrderCidIndexVal, false) * table_order_index;  // insert

/******************************************************************************
 * data loaders
 *****************************************************************************/
// so tricky....
std::map<NO_key, NO_val> temp_no_map;
std::map<OL_key, OL_val> temp_ol_map;
std::map<OrderCidIndexKey, OrderCidIndexVal> temp_order_index_map;
std::map<CustomerNameIndexKey, CustomerNameIndexVal> temp_customer_index_map;

void load_warehouse(unsigned long seed, std::map<W_key, W_val> &kvmap) {
  fast_random r(seed);
  for (uint32_t i = 1; i <= warehouse_num; ++i) {
    W_key k(i);
    W_val v{};
    v.w_ytd = 300000;
    v.w_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
    v.w_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
    v.w_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
    v.w_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
    v.w_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
    v.w_state.assign(RandomStr(r, 3));
    v.w_zip.assign("123456789");
    kvmap.emplace(k, v);
  }
}

void load_district(unsigned long seed, std::map<D_key, D_val> &kvmap) {
  fast_random r(seed);
  for (uint32_t w_i = 1; w_i <= warehouse_num; ++w_i) {
    for (uint32_t d_i = 1; d_i <= num_district_per_warehouse; ++d_i) {
      D_key k(w_i, d_i);
      D_val v{};
      v.d_ytd = 30000;
      v.d_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
      v.d_next_o_id = 3001;
      v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
      v.d_street1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
      v.d_street2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
      v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
      v.d_state.assign(RandomStr(r, 3));
      v.d_zip.assign("123456789");

      kvmap.emplace(k, v);
    }
  }
}

void load_customer(unsigned long seed, std::map<C_key, C_val> &kvmap) {
  temp_customer_index_map.clear();
  fast_random r(seed);
  for (uint32_t w_i = 1; w_i <= warehouse_num; ++w_i) {
    for (uint32_t d_i = 1; d_i <= num_district_per_warehouse; ++d_i) {
      for (uint32_t c_i = 1; c_i <= num_customer_per_district; ++c_i) {
        C_key k(c_i, d_i, w_i);

        C_val v{};
        v.c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
        if (RandomNumber(r, 1, 100) <= 10)
          v.c_credit.assign("BC");
        else
          v.c_credit.assign("GC");

        std::string last_name;
        if (c_i <= 1000)
          last_name = GetCustomerLastName(r, c_i - 1);
        else
          last_name = GetNonUniformCustomerLastNameLoad(r);
        v.c_last.assign(last_name);

        std::string first_name = RandomStr(r, RandomNumber(r, 8, 16));
        v.c_first.assign(first_name);

        v.c_credit_lim = 50000;

        v.c_balance = -10;
        v.c_ytd_payment = 10;
        v.c_payment_cnt = 1;
        v.c_delivery_cnt = 0;

        v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.c_state.assign(RandomStr(r, 3));
        v.c_zip.assign(RandomNStr(r, 4) + "11111");
        v.c_phone.assign(RandomNStr(r, 16));
        v.c_since = GetCurrentTimeMillis();
        v.c_middle.assign("OE");
        v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));
        // v.o_c_id = RandomNumber(r, 1, num_customer_per_district);
        // v.o_carrier_id = 0;
        // v.o_ol_cnt = RandomNumber(r, 5, 15);
        // v.o_all_local = 1;
        // v.o_entry_d = GetCurrentTimeMillis();
        kvmap.emplace(k, v);

        // secondary index on customer's last name
        CustomerNameIndexKey name_index_key{};
        name_index_key.ci_w_id = w_i;
        name_index_key.ci_d_id = d_i;
        name_index_key.ci_last.assign(last_name);
        name_index_key.ci_first.assign(first_name);

        CustomerNameIndexVal name_index_val{};
        name_index_val.ci_c_id = c_i;
        temp_customer_index_map.emplace(name_index_key, name_index_val);
      }
    }
  }

  // struct cmputil {
  //   bool operator()(const C_key &a, const C_key &b) const {
  //     return a.c_w_id < b.c_w_id
  //                ? true
  //                : a.c_d_id < b.c_d_id ? true : a.c_id < b.c_id ? true :
  //                false;
  //   }
  // };

  // std::map<C_key, C_val, cmputil> temp_map;
  // for (auto iter = kvmap.begin(); iter != kvmap.end(); iter++) {
  //   temp_map.emplace(iter->first, iter->second);
  // }

  // auto iter1 = kvmap.begin();
  // auto iter2 = kvmap.begin();
  // while (iter1 != kvmap.end()) {
  //   INVARIANT(iter1->first.c_w_id == iter2->first.c_w_id);
  //   INVARIANT(iter1->first.c_id == iter2->first.c_id);
  //   INVARIANT(iter1->first.c_d_id == iter2->first.c_d_id);
  //   iter1 ++;
  //   iter2 ++;
  // }
}

void load_customer_index(
    unsigned long seed,
    std::map<CustomerNameIndexKey, CustomerNameIndexVal> &kvmap) {
  assert(temp_customer_index_map.size() > 0);
  for (auto iter = temp_customer_index_map.begin();
       iter != temp_customer_index_map.end(); iter++) {
    kvmap.emplace(iter->first, iter->second);
  }
  temp_customer_index_map.clear();
}

void load_history(unsigned long seed, std::map<H_key, H_val> &kvmap) {
  fast_random r(seed);
  for (uint32_t w_i = 1; w_i <= warehouse_num; ++w_i) {
    for (uint32_t d_i = 1; d_i <= num_district_per_warehouse; ++d_i) {
      uint32_t ts = 0;
      for (uint32_t c_i = 1; c_i <= num_customer_per_district; ++c_i) {
        ts++;
        H_key h_k(w_i, d_i, w_i, d_i, c_i, ts);
        // H_key h_k;

        H_val h_v{};
        // h_v.h_c_id = c_i;
        // h_v.h_c_d_id = d_i;
        // h_v.h_c_w_id = w_i;
        // h_v.h_d_id = d_i;
        // h_v.h_w_id = w_i;
        // h_v.h_date = ts;
        h_v.h_amount = 10;
        h_v.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

        kvmap.emplace(h_k, h_v);
      }
    }
  }
}

void load_item(unsigned long seed, std::map<I_key, I_val> &kvmap) {
  fast_random r(seed);
  for (uint32_t i = 1; i <= num_item; i++) {
    I_key k(i);

    I_val v;
    v.i_name.assign(RandomStr(r, RandomNumber(r, 14, 24)));
    v.i_price = (float)RandomNumber(r, 100, 10000) / 100.0;
    const int len = RandomNumber(r, 26, 50);
    if (RandomNumber(r, 1, 100) > 10) {
      v.i_data.assign(RandomStr(r, len));
    } else {
      const int startOriginal = RandomNumber(r, 2, (len - 8));
      v.i_data.assign(RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                      RandomStr(r, len - startOriginal - 7));
    }
    v.i_im_id = RandomNumber(r, 1, 10000);

    kvmap.emplace(k, v);
  }
}

void load_stock(unsigned long seed, std::map<S_key, S_val> &kvmap) {
  fast_random r(seed);
  for (uint32_t w_i = 1; w_i <= warehouse_num; ++w_i) {
    for (uint32_t i_i = 1; i_i <= num_item; ++i_i) {
      S_key k(w_i, i_i);
      S_val v;
      v.s_quantity = RandomNumber(r, 10, 100);
      v.s_ytd = 0;
      v.s_order_cnt = 0;
      v.s_remote_cnt = 0;
      kvmap.emplace(k, v);
    }
  }
}

void load_stock_data(unsigned long seed,
                     std::map<S_Data_key, S_Data_val> &kvmap) {
  fast_random r(seed);
  for (uint32_t w_i = 1; w_i <= warehouse_num; ++w_i) {
    for (uint32_t i_i = 1; i_i <= num_item; ++i_i) {
      S_Data_key k(w_i, i_i);

      S_Data_val v;
      const int len = RandomNumber(r, 26, 50);
      if (RandomNumber(r, 1, 100) > 10) {
        v.s_data.assign(RandomStr(r, len));
      } else {
        const int startOriginal = RandomNumber(r, 2, (len - 8));
        v.s_data.assign(RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                        RandomStr(r, len - startOriginal - 7));
      }
      v.s_dist_01.assign(RandomStr(r, 24));
      v.s_dist_02.assign(RandomStr(r, 24));
      v.s_dist_03.assign(RandomStr(r, 24));
      v.s_dist_04.assign(RandomStr(r, 24));
      v.s_dist_05.assign(RandomStr(r, 24));
      v.s_dist_06.assign(RandomStr(r, 24));
      v.s_dist_07.assign(RandomStr(r, 24));
      v.s_dist_08.assign(RandomStr(r, 24));
      v.s_dist_09.assign(RandomStr(r, 24));
      v.s_dist_10.assign(RandomStr(r, 24));

      kvmap.emplace(k, v);
    }
  }
}

void load_order(unsigned long seed, std::map<O_key, O_val> &kvmap) {
  temp_no_map.clear();
  temp_ol_map.clear();
  temp_order_index_map.clear();
  fast_random r(seed);
  uint32_t ts = 0;
  for (uint32_t w_i = 1; w_i <= warehouse_num; w_i++) {
    for (uint32_t d_i = 1; d_i <= num_district_per_warehouse; d_i++) {
      // o_c_id selected sequentially from a random permutation of [1 .. 3,000]
      // std::set<uint> c_ids_s;
      // std::vector<uint> c_ids;
      // while (c_ids.size() != num_customer_per_district) {
      //   const auto x = (r.next() % num_customer_per_district) + 1;
      //   if (c_ids_s.count(x)) continue;
      //   c_ids_s.insert(x);
      //   c_ids.emplace_back(x);
      // }
      std::vector<uint32_t> c_ids;
      for (size_t i = 1; i <= num_customer_per_district; ++i) {
        c_ids.push_back(i);
      }
      std::shuffle(c_ids.begin(), c_ids.end(), std::default_random_engine());
      for (uint32_t o_i = 1; o_i <= num_customer_per_district; o_i++) {
        ts++;
        // order table
        O_key k_o(w_i, d_i, o_i);
        O_val v_o{};
        v_o.o_c_id = c_ids[o_i - 1];
        if (k_o.o_id < 2101)
          v_o.o_carrier_id = RandomNumber(r, 1, 10);
        else
          v_o.o_carrier_id = 0;
        v_o.o_ol_cnt = RandomNumber(r, 5, 15);
        v_o.o_all_local = 1;
        v_o.o_entry_d = ts;
        kvmap.emplace(k_o, v_o);

        // order secondary index
        OrderCidIndexKey order_index_key(w_i, d_i, v_o.o_c_id, o_i);
        OrderCidIndexVal order_index_val{0};
        temp_order_index_map.emplace(order_index_key, order_index_val);

        // 900 rows in the NEW-ORDER table corresponding
        // to the last 900 rows in the ORDER
        if (o_i >= 2101) {
          NO_key k_no(w_i, d_i, o_i);
          NO_val v_no{};
          v_no.no_dummy.assign("a");
          temp_no_map.emplace(k_no, v_no);
        }

        // order line
        for (uint32_t l_i = 1; l_i <= v_o.o_ol_cnt; l_i++) {
          OL_key k_ol(w_i, d_i, o_i, l_i);
          OL_val v_ol;
          v_ol.ol_i_id = RandomNumber(r, 1, 100000);
          if (k_ol.ol_o_id < 2101) {
            v_ol.ol_delivery_d = v_o.o_entry_d;
            v_ol.ol_amount = 0;
          } else {
            v_ol.ol_delivery_d = 0;
            v_ol.ol_amount = (float)(RandomNumber(r, 1, 999999) / 100.0);
          }
          v_ol.ol_supply_w_id = k_ol.ol_w_id;
          v_ol.ol_quantity = 5;
          temp_ol_map.emplace(k_ol, v_ol);
        }
      }
    }
  }
}

void load_new_order(unsigned long seed, std::map<NO_key, NO_val> &kvmap) {
  assert(!temp_no_map.empty());
  auto iter = temp_no_map.begin();
  size_t key_i = 0;
  while (key_i < temp_no_map.size()) {
    kvmap.emplace(iter->first, iter->second);
    iter++;
    key_i++;
  }
  temp_no_map.clear();
}

void load_online_order(unsigned long seed, std::map<OL_key, OL_val> &kvmap) {
  assert(temp_ol_map.size() > 3000);
  auto iter = temp_ol_map.begin();
  size_t key_i = 0;
  while (key_i < temp_ol_map.size()) {
    kvmap.emplace(iter->first, iter->second);
    iter++;
    key_i++;
  }
  temp_ol_map.clear();
}

void load_order_index(unsigned long seed,
                      std::map<OrderCidIndexKey, OrderCidIndexVal> &kvmap) {
  assert(temp_order_index_map.size() > 0);
  for (auto iter = temp_order_index_map.begin();
       iter != temp_order_index_map.end(); iter++) {
    kvmap.emplace(iter->first, iter->second);
  }
  temp_order_index_map.clear();
}

/******************************************************************************
 * data init functions
 *****************************************************************************/
enum class TableType {
  Warehouse = 0,
  District,
  Customer,
  History,
  Item,
  Stock,
  StockData,
  Order,
  NewOrder,
  OrderLine,
  CustomerIndex,
  OrderIndex
};

template <typename K, typename V>
void load_masstree_kv_data(TableType tab_type, unsigned long seed,
                           threadinfo *main_ti, MasstreeKV<K, V> *&table,
                           void (*load)(unsigned long, std::map<K, V> &),
                           size_t worker_num, int cxl_percentage,
                           uint64_t max_local_memory_usage) {
  std::map<K, V> kvmap;
  load(seed, kvmap);
  COUT_THIS("[TPC-C]: table_size=" << kvmap.size());
  query<row_type> q;

  table = new MasstreeKV<K, V>(main_ti, cxl_percentage, max_local_memory_usage);
  for (auto iter = kvmap.begin(); iter != kvmap.end(); iter++) {
    bool res = table->insert(iter->first, iter->second, main_ti, q,
                             0 /* worker_id, as placeholder */);
    INVARIANT(res);
  }
}

template <typename K, typename V>
void load_art_kv_data(TableType tab_type, unsigned long seed,
                           threadinfo *main_ti, ARTKV<K, V> *&table,
                           void (*load)(unsigned long, std::map<K, V> &),
                           size_t worker_num, int cxl_percentage,
                           uint64_t max_local_memory_usage) {
  std::map<K, V> kvmap;
  load(seed, kvmap);
  COUT_THIS("[TPC-C]: table_size=" << kvmap.size());
  query<row_type> q;

  table = new ARTKV<K, V>(cxl_percentage, max_local_memory_usage);
  for (auto iter = kvmap.begin(); iter != kvmap.end(); iter++) {
    bool res = table->insert(iter->first, iter->second, main_ti, q,
                             0 /* worker_id, as placeholder */);
    INVARIANT(res);
  }
}

template <typename K>
void partition_keys(std::vector<K> &keys, std::vector<size_t> &offsets) {}

template <>
void partition_keys<O_key>(std::vector<O_key> &keys,
                           std::vector<size_t> &offsets) {
  INVARIANT(keys.size() > 0);
  size_t cur_dist = 0;
  for (size_t pos_i = 1; pos_i < keys.size(); ++pos_i) {
    if (pos_i > 0) INVARIANT(keys[pos_i] > keys[pos_i - 1]);
    if (keys[pos_i].o_d_id != cur_dist) {
      cur_dist = keys[pos_i].o_d_id;
      offsets.push_back(pos_i);
    }
  }
  INVARIANT(offsets.size() == warehouse_num * num_district_per_warehouse);
}

template <>
void partition_keys<NO_key>(std::vector<NO_key> &keys,
                            std::vector<size_t> &offsets) {
  INVARIANT(keys.size() > 0);
  size_t cur_dist = 0;
  for (size_t pos_i = 1; pos_i < keys.size(); ++pos_i) {
    if (pos_i > 0) INVARIANT(keys[pos_i] > keys[pos_i - 1]);
    if (keys[pos_i].no_d_id != cur_dist) {
      cur_dist = keys[pos_i].no_d_id;
      offsets.push_back(pos_i);
    }
  }
  INVARIANT(offsets.size() == warehouse_num * num_district_per_warehouse);
}

template <>
void partition_keys<OL_key>(std::vector<OL_key> &keys,
                            std::vector<size_t> &offsets) {
  INVARIANT(keys.size() > 0);
  size_t cur_dist = 0;
  for (size_t pos_i = 1; pos_i < keys.size(); ++pos_i) {
    if (pos_i > 0) INVARIANT(keys[pos_i] > keys[pos_i - 1]);
    if (keys[pos_i].ol_d_id != cur_dist) {
      cur_dist = keys[pos_i].ol_d_id;
      offsets.push_back(pos_i);
    }
  }
  INVARIANT(offsets.size() == warehouse_num * num_district_per_warehouse);
}

template <>
void partition_keys<H_key>(std::vector<H_key> &keys,
                           std::vector<size_t> &offsets) {
  INVARIANT(keys.size() > 0);
  size_t cur_dist = 0;
  for (size_t pos_i = 1; pos_i < keys.size(); ++pos_i) {
    if (pos_i > 0) INVARIANT(keys[pos_i] > keys[pos_i - 1]);
    if (keys[pos_i].h_d_id != cur_dist) {
      cur_dist = keys[pos_i].h_d_id;
      offsets.push_back(pos_i);
    }
  }
  INVARIANT(offsets.size() == warehouse_num * num_district_per_warehouse);
}


void prepare_kvs(threadinfo *main_ti, size_t worker_num) {
  COUT_THIS("[TPC-C LOADER]: loading warehouse");
  KV_DATA_LOADER<W_key, W_val>(TableType::Warehouse, 9394, main_ti,
                               table_warehouse, load_warehouse, worker_num, 0, warehouse_max_local_memory_usage);
  COUT_THIS("[TPC-C LOADER]: loading district");
  KV_DATA_LOADER<D_key, D_val>(TableType::District, 129856349, main_ti,
                               table_district, load_district, worker_num, 0, district_max_local_memory_usage);
  COUT_THIS("[TPC-C LOADER]: loading customer");
  KV_DATA_LOADER<C_key, C_val>(TableType::Customer, 923587856425, main_ti,
                               table_customer, load_customer, worker_num, cxl_percentage, customer_max_local_memory_usage);
  // COUT_THIS("[TPC-C LOADER]: loading customer index");
  // KV_DATA_LOADER<CustomerNameIndexKey,
  // CustomerNameIndexVal>(TableType::CustomerIndex,
  //     923587856425, main_ti, table_customer_index, load_customer_index,
  //     worker_num);
  COUT_THIS("[TPC-C LOADER]: loading history");
  KV_DATA_LOADER<H_key, H_val>(TableType::History, 923587856425, main_ti,
                               table_history, load_history, worker_num, cxl_percentage, history_max_local_memory_usage);
  COUT_THIS("[TPC-C LOADER]: loading item");
  KV_DATA_LOADER<I_key, I_val>(TableType::Item, 235443, main_ti, table_item,
                               load_item, worker_num, 0, item_max_local_memory_usage);
  COUT_THIS("[TPC-C LOADER]: loading stock");
  KV_DATA_LOADER<S_key, S_val>(TableType::Stock, 89785943, main_ti, table_stock,
                               load_stock, worker_num, cxl_percentage, stock_max_local_memory_usage);
  // COUT_THIS("[TPC-C LOADER]: loading stock_data");
  // KV_DATA_LOADER<S_Data_key, S_Data_val>(TableType::StockData, 89785943,
  //                                        main_ti, table_stock_data,
  //                                        load_stock_data, worker_num);
  // order relate
  COUT_THIS("[TPC-C LOADER]: loading order");
  KV_DATA_LOADER<O_key, O_val>(TableType::Order, 2343352, main_ti, table_order,
                               load_order, worker_num, cxl_percentage, order_max_local_memory_usage);
  COUT_THIS("[TPC-C LOADER]: loading new_order");
  KV_DATA_LOADER<NO_key, NO_val>(TableType::NewOrder, 2343352, main_ti,
                                 table_new_order, load_new_order, worker_num, cxl_percentage, new_order_max_local_memory_usage);
  COUT_THIS("[TPC-C LOADER]: loading order_line");
  KV_DATA_LOADER<OL_key, OL_val>(TableType::OrderLine, 2343352, main_ti,
                                 table_order_line, load_online_order,
                                 worker_num, cxl_percentage, order_line_max_local_memory_usage);
  // COUT_THIS("[TPC-C LOADER]: loading order index");
  // KV_DATA_LOADER<OrderCidIndexKey, OrderCidIndexVal>(
  //     TableType::OrderIndex, 2343352, main_ti, table_order_index,
  //     load_order_index, worker_num);
}

#endif  // TPCC_LOADER