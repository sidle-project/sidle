#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <set>
#include <string>

#include "helper.h"
#include "tpcc.h"
#include "tpcc_loader.h"

/******************************************************************************
 * TPC-C Read Operations
 *****************************************************************************/

std::vector<std::vector<uint32_t>> next_o_id_hack_info_;
std::vector<std::vector<uint32_t>> next_ol_number_hack_info_;

typedef void (*op_func_t)(fast_random &, threadinfo *, query<row_type> &,
                          uint32_t);

struct alignas(CACHELINE_SIZE) OpWorkerParam {
  uint64_t throughput;
  uint32_t worker_id;
  uint32_t thread_id;
  unsigned long seed;
  op_func_t runner;
};

void op_warehouse_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                      uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  W_key w_k(warehouse_id);
  W_val w_v;
  volatile bool res = table_warehouse->get(w_k, w_v, ti, q, worker_id);
  INVARIANT(res);
}

void op_district_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                     uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  D_key d_k(warehouse_id, district_id);
  D_val d_v;
  volatile bool res = table_district->get(d_k, d_v, ti, q, worker_id);
  INVARIANT(res);
}

void op_customer_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                     uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t customer_id = GetCustomerId(r);

  C_key c_k(customer_id, district_id, warehouse_id);
  C_val c_v;
  volatile bool res = table_customer->get(c_k, c_v, ti, q, worker_id);
  INVARIANT(res);
}

void op_item_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                 uint32_t worker_id) {
  I_key k_i(GetItemId(r));
  I_val v_i;
  volatile bool res = table_item->get(k_i, v_i, ti, q, worker_id);
  INVARIANT(res);
}

void op_stock_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                  uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t item_id = GetItemId(r);
  S_key k_s(warehouse_id, item_id);
  S_val v_s;
  volatile bool res = table_stock->get(k_s, v_s, ti, q, worker_id);
  INVARIANT(res);
}

void op_order_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                  uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t o_id = RandomNumber(r, 1, num_customer_per_district);
  O_key k_o(warehouse_id, district_id, o_id);
  O_val v_o;
  volatile bool res = table_order->get(k_o, v_o, ti, q, worker_id);
  INVARIANT(res);
}

void op_new_order_get(fast_random &r, threadinfo *ti, query<row_type> &q,
                      uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t o_id = RandomNumber(r, 2101, num_customer_per_district);
  NO_key k_no(warehouse_id, district_id, o_id);
  NO_val v_no;
  volatile bool res = table_new_order->get(k_no, v_no, ti, q, worker_id);
  INVARIANT(res);
}

/******************************************************************************
 * TPC-C Update Operations
 *****************************************************************************/

void op_district_update(fast_random &r, threadinfo *ti, query<row_type> &q,
                        uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t dstrict_id = RandomNumber(r, 1, num_district_per_warehouse);

  D_key k(warehouse_id, dstrict_id);
  D_val v;
  v.d_ytd = 30000;
  v.d_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
  v.d_next_o_id = 3001;
  v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
  v.d_street1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
  v.d_street2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
  v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
  v.d_state.assign(RandomStr(r, 3));
  v.d_zip.assign("123456789");

  volatile bool res = table_district->insert(k, v, ti, q, worker_id);
  INVARIANT(res);
}

void op_stock_update(fast_random &r, threadinfo *ti, query<row_type> &q,
                     uint32_t worker_id) {
  uint32_t ol_supply_w_id = RandomNumber(r, 1, warehouse_num);
  uint32_t item_id = RandomNumber(r, 1, num_item);
  S_key k_s(ol_supply_w_id, item_id);
  S_val v_s;
  v_s.s_quantity = RandomNumber(r, 10, 100);
  v_s.s_ytd = float(RandomNumber(r, 1, 10000));
  v_s.s_order_cnt = RandomNumber(r, 10, 100);
  v_s.s_remote_cnt = RandomNumber(r, 10, 100);

  volatile bool res = table_stock->insert(k_s, v_s, ti, q, worker_id);
  INVARIANT(res);
}

void op_warehouse_udpate(fast_random &r, threadinfo *ti, query<row_type> &q,
                         uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  W_key k(warehouse_id);
  W_val v;
  v.w_ytd = 300000;
  v.w_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
  v.w_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
  v.w_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
  v.w_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
  v.w_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
  v.w_state.assign(RandomStr(r, 3));
  v.w_zip.assign("123456789");

  volatile bool res = table_warehouse->insert(k, v, ti, q, worker_id);
  INVARIANT(res);
}

void op_customer_udpate(fast_random &r, threadinfo *ti, query<row_type> &q,
                        uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t customer_id = RandomNumber(r, 1, num_customer_per_district);
  uint32_t dstrict_id = RandomNumber(r, 1, num_district_per_warehouse);

  C_key k(customer_id, dstrict_id, warehouse_id);
  C_val v;
  v.c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
  if (RandomNumber(r, 1, 100) <= 10)
    v.c_credit.assign("BC");
  else
    v.c_credit.assign("GC");

  std::string last_name = GetNonUniformCustomerLastNameLoad(r);
  std::string first_name = RandomStr(r, RandomNumber(r, 8, 16));

  v.c_last.assign(last_name);
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

  volatile bool res = table_customer->insert(k, v, ti, q, worker_id);
  INVARIANT(res);
}

void op_order_udpate(fast_random &r, threadinfo *ti, query<row_type> &q,
                     uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t customer_id = GetCustomerId(r);
  uint32_t o_id = RandomNumber(r, 1, num_customer_per_district);
  uint32_t num_items = RandomNumber(r, 5, 15);

  O_key k_o(warehouse_id, district_id, o_id);
  O_val v_o;
  v_o.o_c_id = customer_id;
  v_o.o_carrier_id = RandomNumber(r, 1, num_district_per_warehouse);
  v_o.o_ol_cnt = (uint8_t)num_items;
  v_o.o_all_local = 1;
  v_o.o_entry_d = 0;

  volatile bool res = table_order->insert(k_o, v_o, ti, q, worker_id);
  INVARIANT(res);
}

void op_order_line_update(fast_random &r, threadinfo *ti, query<row_type> &q,
                          uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t o_id = RandomNumber(r, 1, num_customer_per_district);
  uint32_t ol_number = RandomNumber(r, 1, 5);  // use [1,5] to ensure the key
                                               // exist RandomNumber(r, 1, 10);

  OL_key k_ol(warehouse_id, district_id, o_id, ol_number);
  OL_val v_ol;
  v_ol.ol_i_id = uint32_t(GetItemId(r));
  v_ol.ol_delivery_d = 1;
  v_ol.ol_amount =
      float(RandomNumber(r, 1, std::numeric_limits<uint32_t>::max() - 1));
  v_ol.ol_supply_w_id = uint32_t(warehouse_id);
  v_ol.ol_quantity = warehouse_id;

  volatile bool res = table_order_line->insert(k_ol, v_ol, ti, q, worker_id);
  INVARIANT(res);
}

/******************************************************************************
 * TPC-C Insert Operations
 *****************************************************************************/

void op_new_order_insert(fast_random &r, threadinfo *ti, query<row_type> &q,
                         uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t next_o_id = next_o_id_hack_info_[warehouse_id - 1][district_id - 1];
  next_o_id_hack_info_[warehouse_id - 1][district_id - 1]++;

  NO_key k_no(warehouse_id, district_id, next_o_id);
  NO_val v_no;
  v_no.no_dummy.assign("no");
  volatile bool res = table_new_order->insert(k_no, v_no, ti, q, worker_id);
  INVARIANT(res);
}

void op_order_insert(fast_random &r, threadinfo *ti, query<row_type> &q,
                     uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t customer_id = GetCustomerId(r);
  uint32_t num_items = RandomNumber(r, 5, 15);
  uint32_t next_o_id = next_o_id_hack_info_[warehouse_id - 1][district_id - 1];
  next_o_id_hack_info_[warehouse_id - 1][district_id - 1]++;

  O_key k_o(warehouse_id, district_id, next_o_id);
  O_val v_o;
  v_o.o_c_id = customer_id;
  v_o.o_carrier_id = RandomNumber(r, 1, num_district_per_warehouse);
  v_o.o_ol_cnt = (uint8_t)num_items;
  v_o.o_all_local = 1;
  v_o.o_entry_d = 0;

  volatile bool res = table_order->insert(k_o, v_o, ti, q, worker_id);
  INVARIANT(res);
}

// increase ol_number before increase o_id
void op_order_line_insert(fast_random &r, threadinfo *ti, query<row_type> &q,
                          uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t item_num = RandomNumber(r, 5, 15);
  uint32_t next_o_id = next_o_id_hack_info_[warehouse_id - 1][district_id - 1];
  uint32_t ol_number =
      next_ol_number_hack_info_[warehouse_id - 1][district_id - 1];

  if (ol_number > item_num) {
    next_o_id_hack_info_[warehouse_id - 1][district_id - 1]++;
    next_ol_number_hack_info_[warehouse_id - 1][district_id - 1] = 0;
  } else {
    next_ol_number_hack_info_[warehouse_id - 1][district_id - 1]++;
  }

  OL_key k_ol(warehouse_id, district_id, next_o_id, ol_number);
  OL_val v_ol;
  v_ol.ol_i_id = uint32_t(GetItemId(r));
  v_ol.ol_delivery_d = 0;  // not delivered yet
  v_ol.ol_amount =
      float(RandomNumber(r, 1, std::numeric_limits<uint32_t>::max() - 1));
  v_ol.ol_supply_w_id = uint32_t(warehouse_id);
  v_ol.ol_quantity = warehouse_id;

  volatile bool res = table_order_line->insert(k_ol, v_ol, ti, q, worker_id);
  INVARIANT(res);
}

void op_history_insert(fast_random &r, threadinfo *ti, query<row_type> &q,
                       uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t customer_warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t customer_dstrict_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
  uint32_t ts = GetCurrentTimeMillis();
  uint32_t customer_id = GetCustomerId(r);
  H_key k_h(warehouse_id, district_id, customer_warehouse_id,
            customer_dstrict_id, customer_id, ts);
  // H_key k_h;
  H_val v_h;
  // v_h.h_c_id = customer_id;
  // v_h.h_c_d_id = customer_dstrict_id;
  // v_h.h_c_w_id = customer_warehouse_id;
  // v_h.h_d_id = district_id;
  // v_h.h_w_id = warehouse_id;
  // v_h.h_date = s;
  v_h.h_amount = paymentAmount;
  v_h.h_data.assign(RandomStr(r, RandomNumber(r, 12, 20)));

  volatile bool res = table_history->insert(k_h, v_h, ti, q, worker_id);
  INVARIANT(res);
}

// FIXME here may remove a non-exist key, which is not what we expected.
void op_new_order_remove(fast_random &r, threadinfo *ti, query<row_type> &q,
                         uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t no_o_id =
      RandomNumber(r, 1, std::numeric_limits<uint32_t>::max() - 1);

  NO_key k_no_min(warehouse_id, district_id, no_o_id);
  volatile bool res = table_new_order->remove(k_no_min, ti, q, worker_id);
  UNUSED(res);
}

/*
void op_order_cid_index_insert(fast_random &r, threadinfo *ti,
                               query<row_type> &q, uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t customer_id = GetCustomerId(r);
  uint32_t num_items = RandomNumber(r, 5, 15);
  uint32_t next_o_id =
      next_o_id_hack_info_[warehouse_id - 1][district_id - 1];
  next_o_id_hack_info_[warehouse_id - 1][district_id - 1]++;

  OrderCidIndexKey k_o_idx(warehouse_id, district_id, customer_id, next_o_id);
  OrderCidIndexVal v_o_idx{11};
  volatile bool res =
      table_order_index->insert(k_o_idx, v_o_idx, ti, q, worker_id);
  INVARIANT(res);
}
*/

/******************************************************************************
 * TPC-C Scan Operations
 *****************************************************************************/

void op_order_line_scan_n_short(fast_random &r, threadinfo *ti,
                                query<row_type> &q, uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t newest_o_c_id = RandomNumber(r, 1, num_customer_per_district);

  OL_key k_ol_lower(warehouse_id, district_id, newest_o_c_id, 0);

  std::vector<std::pair<OL_key, OL_val>> kvs_ol;
  volatile size_t ret =
      table_order_line->scan(k_ol_lower, 10, kvs_ol, ti, q, worker_id);
  INVARIANT(ret >= 0 && ret == kvs_ol.size());
}

void op_order_line_scan_n_long(fast_random &r, threadinfo *ti,
                               query<row_type> &q, uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t cur_next_o_id = RandomNumber(r, 1, num_customer_per_district);
  uint32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;

  OL_key k_ol_lower(warehouse_id, district_id, lower, 0);

  std::vector<std::pair<OL_key, OL_val>> kvs_ol;
  volatile size_t ret =
      table_order_line->scan(k_ol_lower, 200, kvs_ol, ti, q, worker_id);
  INVARIANT(ret >= 0 && ret == kvs_ol.size());
}

void op_order_line_range_scan_short(fast_random &r, threadinfo *ti,
                                    query<row_type> &q, uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t newest_o_c_id = RandomNumber(r, 1, num_customer_per_district);

  OL_key k_ol_lower(warehouse_id, district_id, newest_o_c_id, 0);
  OL_key k_ol_upper(warehouse_id, district_id, newest_o_c_id,
                    std::numeric_limits<uint16_t>::max());

  std::vector<std::pair<OL_key, OL_val>> kvs_ol;
  volatile size_t ret = table_order_line->range_scan(k_ol_lower, k_ol_upper,
                                                     kvs_ol, ti, q, worker_id);
  INVARIANT(ret >= 0 && ret == kvs_ol.size());
}

void op_order_line_range_scan_long(fast_random &r, threadinfo *ti,
                                   query<row_type> &q, uint32_t worker_id) {
  uint32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  uint32_t cur_next_o_id = RandomNumber(r, 1, num_customer_per_district);
  uint32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;

  OL_key k_ol_lower(warehouse_id, district_id, lower, 0);
  OL_key k_ol_upper(warehouse_id, district_id, cur_next_o_id,
                    std::numeric_limits<uint16_t>::max());

  std::vector<std::pair<OL_key, OL_val>> kvs_ol;
  volatile size_t ret = table_order_line->range_scan(k_ol_lower, k_ol_upper,
                                                     kvs_ol, ti, q, worker_id);
  INVARIANT(ret >= 0 && ret == kvs_ol.size());
}

/**
void op_order_range_scan(fast_random &r, threadinfo *ti, query<row_type> &q,
                         uint32_t worker_id) {
  int32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  int32_t district_id = RandomNumber(r, 1, 10);

  O_key k_o_lower, k_o_upper;
  k_o_lower.o_w_id = warehouse_id;
  k_o_lower.o_d_id = district_id;
  k_o_lower.o_id = 0;
  k_o_upper.o_w_id = warehouse_id;
  k_o_upper.o_d_id = district_id;
  memset(&k_o_upper.o_id, ~0, sizeof(k_o_upper.o_id));

  std::vector<std::pair<O_key, O_val>> kvs_o;
  volatile size_t ret =
      table_order->range_scan(k_o_lower, k_o_upper, kvs_o, ti, q, worker_id);
  assert(ret > 0);
}
**/

/**
void op_new_order_lower_bound(fast_random &r, threadinfo *ti,
                              query<row_type> &q, uint32_t worker_id) {
  int32_t warehouse_id = RandomNumber(r, 1, warehouse_num);
  int32_t district_id = RandomNumber(r, 1, 10);

  NO_key k_no_min;
  NO_val v_no;
  k_no_min.no_w_id = warehouse_id;
  k_no_min.no_d_id = district_id;
  k_no_min.no_o_id = 0;

  volatile bool res =
      table_new_order->lower_bound(k_no_min, v_no, ti, q, worker_id);
  assert(res);
}
**/

struct Operation {
  op_func_t func;
  std::string name;
};

std::vector<Operation> operations = {
    // get
    {op_warehouse_get, "op_warehouse_get"},  // 0
    {op_district_get, "op_district_get"},    // 1
    {op_customer_get, "op_customer_get"},    // 2
    {op_item_get, "op_item_get"},            // 3
    {op_stock_get, "op_stock_get"},          // 4
    {op_order_get, "op_order_get"},          // 5
    {op_new_order_get, "op_new_order_get"},  // 6
    // scan
    {op_order_line_scan_n_short, "op_order_line_scan_n_short"},  // 7
    {op_order_line_scan_n_long, "op_order_line_scan_n_long"},    // 8
    // {op_order_line_range_scan_short, "op_order_line_range_scan_short"},
    // {op_order_line_range_scan_long, "op_order_line_range_scan_long"},
    // {op_order_range_scan, "op_order_range_scan"},
    // {op_new_order_lower_bound, "op_new_order_lower_bound"}
    // update
    {op_district_update, "op_district_update"},      // 9
    {op_stock_update, "op_stock_update"},            // 10
    {op_warehouse_udpate, "op_warehouse_udpate"},    // 11
    {op_customer_udpate, "op_customer_udpate"},      // 12
    {op_order_udpate, "op_order_udpate"},            // 13
    {op_order_line_update, "op_order_line_update"},  // 14
    // insert
    {op_new_order_insert, "op_new_order_insert"},    // 15
    {op_order_insert, "op_order_insert"},            // 16
    {op_order_line_insert, "op_order_line_insert"},  // 17
    {op_history_insert, "op_history_insert"},        // 18
    {op_new_order_remove, "op_new_order_remove"},    // 19
};

/******************************************************************************
 * run TPC-C opertaions
 *****************************************************************************/

void *run_op_worker(void *param) {
  OpWorkerParam &thread_param = *(OpWorkerParam *)param;
  bind_to_physical_cores(thread_param.thread_id);
  uint32_t worker_id = thread_param.worker_id;
  fast_random r(thread_param.seed);
  threadinfo *ti =
      threadinfo::make(threadinfo::TI_PROCESS, thread_param.thread_id);
  query<row_type> q;

  COUT_THIS("worker " << worker_id << " start!");

  table_warehouse->worker_enter(worker_id);
  table_district->worker_enter(worker_id);
  table_customer->worker_enter(worker_id);
  table_history->worker_enter(worker_id);
  table_item->worker_enter(worker_id);
  table_stock->worker_enter(worker_id);
  if (table_stock_data != nullptr) {
    table_stock_data->worker_enter(worker_id);
  }
  table_order->worker_enter(worker_id);
  table_new_order->worker_enter(worker_id);
  table_order_line->worker_enter(worker_id);
  if (table_customer_index != nullptr) {
    table_customer_index->worker_enter(worker_id);
  }
  if (table_order_index != nullptr) {
    table_order_index->worker_enter(worker_id);
  }

  while (!running)
    ;

  while (running) {
    thread_param.runner(r, ti, q, worker_id);
    thread_param.throughput++;
  }

  table_warehouse->worker_exit(worker_id);
  table_district->worker_exit(worker_id);
  table_customer->worker_exit(worker_id);
  table_history->worker_exit(worker_id);
  table_item->worker_exit(worker_id);
  table_stock->worker_exit(worker_id);
  if (table_stock_data != nullptr) {
    table_stock_data->worker_exit(worker_id);
  }
  table_order->worker_exit(worker_id);
  table_new_order->worker_exit(worker_id);
  table_order_line->worker_exit(worker_id);
  if (table_customer_index != nullptr) {
    table_customer_index->worker_exit(worker_id);
  }
  if (table_order_index != nullptr) {
    table_order_index->worker_exit(worker_id);
  }

  pthread_exit(nullptr);
}

void run_tpcc_ops(size_t sec, size_t op_id, threadinfo *main_ti,
                  std::vector<int> &avg_throughput) {
  pthread_t threads[fg_n];
  OpWorkerParam worker_params[fg_n];

  // init hack info
  for (size_t i = 0; i < warehouse_num; ++i) {
    for (size_t j = 0; j < num_district_per_warehouse; ++j) {
      next_o_id_hack_info_[i][j] = 3001;
      next_ol_number_hack_info_[i][j] = 1;
    }
  }

  running = false;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    worker_params[worker_i].throughput = 0;
    worker_params[worker_i].worker_id = worker_i;
    worker_params[worker_i].thread_id = worker_i;
    worker_params[worker_i].runner = operations[op_id].func;
    worker_params[worker_i].seed = static_cast<unsigned long>(time(nullptr));

    int ret = pthread_create(&threads[worker_i], nullptr, run_op_worker,
                             (void *)&worker_params[worker_i]);
    if (ret) {
      COUT_THIS("Error:unable to create worker thread," << ret);
      exit(-1);
    }
  }

  sleep(1);
  timeit();
  running = true;

  std::vector<size_t> tput_history(fg_n, 0);
  size_t current_sec = 0;
  while (current_sec < sec) {
    sleep(1);
    size_t throughput = 0;
    for (size_t i = 0; i < fg_n; ++i) {
      throughput += worker_params[i].throughput - tput_history[i];
      tput_history[i] = worker_params[i].throughput;
    }
    COUT_THIS(">>> operations:" << operations[op_id].name << " " << current_sec
                                << " seconds: throughput: " << throughput);
    ++current_sec;
  }

  running = false;
  timeit();

  void *status;
  for (size_t i = 0; i < fg_n; ++i) {
    int rc = pthread_join(threads[i], &status);
    if (rc) {
      COUT_THIS("Error:unable to join," << rc);
      exit(-1);
    }
    COUT_THIS("worker " << i << " quit!");
  }

  size_t throughput = 0;
  for (auto &j : worker_params) throughput += j.throughput;
  avg_throughput[op_id] += throughput / sec;
}

int main(int argc, char **argv) {
  parse_args(argc, argv);

#if defined(USE_MT)
  COUT_THIS("[TPC-C OP]: RUNNING masstree");
#elif defined(USE_WH)
  COUT_THIS("[TPC-C OP]: RUNNING wormhole");
#elif defined(USE_LIB)
  COUT_THIS("[TPC-C OP]: RUNNING baseline");
  COUT_THIS("[TPC-C OP]: LI baseline model_n: " << model_n);
#else
  COUT_THIS("[TPC-C OP]: RUNNING xindex");
#endif

  COUT_THIS("[TPC-C OP]: warmup: " << warmup);
  COUT_THIS("[TPC-C OP]: runtime: " << runtime);
  COUT_THIS("[TPC-C OP]: warehouse_num: " << warehouse_num);
  COUT_THIS("[TPC-C OP]: fg_n: " << fg_n);
  COUT_THIS("[TPC-C OP]: bg_n: " << bg_n);
  COUT_THIS("[TPC-C OP]: operation_id: " << operation_id);

  // prepare kv data
  threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  prepare_kvs(main_ti, fg_n);
  timeit();

  // init hack info
  for (size_t i = 0; i < warehouse_num; ++i) {
    std::vector<uint32_t> temp(num_district_per_warehouse, 3001);
    next_o_id_hack_info_.push_back(temp);
    next_ol_number_hack_info_.push_back(temp);
  }

  COUT_THIS("[TPC-C OP] start background ....");
  // xindex::config.is_warmup = true;
  table_order->start_bg();       // insert
  table_new_order->start_bg();   // insert
  table_order_line->start_bg();  // insert
  table_history->start_bg();     // insert only
  if (table_order_index != nullptr) {
    COUT_N_EXIT("be carefull with secondary index!!!");
    table_order_index->start_bg();  // insert
  }
  table_warehouse->start_bg();  // in-place upate
  table_district->start_bg();   // in-place upate
  table_customer->start_bg();   // in-place upate
  table_stock->start_bg();      // in-place upate
  table_item->start_bg();       // read-only
  if (table_customer_index != nullptr) {
    COUT_N_EXIT("be carefull with secondary index!!!");
    table_customer_index->start_bg();  // scan-only
  }
  if (table_stock_data != nullptr) {
    table_stock_data->start_bg();  // no_operation
  }
  timeit();

  sleep(warmup);
  timeit();

  COUT_THIS("[TPC-C OP] Terminate useless background ....");
  // table_stock_data->terminate_bg();  // no_operation
  // if (table_customer_index != nullptr) {
  //   table_customer_index->terminate_bg();  // scan-only
  // }
  // table_item->terminate_bg();       // read-only
  // table_stock->terminate_bg();      // in-place upate
  // table_customer->terminate_bg();   // in-place upate
  // table_district->terminate_bg();   // in-place upate
  // table_warehouse->terminate_bg();  // in-place upate
  // xindex::config.is_warmup = false;
  timeit();

  size_t loop_num = 1;
  std::vector<int> avg_throughput(operations.size(), 0);
  if (operation_id >= 0) {
    run_tpcc_ops(runtime, operation_id, main_ti, avg_throughput);

  } else {
    for (size_t round_i = 0; round_i < loop_num; round_i++) {
      for (size_t op_i = 0; op_i < operations.size(); op_i++) {
        run_tpcc_ops(runtime, op_i, main_ti, avg_throughput);
        sleep(1);
      }
    }
  }

  for (size_t op_i = 0; op_i < operations.size(); op_i++) {
    COUT_THIS("[Overall] operation "
              << op_i << " (" << operations[op_i].name
              << ") :" << avg_throughput[op_i] / loop_num);
  }
}
