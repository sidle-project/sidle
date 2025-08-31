#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <set>
#include <vector>
#include <iomanip>

#include "helper.h"
#include "tpcc.h"
#include "tpcc_loader.h"


/******************************************************************************
 * Latency Statistics
 *****************************************************************************/

class LatencyStats {
public:
    std::vector<uint64_t> latencies;
    uint64_t total_latency;
    uint64_t count;

    LatencyStats() : total_latency(0), count(0) {
        latencies.reserve(1000000); // Pre-allocate for better performance
    }
    
    void add_latency(uint64_t latency_ns) {
        latencies.push_back(latency_ns);
        total_latency += latency_ns;
        count++;
    }
    
    double get_avg() const {
        return count > 0 ? (double)total_latency / count : 0.0;
    }
    
    uint64_t get_percentile(double percentile) const {
        if (latencies.empty()) return 0;
        
        std::vector<uint64_t> sorted_latencies = latencies;
        std::sort(sorted_latencies.begin(), sorted_latencies.end());
        
        size_t index = (size_t)(percentile / 100.0 * (sorted_latencies.size() - 1));
        return sorted_latencies[index];
    }
    
    uint64_t get_p50() const { return get_percentile(50.0); }
    uint64_t get_p90() const { return get_percentile(90.0); }
    uint64_t get_p99() const { return get_percentile(99.0); }
    uint64_t get_cur_count() const { return count; }
};


// Convert CPU cycles to nanoseconds
inline uint64_t cycles_to_ns(uint64_t cycles) {
    return cycles / CPU_CYCLE_PER_NANOSEC;
}


/******************************************************************************
 * transactions
 *****************************************************************************/

// uint64_t delivery_scan_len = 0;
// uint64_t delivery_scan_n = 0;
// uint64_t order_status_scan_len = 0;
// uint64_t order_status_scan_n = 0;
// uint64_t stock_level_scan_len = 0;
// uint64_t stock_level_scan_n = 0;

std::vector<std::vector<uint64_t>> pending_new_order_per_dis;
std::vector<std::vector<std::vector<uint64_t>>> latest_order_id_per_customer;

void txn_new_order(fast_random &r, uint32_t warehouse_id, threadinfo *ti,
                   query<row_type> q, const uint32_t worker_id) {
  const uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  const uint32_t customer_id = GetCustomerId(r);  // NOTE: different from spec?
  const uint32_t num_items = RandomNumber(r, 5, 15);
  // generate iterms first
  uint32_t item_ids[15], supplier_warehouse_ids[15], order_quantities[15];
  bool all_local = true;
  for (uint32_t i = 0; i < num_items; i++) {
    item_ids[i] = GetItemId(r);  // NOTE: different from spec? how to set UNUSED
    if (likely(RandomNumber(r, 1, 100) > g_new_order_remote_item_pct ||
               warehouse_num == 1)) {
      supplier_warehouse_ids[i] = warehouse_id;
    } else {
      do {
        supplier_warehouse_ids[i] = RandomNumber(r, 1, warehouse_num);
      } while (supplier_warehouse_ids[i] == warehouse_id);
      all_local = false;
    }
    order_quantities[i] = RandomNumber(r, 1, 10);
  }

  // read warehouse
  W_key k_w(warehouse_id);
  W_val v_w;
  bool res = table_warehouse->get(k_w, v_w, ti, q, worker_id);
  // INVARIANT(res);

  // read district
  // D_key(uint32_t dw, uint32_t di) : d_id(di), d_w_id(dw) {}
  D_key k_d(warehouse_id, district_id);
  D_val v_d;
  res = table_district->get(k_d, v_d, ti, q, worker_id);
  // INVARIANT(res);

  v_d.d_next_o_id = v_d.d_next_o_id + 1;
  res = table_district->insert(k_d, v_d, ti, q, worker_id);
  // INVARIANT(res);

  // read customer
  // C_key(uint32_t ci, uint16_t di, uint16_t wi)
  C_key k_c(customer_id, district_id, warehouse_id);
  C_val v_c;
  res = table_customer->get(k_c, v_c, ti, q, worker_id);
  // INVARIANT(res);

  // insert new order
  uint32_t my_next_o_id = v_d.d_next_o_id;

  // NO_key(uint16_t wi, uint16_t di, uint32_t oi)
  NO_key k_no(warehouse_id, district_id, my_next_o_id);
  NO_val v_no;
  v_no.no_dummy.assign("no");
  res = table_new_order->insert(k_no, v_no, ti, q, worker_id);
  // INVARIANT(res);
  pending_new_order_per_dis[warehouse_id - 1][district_id - 1] = my_next_o_id;
  latest_order_id_per_customer[warehouse_id - 1][district_id - 1]
                              [customer_id - 1] = my_next_o_id;

  // insert order
  O_key k_o(warehouse_id, district_id, k_no.no_o_id);
  O_val v_o;
  v_o.o_c_id = customer_id;
  v_o.o_carrier_id = 0;
  v_o.o_ol_cnt = (int8_t)num_items;
  v_o.o_all_local = all_local;
  v_o.o_entry_d = GetCurrentTimeMillis();
  res = table_order->insert(k_o, v_o, ti, q, worker_id);
  // INVARIANT(res);

#ifdef ORIGIN_TPCC
  OrderCidIndexKey k_o_idx(warehouse_id, district_id, customer_id,
                           k_no.no_o_id);
  OrderCidIndexVal v_o_idx;
  res = table_order_index->insert(k_o_idx, v_o_idx, ti, q, worker_id);
  // INVARIANT(res);
#endif

  float all_amount = 0;
  for (uint32_t ol_number = 1; ol_number <= num_items; ol_number++) {
    uint32_t ol_supply_w_id = supplier_warehouse_ids[ol_number - 1];
    uint32_t ol_i_id = item_ids[ol_number - 1];
    uint32_t ol_quantity = order_quantities[ol_number - 1];

    // read item
    I_key k_i(ol_i_id);
    I_val v_i;
    res = table_item->get(k_i, v_i, ti, q, worker_id);
    if (!res) {
      // COUT_THIS("abort new order");
      v_d.d_next_o_id = v_d.d_next_o_id - 1;
      table_district->insert(k_d, v_d, ti, q, worker_id);
      table_new_order->remove(k_no, ti, q, worker_id);
      table_order->remove(k_o, ti, q, worker_id);
#ifdef ORIGIN_TPCC
      table_order_index->remove(k_o_idx, ti, q, worker_id);
#endif
      return;
    }

    // read stock
    S_key k_s(ol_supply_w_id, ol_i_id);
    S_val v_s;
    res = table_stock->get(k_s, v_s, ti, q, worker_id);
    // INVARIANT(res);

    // update stock
    S_val v_s_new;
    v_s_new.s_quantity = v_s.s_quantity;
    v_s_new.s_ytd = v_s.s_ytd;
    v_s_new.s_order_cnt = v_s.s_order_cnt;
    v_s_new.s_remote_cnt = v_s.s_remote_cnt;
    if (v_s_new.s_quantity - ol_quantity >= 10) {
      v_s_new.s_quantity -= ol_quantity;
    } else {
      v_s_new.s_quantity += 91 - int32_t(ol_quantity);
    }
    v_s_new.s_ytd += ol_quantity;
    v_s_new.s_order_cnt++;
    v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;
    res = table_stock->insert(k_s, v_s_new, ti, q, worker_id);
    // INVARIANT(res);

    // insert online order
    OL_key k_ol(warehouse_id, district_id, k_no.no_o_id, ol_number);
    OL_val v_ol;
    v_ol.ol_i_id = ol_i_id;
    v_ol.ol_delivery_d = 0;  // not delivered yet
    v_ol.ol_amount = 1.0 * ol_quantity * v_i.i_price;
    v_ol.ol_supply_w_id = ol_supply_w_id;
    v_ol.ol_quantity = ol_quantity;

    res = table_order_line->insert(k_ol, v_ol, ti, q, worker_id);
    // INVARIANT(res);
    all_amount += v_ol.ol_amount;
  }

  // the computation part
  volatile float total_amount =
      all_amount * (1 - v_c.c_discount) * (1 + v_w.w_tax + v_d.d_tax);
  UNUSED(total_amount);
}

void txn_payment(fast_random &r, uint32_t warehouse_id, threadinfo *ti,
                 query<row_type> q, const uint32_t worker_id) {
  const uint32_t districtID = RandomNumber(r, 1, 10);
  uint32_t customerDistrictID, customerWarehouseID;
  if (likely(warehouse_num == 1 || RandomNumber(r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, 10);
    do {
      customerWarehouseID = RandomNumber(r, 1, warehouse_num);
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float)(RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();

  uint32_t customerID;
  // FIXME: current we disable this way: use last_name to get bunch of customers
  if (false /* RandomNumber(r, 1, 100) <= 60 */) {
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    assert(sizeof(lastname_buf) == 16);
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    CustomerNameIndexKey k_c_idx_0;
    k_c_idx_0.ci_w_id = customerWarehouseID;
    k_c_idx_0.ci_d_id = customerDistrictID;
    k_c_idx_0.ci_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.ci_first.assign(zeros);

    std::vector<std::pair<CustomerNameIndexKey, CustomerNameIndexVal>>
        customer_index_kvs;

#ifdef ORIGIN_TPCC
    CustomerNameIndexKey k_c_idx_1;
    k_c_idx_1.ci_w_id = customerWarehouseID;
    k_c_idx_1.ci_d_id = customerDistrictID;
    k_c_idx_1.ci_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.ci_first.assign(ones);

    size_t ret = table_customer_index->range_scan(
        k_c_idx_0, k_c_idx_1, customer_index_kvs, ti, q, worker_id);
    INVARIANT(ret == customer_index_kvs.size());
#else
    COUT_N_EXIT("check scan number when use this");
    size_t ret = table_customer_index->scan(k_c_idx_0, 10, customer_index_kvs,
                                            ti, q, worker_id);
    INVARIANT(ret == customer_index_kvs.size());
#endif

    if (customer_index_kvs.size() == 0) {
      COUT_THIS("abort payment, customer not found by last name");
      return;
    }
    customerID =
        customer_index_kvs[customer_index_kvs.size() / 2].second.ci_c_id;
  } else {
    customerID = GetCustomerId(r);
  }

  // read warehouse
  W_key k_w(warehouse_id);
  W_val v_w;
  bool res = table_warehouse->get(k_w, v_w, ti, q, worker_id);
  // INVARIANT(res);

  // update warehouse
  v_w.w_ytd += paymentAmount;
  res = table_warehouse->insert(k_w, v_w, ti, q, worker_id);
  // INVARIANT(res);

  // read district
  D_key k_d(warehouse_id, districtID);
  D_val v_d;
  res = table_district->get(k_d, v_d, ti, q, worker_id);
  // INVARIANT(res);

  // update distric
  v_d.d_ytd += paymentAmount;
  res = table_district->insert(k_d, v_d, ti, q, worker_id);
  // INVARIANT(res);

  // read customer
  C_key k_c(customerID, customerDistrictID, customerWarehouseID);
  C_val v_c;
  res = table_customer->get(k_c, v_c, ti, q, worker_id);
  // INVARIANT(res);

  // update customer
  v_c.c_balance -= paymentAmount;
  v_c.c_ytd_payment += paymentAmount;
  v_c.c_payment_cnt++;
  if (memcmp(v_c.c_credit.buf, "BC", 2) == 0) {
    char buf[501];
    snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", k_c.c_id, k_c.c_d_id,
             k_c.c_w_id, districtID, warehouse_id, paymentAmount,
             v_c.c_data.buf);
    memcpy(v_c.c_data.buf, buf, 500);
  }
  res = table_customer->insert(k_c, v_c, ti, q, worker_id);
  // INVARIANT(res);

  // insert history
  H_key k_h(warehouse_id, districtID, customerWarehouseID, customerDistrictID,
            customerID, ts);
  // H_key k_h;
  H_val v_h;
  // v_h.h_c_id = customerID;
  // v_h.h_c_d_id = customerDistrictID;
  // v_h.h_c_w_id = customerWarehouseID;
  // v_h.h_d_id = districtID;
  // v_h.h_w_id = warehouse_id;
  // v_h.h_date = ts;
  v_h.h_amount = paymentAmount;

  size_t space_n = 4;
  memcpy(v_h.h_data.buf, v_w.w_name.buf, sizeof(v_w.w_name.buf));
  memset(v_h.h_data.buf + sizeof(v_w.w_name.buf), ' ', space_n);
  memcpy(v_h.h_data.buf + sizeof(v_w.w_name.buf) + space_n, v_d.d_name.buf,
         sizeof(v_d.d_name.buf));
  res = table_history->insert(k_h, v_h, ti, q, worker_id);
  // INVARIANT(res);
}

void txn_delivery(fast_random &r, uint32_t warehouse_id, threadinfo *ti,
                  query<row_type> q, const uint32_t worker_id) {
  uint32_t o_carrier_id = RandomNumber(r, 1, num_district_per_warehouse);

  bool res = false;
  for (uint32_t d_id = 1; d_id <= num_district_per_warehouse; d_id++) {
    // read the newest new order
    uint32_t latest_no_id =
        pending_new_order_per_dis[warehouse_id - 1][d_id - 1];
    NO_key k_no_min(warehouse_id, d_id, latest_no_id);
    NO_val v_no;

    // if no new order is found
    if (!table_new_order->get(k_no_min, v_no, ti, q, worker_id)) {
      continue;
    }

    uint32_t min_no_o_id = k_no_min.no_o_id;

    // read order
    O_key k_o(warehouse_id, d_id, min_no_o_id);
    O_val v_o;
    if (!table_order->get(k_o, v_o, ti, q, worker_id)) {
      // even if we read the new order entry, there's no guarantee
      // we will read the oorder entry: in this case the txn will abort,
      // but we're simply bailing out early
      COUT_THIS("abort delivery due to no oorder entry");
      return;
    }

    // if at least one new_order found, delete new order
    res = table_new_order->remove(k_no_min, ti, q, worker_id);
    // INVARIANT(res);

    // update order carrier
    v_o.o_carrier_id = o_carrier_id;
    res = table_order->insert(k_o, v_o, ti, q, worker_id);
    // INVARIANT(res);

    // range scan orderline by [w_id, d_id, o_id, 0~max]
    OL_key k_ol_lower(warehouse_id, d_id, min_no_o_id, 0);
    std::vector<std::pair<OL_key, OL_val>> kvs_ol;
#ifdef ORIGIN_TPCC
    OL_key k_ol_upper(warehouse_id, d_id, min_no_o_id,
                      std::numeric_limits<uint16_t>::max());
    size_t ret = table_order_line->range_scan(k_ol_lower, k_ol_upper, kvs_ol,
                                              ti, q, worker_id);
    INVARIANT(ret == kvs_ol.size());
#else
    // FIXME: use scan n to simulate range scan currently
    size_t ret =
        table_order_line->scan(k_ol_lower, 10, kvs_ol, ti, q, worker_id);
    INVARIANT(ret == kvs_ol.size());
    // delivery_scan_len += ret;
    // delivery_scan_n++;
#endif

    float ol_total = 0.0;
    for (size_t i = 0; i < kvs_ol.size(); i++) {
      OL_val ol_val_ = kvs_ol[i].second;
      ol_total += ol_val_.ol_amount;
      // update ol_delivery_d
      ol_val_.ol_delivery_d = GetCurrentTimeMillis();
      res =
          table_order_line->insert(kvs_ol[i].first, ol_val_, ti, q, worker_id);
      assert(res);
    }

    // update customer
    C_key k_c(v_o.o_c_id, d_id, warehouse_id);
    C_val v_c;
    res = table_customer->get(k_c, v_c, ti, q, worker_id);
    // INVARIANT(res);
    v_c.c_balance += ol_total;
    v_c.c_delivery_cnt++;
    res = table_customer->insert(k_c, v_c, ti, q, worker_id);
    // INVARIANT(res);
  }
}

void txn_order_status(fast_random &r, uint32_t warehouse_id, threadinfo *ti,
                      query<row_type> q, const uint32_t worker_id) {
  const uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);

  // r Customer
  uint32_t customer_id;
  // FIXME: current we disable this way: use last_name to get bunch of customers
  if (false /* RandomNumber(r, 1, 100) <= 60 */) {
    uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
    assert(sizeof(lastname_buf) == 16);
    memset(lastname_buf, 0, sizeof(lastname_buf));
    GetNonUniformCustomerLastNameRun(lastname_buf, r);

    static const std::string zeros(16, 0);
    static const std::string ones(16, 255);

    CustomerNameIndexKey k_c_idx_0;
    k_c_idx_0.ci_w_id = warehouse_id;
    k_c_idx_0.ci_d_id = district_id;
    k_c_idx_0.ci_last.assign((const char *)lastname_buf, 16);
    k_c_idx_0.ci_first.assign(zeros);

    std::vector<std::pair<CustomerNameIndexKey, CustomerNameIndexVal>>
        customer_index_kvs;
#ifdef ORIGIN_TPCC
    CustomerNameIndexKey k_c_idx_1;
    k_c_idx_1.ci_w_id = warehouse_id;
    k_c_idx_1.ci_d_id = district_id;
    k_c_idx_1.ci_last.assign((const char *)lastname_buf, 16);
    k_c_idx_1.ci_first.assign(ones);

    size_t ret = table_customer_index->range_scan(
        k_c_idx_0, k_c_idx_1, customer_index_kvs, ti, q, worker_id);
    INVARIANT(ret == customer_index_kvs.size());
#else
    COUT_N_EXIT("check scan number when use this");
    size_t ret = table_customer_index->scan(k_c_idx_0, 10, customer_index_kvs,
                                            ti, q, worker_id);
    INVARIANT(ret == customer_index_kvs.size());
#endif

    if (customer_index_kvs.size() == 0) {
      COUT_THIS("abort status, customer not found by last name");
      return;
    }
    customer_id =
        customer_index_kvs[customer_index_kvs.size() / 2].second.ci_c_id;
  } else {
    customer_id = GetCustomerId(r);
  }

#ifdef ORIGIN_TPCC
  // find largest order id
  COUT_N_EXIT("fix to rscan before use");
  OrderCidIndexKey o_c_idx_0(warehouse_id, district_id, customer_id, 0);
  std::vector<std::pair<OrderCidIndexKey, OrderCidIndexVal>> order_cid_kvs;
  OrderCidIndexKey o_c_idx_1(warehouse_id, district_id, customer_id,
                             std::numeric_limits<uint32_t>::max());
  size_t ret = table_order_index->range_scan(o_c_idx_0, o_c_idx_1,
                                             order_cid_kvs, ti, q, worker_id);
  INVARIANT(ret == order_cid_kvs.size());
  if (order_cid_kvs.size() == 0) {
    COUT_THIS("abort order status, no order under such customer");
    return;
  }
  uint32_t newest_o_id = order_cid_kvs.back().first.oc_o_id;
#else
  uint32_t newest_o_id =
      latest_order_id_per_customer[warehouse_id - 1][district_id - 1]
                                  [customer_id - 1];
#endif

  // r Orderline range scan order line with[w_id, d_id, o_id, 0~max]
  OL_key k_ol_lower(warehouse_id, district_id, newest_o_id, 0);
  std::vector<std::pair<OL_key, OL_val>> kvs_ol;

#ifdef ORIGIN_TPCC
  OL_key k_ol_upper(warehouse_id, district_id, newest_o_id,
                    std::numeric_limits<uint64_t>::max());
  ret = table_order_line->range_scan(k_ol_lower, k_ol_upper, kvs_ol, ti, q,
                                     worker_id);
  INVARIANT(ret == kvs_ol.size());
#else
  size_t ret = table_order_line->scan(k_ol_lower, 10, kvs_ol, ti, q, worker_id);
  INVARIANT(ret == kvs_ol.size());
  // order_status_scan_len += ret;
  // order_status_scan_n++;
#endif
}

void txn_stock_level(fast_random &r, uint32_t warehouse_id, threadinfo *ti,
                     query<row_type> q, const uint32_t worker_id) {
  const uint32_t district_id = RandomNumber(r, 1, num_district_per_warehouse);
  const uint32_t threshold = RandomNumber(r, 10, 20);

  // r District
  D_key k_d(warehouse_id, district_id);
  D_val v_d;
  bool res = table_district->get(k_d, v_d, ti, q, worker_id);
  // INVARIANT(res);

  uint32_t cur_next_o_id = v_d.d_next_o_id;
  uint32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;

  // range scan order line with [w_id, d_id, lower~cur_next_o_id]
  OL_key k_ol_lower(warehouse_id, district_id, lower, 0);
  std::vector<std::pair<OL_key, OL_val>> kvs_ol;

#ifdef ORIGIN_TPCC
  OL_key k_ol_upper(warehouse_id, district_id, cur_next_o_id,
                    std::numeric_limits<uint16_t>::max());
  size_t ret = table_order_line->range_scan(k_ol_lower, k_ol_upper, kvs_ol, ti,
                                            q, worker_id);
  INVARIANT(ret == kvs_ol.size());
#else
  size_t ret =
      table_order_line->scan(k_ol_lower, 10 * 20, kvs_ol, ti, q, worker_id);
  INVARIANT(ret == kvs_ol.size());
  // stock_level_scan_len += ret;
  // stock_level_scan_n++;
#endif

  // r Stock
  volatile size_t fake_size = 0;
  for (size_t i = 0; i < kvs_ol.size(); i++) {
    uint32_t ol_i_id = kvs_ol[i].second.ol_i_id;
    S_key k_s(warehouse_id, ol_i_id);
    S_val v_s;
    bool res = table_stock->get(k_s, v_s, ti, q, worker_id);
    // INVARIANT(res);
    if (v_s.s_quantity < threshold) {
      fake_size++;
    }
  }
}

/******************************************************************************
 * TPC-C
 *****************************************************************************/

template <bool stat_lat>
void *run_worker(void *param) {
  WorkerThreadParam &thread_param = *(WorkerThreadParam *)param;

  uint32_t w_start = thread_param.w_id_start;
  uint32_t w_end = thread_param.w_id_end;
  uint32_t worker_id = thread_param.worker_id;
  uint32_t warehouse_id = w_start;
  bind_to_physical_cores(worker_id);

  fast_random r(thread_param.seed);
  threadinfo *ti = threadinfo::make(threadinfo::TI_PROCESS, worker_id);
  query<row_type> q;

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

  COUT_THIS("worker " << worker_id << " warmup!");

  while (!running)
    ;

  COUT_THIS("worker " << worker_id << " start!");
  size_t op_i = 0;
  // Get latency stats objects
  LatencyStats* new_order_stats = (LatencyStats*)thread_param.new_order_latency_stats;
  LatencyStats* payment_stats = (LatencyStats*)thread_param.payment_latency_stats;
  LatencyStats* delivery_stats = (LatencyStats*)thread_param.delivery_latency_stats;
  LatencyStats* order_status_stats = (LatencyStats*)thread_param.order_status_latency_stats;
  LatencyStats* stock_level_stats = (LatencyStats*)thread_param.stock_level_latency_stats;
  while (running) {
    if (op_i % 10 == 0) {
      epochinc();
      ti->rcu_quiesce();
    } 
    op_i += 1;
    double d = RandomNumber(r, 1, 100);
    warehouse_id = RandomNumber(r, w_start, w_end - 1);
    if (d <= ratio[0]) {
      if (stat_lat) {
        uint64_t t0 = util_rdtsc();
        txn_new_order(r, warehouse_id, ti, q, worker_id);
        uint64_t cycles = util_rdtsc() - t0;
        thread_param.new_order_cycles += (cycles);
        thread_param.new_order_tput++;
        new_order_stats->add_latency(cycles_to_ns(cycles));
      } else {
        txn_new_order(r, warehouse_id, ti, q, worker_id);
      }
    } else if (d <= ratio[0] + ratio[1]) {
      if (stat_lat) {
        uint64_t t0 = util_rdtsc();
        txn_payment(r, warehouse_id, ti, q, worker_id);
        uint64_t cycles = util_rdtsc() - t0;
        thread_param.payment_cycles += cycles;
        thread_param.payment_tput++;
        payment_stats->add_latency(cycles_to_ns(cycles));
      } else {
        txn_payment(r, warehouse_id, ti, q, worker_id);
      }
    } else if (d <= ratio[0] + ratio[1] + ratio[2]) {
      if (stat_lat) {
        uint64_t t0 = util_rdtsc();
        txn_delivery(r, warehouse_id, ti, q, worker_id);
        uint64_t cycles = util_rdtsc() - t0;
        thread_param.delivery_cycles += cycles;
        thread_param.delivery_tput++;
        delivery_stats->add_latency(cycles_to_ns(cycles));
      } else {
        txn_delivery(r, warehouse_id, ti, q, worker_id);
      }
    } else if (d <= ratio[0] + ratio[1] + ratio[2] + ratio[3]) {
      if (stat_lat) {
        uint64_t t0 = util_rdtsc();
        txn_order_status(r, warehouse_id, ti, q, worker_id);
        uint64_t cycles = util_rdtsc() - t0;
        thread_param.order_status_cycles += cycles;
        thread_param.order_status_tput++;
        order_status_stats->add_latency(cycles_to_ns(cycles));
      } else {
        txn_order_status(r, warehouse_id, ti, q, worker_id);
      }
    } else {
      if (stat_lat) {
        uint64_t t0 = util_rdtsc();
        txn_stock_level(r, warehouse_id, ti, q, worker_id);
        uint64_t cycles = util_rdtsc() - t0;
        thread_param.stock_level_cycles += cycles;
        thread_param.stock_level_tput++;
        stock_level_stats->add_latency(cycles_to_ns(cycles));
      } else {
        txn_stock_level(r, warehouse_id, ti, q, worker_id);
      }
    }
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

void run_tpcc() {
  pthread_t threads[fg_n];
  WorkerThreadParam worker_params[fg_n];
  COUT_THIS("[TPC-C] Start TPC-C benchmark ...");

  // create some hack info
  for (size_t i = 0; i < warehouse_num; ++i) {
    std::vector<uint64_t> p_orders(num_district_per_warehouse, 3000, std::allocator<uint64_t>());
    pending_new_order_per_dis.push_back(p_orders);
    std::vector<std::vector<uint64_t>> l_o_ids;
    for (size_t j = 0; j < num_district_per_warehouse; ++j) {
      std::vector<uint64_t> l_o(num_customer_per_district, 3000, std::allocator<uint64_t>());;
      l_o_ids.push_back(l_o);
    }
    latest_order_id_per_customer.push_back(l_o_ids);
  }
  COUT_THIS("[TPC-C] Hack info created ...");
  INVARIANT(pending_new_order_per_dis.size() == warehouse_num);
  INVARIANT(latest_order_id_per_customer.size() == warehouse_num);
  INVARIANT(latest_order_id_per_customer[0].size() ==
            num_district_per_warehouse);
  INVARIANT(pending_new_order_per_dis[0].size() == num_district_per_warehouse);
  COUT_THIS("latest_order_id_per_customer[0][0].size(): " << latest_order_id_per_customer[0][0].size() << " num_customer_per_district: " << num_customer_per_district);
  INVARIANT(latest_order_id_per_customer[0][0].size() ==
            num_customer_per_district);

  COUT_THIS("[TPC-C] Load data ...");
  // prepare kv data
  threadinfo *main_ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  COUT_THIS("[TPC-C] Prepare kv data ...");
  prepare_kvs(main_ti, fg_n);
  timeit();

  // create threads
  COUT_THIS("[TPC-C] Create threads....");
  running = false;
  size_t w_num_per_thread = warehouse_num / fg_n, prev_end_i = 0;
  size_t trailing_warehouse = warehouse_num - w_num_per_thread * fg_n;
  for (size_t worker_i = 0; worker_i < fg_n; worker_i++) {
    size_t begin_i = prev_end_i;
    size_t end_i = begin_i + w_num_per_thread;
    if (trailing_warehouse > 0) {
      end_i++;
      trailing_warehouse--;
    }
    prev_end_i = end_i;
    worker_params[worker_i].throughput = 0;
    worker_params[worker_i].worker_id = worker_i;
    worker_params[worker_i].w_id_start = begin_i + 1;
    worker_params[worker_i].w_id_end = end_i + 1;
    worker_params[worker_i].seed = static_cast<unsigned long>(time(nullptr));
    worker_params[worker_i].new_order_cycles = 0;
    worker_params[worker_i].new_order_tput = 0;
    worker_params[worker_i].payment_cycles = 0;
    worker_params[worker_i].payment_tput = 0;
    worker_params[worker_i].delivery_cycles = 0;
    worker_params[worker_i].delivery_tput = 0;
    worker_params[worker_i].order_status_cycles = 0;
    worker_params[worker_i].order_status_tput = 0;
    worker_params[worker_i].stock_level_cycles = 0;
    worker_params[worker_i].stock_level_tput = 0;

    // Initialize latency statistics objects
    worker_params[worker_i].new_order_latency_stats = new LatencyStats();
    worker_params[worker_i].payment_latency_stats = new LatencyStats();
    worker_params[worker_i].delivery_latency_stats = new LatencyStats();
    worker_params[worker_i].order_status_latency_stats = new LatencyStats();
    worker_params[worker_i].stock_level_latency_stats = new LatencyStats();

    int ret;
    if (stat_latency) {
      ret = pthread_create(&threads[worker_i], nullptr, run_worker<true>,
                           (void *)&worker_params[worker_i]);
    } else {
      ret = pthread_create(&threads[worker_i], nullptr, run_worker<false>,
                           (void *)&worker_params[worker_i]);
    }
    if (ret) {
      COUT_THIS("Error:unable to create worker thread," << ret);
      exit(-1);
    }
  }

  // start warmup
  COUT_THIS("[TPC-C] Warmup ...");
//   xindex::config.is_warmup = true;
  sleep(warmup);
  timeit();

  // end warmup and start running
//   xindex::config.is_warmup = false;
  timeit();
  COUT_THIS("[TPC-C] Start running!!!");
  running = true;

  // to ensure that active flags are correctly set
  sleep(1);

  // start table background.
  // Be carefull with start_bg order, later started background thread may be
  // scheduled out insert -> in-place update -> read
  COUT_THIS("[TPC-C] Start background thread ...");
  table_order_line->start_bg();  // insert
  table_order->start_bg();       // insert
  table_new_order->start_bg();   // insert
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

  std::vector<size_t> tput_history(fg_n, 0);
  size_t current_sec = 0;
  while (current_sec < runtime) {
    sleep(1);
    size_t throughput = 0;
    for (size_t i = 0; i < fg_n; ++i) {
      throughput += worker_params[i].throughput - tput_history[i];
      tput_history[i] = worker_params[i].throughput;
    }

    COUT_THIS(">>> sec " << current_sec << " throughput: " << throughput);
    ++current_sec;
  }

  // finish running
  running = false;
  timeit();

  // thread join
  void *status;
  for (size_t i = 0; i < fg_n; ++i) {
    int rc = pthread_join(threads[i], &status);
    if (rc) {
      COUT_N_EXIT("Error:unable to join," << rc);
    }
    COUT_THIS("worker " << i << " quit!");
  }

  // running result
  uint64_t throughput = 0, all_new_order_tput = 0, all_new_order_cycles = 0,
           all_payment_tput = 0, all_payment_cycles = 0, all_delivery_tput = 0,
           all_delivery_cycles = 0, all_order_status_tput = 0,
           all_order_status_cycles = 0, all_stock_level_tput = 0,
           all_stock_level_cycles = 0;
  for (auto &j : worker_params) {
    throughput += j.throughput;
    all_new_order_cycles += j.new_order_cycles;
    all_new_order_tput += j.new_order_tput;
    all_payment_cycles += j.payment_cycles;
    all_payment_tput += j.payment_tput;
    all_delivery_cycles += j.delivery_cycles;
    all_delivery_tput += j.delivery_tput;
    all_order_status_cycles += j.order_status_cycles;
    all_order_status_tput += j.order_status_tput;
    all_stock_level_cycles += j.stock_level_cycles;
    all_stock_level_tput += j.stock_level_tput;
  }

  COUT_THIS("[Overall] Throughput(tx/s): " << throughput / runtime);
  if (all_new_order_tput > 0) {
    double new_order_ns =
        all_new_order_cycles / (all_new_order_tput * CPU_CYCLE_PER_NANOSEC);
    double payment_ns =
        all_payment_cycles / (all_payment_tput * CPU_CYCLE_PER_NANOSEC);
    double delivery_ns =
        all_delivery_cycles / (all_delivery_tput * CPU_CYCLE_PER_NANOSEC);
    double order_status_ns = all_order_status_cycles /
                             (all_order_status_tput * CPU_CYCLE_PER_NANOSEC);
    double stock_level_ns =
        all_stock_level_cycles / (all_stock_level_tput * CPU_CYCLE_PER_NANOSEC);

    COUT_THIS("[Overall] new_order_ns: " << new_order_ns);
    COUT_THIS("[Overall] payment_ns: " << payment_ns);
    COUT_THIS("[Overall] delivery_ns: " << delivery_ns);
    COUT_THIS("[Overall] order_status_ns: " << order_status_ns);
    COUT_THIS("[Overall] stock_level_ns: " << stock_level_ns);
  }

  // Aggregate and print latency statistics
  if (stat_latency) {
    COUT_THIS("\n[Latency Statistics]");
    
    // Aggregate all transaction latencies across all workers
    LatencyStats global_all_transactions;
    
    for (size_t i = 0; i < fg_n; ++i) {
      LatencyStats* new_order_stats = (LatencyStats*)worker_params[i].new_order_latency_stats;
      LatencyStats* payment_stats = (LatencyStats*)worker_params[i].payment_latency_stats;
      LatencyStats* delivery_stats = (LatencyStats*)worker_params[i].delivery_latency_stats;
      LatencyStats* order_status_stats = (LatencyStats*)worker_params[i].order_status_latency_stats;
      LatencyStats* stock_level_stats = (LatencyStats*)worker_params[i].stock_level_latency_stats;
      
      // Collect all latencies from all transaction types into one global stats
      for (const auto& latency : new_order_stats->latencies) {
        global_all_transactions.add_latency(latency);
      }
      for (const auto& latency : payment_stats->latencies) {
        global_all_transactions.add_latency(latency);
      }
      for (const auto& latency : delivery_stats->latencies) {
        global_all_transactions.add_latency(latency);
      }
      for (const auto& latency : order_status_stats->latencies) {
        global_all_transactions.add_latency(latency);
      }
      for (const auto& latency : stock_level_stats->latencies) {
        global_all_transactions.add_latency(latency);
      }
    }
    
    // Print overall latency statistics for all transactions
    if (global_all_transactions.get_cur_count() > 0) {
      COUT_THIS("[Overall Transactions] Count: " << global_all_transactions.get_cur_count());
      COUT_THIS("[Overall Transactions] Avg: " << std::fixed << std::setprecision(2) << global_all_transactions.get_avg() << " ns");
      COUT_THIS("[Overall Transactions] P50: " << global_all_transactions.get_p50() << " ns");
      COUT_THIS("[Overall Transactions] P90: " << global_all_transactions.get_p90() << " ns");
      COUT_THIS("[Overall Transactions] P99: " << global_all_transactions.get_p99() << " ns");
    } else {
      COUT_THIS("[Overall Transactions] No latency data collected");
    }
  }
}

int main(int argc, char **argv) {
  parse_args(argc, argv);

#if defined(USE_MT)
  // COUT_THIS("[TPC-C]: RUNNING masstree");
#elif defined(USE_WH)
  COUT_THIS("[TPC-C]: RUNNING wormhole");
#elif defined(USE_LIB)
  COUT_THIS("[TPC-C]: RUNNING LI baseline. model_n=" << model_n);
#else
  COUT_THIS("[TPC-C]: RUNNING xindex");
#endif

  INVARIANT(ratio[0] + ratio[1] + ratio[2] + ratio[3] + ratio[4] == 100);
  // COUT_THIS("[TPC-C]: N:P:D:O:S = " << ratio[0] << ":" << ratio[1] << ":"
  //                                   << ratio[2] << ":" << ratio[3] << ":"
  //                                   << ratio[4]);
  printf("[TPC-C]: N:P:D:O:S = %d:%d:%d:%d:%d\n", ratio[0], ratio[1], ratio[2],
         ratio[3], ratio[4]);
  printf("[TPC-C]: warmup: %d, runtime: %d\n", warmup, runtime);
  printf("[TPC-C]: warehouse_num: %d, fg_n: %d, bg_n: %d\n", warehouse_num, fg_n,
         bg_n);
  // COUT_THIS("[TPC-C]: warmup: " << warmup);
  // COUT_THIS("[TPC-C]: runtime: " << runtime);
  // COUT_THIS("[TPC-C]: warehouse_num: " << warehouse_num);
  // COUT_THIS("[TPC-C]: fg_n: " << fg_n);
  // COUT_THIS("[TPC-C]: bg_n: " << bg_n);

  timeit();
  run_tpcc();

  // double avg_delivery_scan_len = delivery_scan_len * 1.0 / delivery_scan_n;
  // double avg_order_status_scan_len =
  //     order_status_scan_len * 1.0 / order_status_scan_n;
  // double avg_stock_level_scan_len =
  //     stock_level_scan_len * 1.0 / stock_level_scan_n;
  // COUT_VAR(avg_delivery_scan_len);
  // COUT_VAR(avg_order_status_scan_len);
  // COUT_VAR(avg_stock_level_scan_len);
}
