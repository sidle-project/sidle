#include "sidle_struct.hh"

namespace sidle {

migration_queue art_worker_base::queue_;
std::atomic<bool> art_worker_base::need_demotion_(false);
std::atomic<bool> art_worker_base::need_promotion_(false);
std::atomic<bool> art_worker_base::can_trigger_(true);
std::atomic<bool> art_worker_base::can_promote_(true);
std::atomic<bool> art_worker_base::can_demote_(true);
std::atomic<bool> art_worker_base::is_demoting_(false);
std::atomic<bool> art_worker_base::is_triggering_migration_(false);
std::atomic<bool> art_worker_base::need_adjust_threshold_(false);
std::atomic<bool> art_worker_base::demoting_complete_(false);
std::condition_variable art_worker_base::trigger_wakeup_cv_;
std::condition_variable art_worker_base::promotion_trigger_cv_;
std::condition_variable art_worker_base::demotion_trigger_cv_;
std::condition_variable art_worker_base::demotion_complete_cv_;
std::condition_variable art_worker_base::adjuster_wakeup_cv_;
std::mutex art_worker_base::trigger_wakeup_mtx_;
std::mutex art_worker_base::demotion_mtx_;
std::mutex art_worker_base::promotion_mtx_;
std::mutex art_worker_base::demotion_complete_mtx_;
std::mutex art_worker_base::adjuster_wakeup_mtx_;
std::atomic<bool> art_worker_base::is_running_(false);

} // namespace sidle