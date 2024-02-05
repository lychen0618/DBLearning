//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
const uint32_t S_IDX = static_cast<uint32_t>(LockManager::LockMode::SHARED);
const uint32_t X_IDX = static_cast<uint32_t>(LockManager::LockMode::EXCLUSIVE);
const uint32_t IS_IDX = static_cast<uint32_t>(LockManager::LockMode::INTENTION_SHARED);
const uint32_t IX_IDX = static_cast<uint32_t>(LockManager::LockMode::INTENTION_EXCLUSIVE);
const uint32_t SIX_IDX = static_cast<uint32_t>(LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE);

auto LockManager::CheckIfCanLock(const std::shared_ptr<LockRequestQueue> &queue, LockMode lock_mode) -> bool {
  if (lock_mode == LockMode::INTENTION_SHARED) {
    if (queue->granted_lock_cnts_[X_IDX] != 0) {
      return false;
    }
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    if (queue->granted_lock_cnts_[S_IDX] != 0 || queue->granted_lock_cnts_[SIX_IDX] != 0 ||
        queue->granted_lock_cnts_[X_IDX] != 0) {
      return false;
    }
  } else if (lock_mode == LockMode::SHARED) {
    if (queue->granted_lock_cnts_[IX_IDX] != 0 || queue->granted_lock_cnts_[SIX_IDX] != 0 ||
        queue->granted_lock_cnts_[X_IDX] != 0) {
      return false;
    }
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    if (queue->granted_lock_cnts_[IX_IDX] != 0 || queue->granted_lock_cnts_[S_IDX] != 0 ||
        queue->granted_lock_cnts_[SIX_IDX] != 0 || queue->granted_lock_cnts_[X_IDX] != 0) {
      return false;
    }
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (queue->granted_lock_cnts_[IS_IDX] != 0 || queue->granted_lock_cnts_[IX_IDX] != 0 ||
        queue->granted_lock_cnts_[S_IDX] != 0 || queue->granted_lock_cnts_[SIX_IDX] != 0 ||
        queue->granted_lock_cnts_[X_IDX] != 0) {
      return false;
    }
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // Checks:
  // 1. SUPPORTED LOCK MODES: No need for locking table
  // 2. ISOLATION LEVEL
  auto txn_id = txn->GetTransactionId();
  auto isolation_level = txn->GetIsolationLevel();
  auto state = txn->GetState();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (state == TransactionState::SHRINKING &&
        (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // Lock operations
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto &queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  queue->latch_.lock();
  // Check upgrade
  if (queue->granted_lock_req_map_.count(txn_id) != 0) {
    auto request = queue->granted_lock_req_map_[txn_id];
    const auto &prev_mode = request->lock_mode_;
    if (lock_mode == prev_mode) {
      queue->latch_.unlock();
      return true;
    }
    // 1. Check the precondition of upgrade
    bool permitted = false;
    if (prev_mode == LockMode::INTENTION_SHARED ||
        (prev_mode == LockMode::SHARED &&
         (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) ||
        (prev_mode == LockMode::INTENTION_EXCLUSIVE &&
         (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) ||
        (prev_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE)) {
      permitted = true;
    }
    if (!permitted) {
      queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (queue->upgrading_ != INVALID_TXN_ID && queue->upgrading_ != txn_id) {
      queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    // 2. Drop the current lock, reserve the upgrade position
    queue->upgrading_ = txn_id;
    auto prev_mode_idx = static_cast<uint32_t>(prev_mode);
    queue->granted_lock_cnts_[prev_mode_idx]--;
    queue->granted_lock_req_map_.erase(txn_id);
    queue->request_queue_.emplace_front(new LockRequest(txn_id, lock_mode, oid));
    if (prev_mode == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->erase(oid);
    } else if (prev_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->erase(oid);
    } else if (prev_mode == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    } else if (prev_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    } else if (prev_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    }
    // 3. Wait to get the new lock granted
  } else {
    queue->request_queue_.emplace_back(new LockRequest(txn_id, lock_mode, oid));
  }
  while (txn->GetState() != TransactionState::ABORTED &&
         (queue->request_queue_.front()->txn_id_ != txn_id ||
          !CheckIfCanLock(queue, lock_mode))) {  // TODO(1ycheen): this cond may wrong
    std::unique_lock<std::mutex> l(queue->latch_, std::defer_lock);
    queue->cv_.wait(l);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    auto it = queue->request_queue_.begin();
    for (; it != queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn_id) {
        break;
      }
    }
    queue->request_queue_.erase(it);
    if (queue->upgrading_ == txn_id) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    queue->cv_.notify_all();
    queue->latch_.unlock();
    return false;
  }
  auto request = queue->request_queue_.front();
  request->granted_ = true;
  auto lock_mode_idx = static_cast<uint32_t>(request->lock_mode_);
  queue->granted_lock_cnts_[lock_mode_idx]++;
  queue->granted_lock_req_map_[txn_id] = request;
  queue->request_queue_.pop_front();
  // If a lock is granted to a transaction, lock manager should update
  // its lock sets appropriately (check transaction.h)
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
  if (queue->upgrading_ == txn_id) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  queue->cv_.notify_all();
  queue->latch_.unlock();

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // Checks:
  // 1. ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
  // 2. TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS
  auto txn_id = txn->GetTransactionId();
  if (!txn->IsTableSharedLocked(oid) && !txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionSharedLocked(oid) &&
      !txn->IsTableIntentionExclusiveLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    txn->SetState(TransactionState::ABORTED);
    std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (txn->GetSharedRowLockSet()->count(oid) != 0 && !txn->GetSharedRowLockSet()->at(oid).empty()) {
    txn->SetState(TransactionState::ABORTED);
    std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
    throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto isolation_level = txn->GetIsolationLevel();

  // Unlock operations
  table_lock_map_latch_.lock();
  auto &queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  queue->latch_.lock();
  auto request = queue->granted_lock_req_map_[txn_id];
  auto lock_mode_idx = static_cast<uint32_t>(request->lock_mode_);
  queue->granted_lock_cnts_[lock_mode_idx]--;
  queue->granted_lock_req_map_.erase(txn_id);
  const auto lock_mode = request->lock_mode_;
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED || isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  // After a resource is unlocked, lock manager should update
  // the transaction's lock sets appropriately (check transaction.h)
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
  queue->cv_.notify_all();
  queue->latch_.unlock();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // Checks:
  // 1. SUPPORTED LOCK MODES
  // 2. ISOLATION LEVEL
  // 3. TABLE_LOCK_NOT_PRESENT
  auto txn_id = txn->GetTransactionId();
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  auto isolation_level = txn->GetIsolationLevel();
  auto state = txn->GetState();
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_COMMITTED) {
    if (state == TransactionState::SHRINKING && (lock_mode != LockMode::SHARED)) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode != LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    }
  }
  bool table_lock_present = true;
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (txn->GetExclusiveTableLockSet()->count(oid) == 0 && txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      table_lock_present = false;
    }
  } else {
    if (txn->GetSharedTableLockSet()->count(oid) == 0 && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionSharedTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {  // TODO(1ycheen): not sure.
      table_lock_present = false;
    }
  }
  if (!table_lock_present) {
    txn->SetState(TransactionState::ABORTED);
    std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
    throw TransactionAbortException(txn_id, AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  // Lock operations
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  auto &queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  queue->latch_.lock();
  // Check upgrade
  if (queue->granted_lock_req_map_.count(txn_id) != 0) {
    auto request = queue->granted_lock_req_map_[txn_id];
    const auto &prev_mode = request->lock_mode_;
    if (lock_mode == prev_mode) {
      queue->latch_.unlock();
      return true;
    }
    // 1. Check the precondition of upgrade
    bool permitted = false;
    if (prev_mode == LockMode::SHARED && lock_mode == LockMode::EXCLUSIVE) {
      permitted = true;
    }
    if (!permitted) {
      queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::INCOMPATIBLE_UPGRADE);
    }
    if (queue->upgrading_ != INVALID_TXN_ID && queue->upgrading_ != txn_id) {
      queue->latch_.unlock();
      txn->SetState(TransactionState::ABORTED);
      std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    }
    // 2. Drop the current lock, reserve the upgrade position
    queue->upgrading_ = txn_id;
    auto prev_mode_idx = static_cast<uint32_t>(prev_mode);
    queue->granted_lock_cnts_[prev_mode_idx]--;
    queue->granted_lock_req_map_.erase(txn_id);
    queue->request_queue_.emplace_front(new LockRequest(txn_id, lock_mode, oid, rid));
    if (prev_mode == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
    } else if (prev_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
    }
    // 3. Wait to get the new lock granted
  } else {
    queue->request_queue_.emplace_back(new LockRequest(txn_id, lock_mode, oid, rid));
  }
  while (txn->GetState() != TransactionState::ABORTED &&
         (queue->request_queue_.front()->txn_id_ != txn_id ||
          !CheckIfCanLock(queue, lock_mode))) {  // TODO(1ycheen): this cond may wrong
    std::unique_lock<std::mutex> l(queue->latch_, std::defer_lock);
    queue->cv_.wait(l);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    auto it = queue->request_queue_.begin();
    for (; it != queue->request_queue_.end(); ++it) {
      if ((*it)->txn_id_ == txn_id) {
        break;
      }
    }
    queue->request_queue_.erase(it);
    if (queue->upgrading_ == txn_id) {
      queue->upgrading_ = INVALID_TXN_ID;
    }
    queue->cv_.notify_all();
    queue->latch_.unlock();
    return false;
  }
  auto request = queue->request_queue_.front();
  request->granted_ = true;
  auto lock_mode_idx = static_cast<uint32_t>(request->lock_mode_);
  queue->granted_lock_cnts_[lock_mode_idx]++;
  queue->granted_lock_req_map_[txn_id] = request;
  queue->request_queue_.pop_front();
  // If a lock is granted to a transaction, lock manager should update
  // its lock sets appropriately (check transaction.h)
  if (lock_mode == LockMode::SHARED) {
    if (txn->GetSharedRowLockSet()->count(oid) == 0) {
      txn->GetSharedRowLockSet()->insert({oid, {}});
    }
    txn->GetSharedRowLockSet()->at(oid).insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    if (txn->GetExclusiveRowLockSet()->count(oid) == 0) {
      txn->GetExclusiveRowLockSet()->insert({oid, {}});
    }
    txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
  }
  if (queue->upgrading_ == txn_id) {
    queue->upgrading_ = INVALID_TXN_ID;
  }
  queue->cv_.notify_all();
  queue->latch_.unlock();

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // Checks:
  // 1. ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
  auto txn_id = txn->GetTransactionId();
  if (!force && !txn->IsRowExclusiveLocked(oid, rid) && !txn->IsRowSharedLocked(oid, rid)) {
    txn->SetState(TransactionState::ABORTED);
    std::cerr << __FUNCTION__ << " " << __LINE__ << std::endl;
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto isolation_level = txn->GetIsolationLevel();

  // Unlock operations
  row_lock_map_latch_.lock();
  auto &queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  queue->latch_.lock();
  if (queue->granted_lock_req_map_.count(txn_id) == 0) {
    queue->cv_.notify_all();
    queue->latch_.unlock();
    return true;
  }
  auto request = queue->granted_lock_req_map_[txn_id];
  auto lock_mode_idx = static_cast<uint32_t>(request->lock_mode_);
  queue->granted_lock_cnts_[lock_mode_idx]--;
  queue->granted_lock_req_map_.erase(txn_id);
  const auto lock_mode = request->lock_mode_;
  if (!force) {
    if (isolation_level == IsolationLevel::REPEATABLE_READ) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    } else if (isolation_level == IsolationLevel::READ_COMMITTED ||
               isolation_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }
  }
  // After a resource is unlocked, lock manager should update
  // the transaction's lock sets appropriately (check transaction.h)
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedRowLockSet()->at(oid).erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
  }
  queue->cv_.notify_all();
  queue->latch_.unlock();

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  for (const auto &txn_id : waits_for_[t1]) {
    if (txn_id == t2) {
      return;
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto it = waits_for_.find(t1);
  if (it == waits_for_.end()) {
    return;
  }
  for (uint32_t idx = 0; idx < it->second.size(); ++idx) {
    if (it->second[idx] == t2) {
      it->second.erase(it->second.begin() + idx);
      break;
    }
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, txn_id_t youngest_txn_id, std::unordered_set<txn_id_t> &visited,
                            std::unordered_set<txn_id_t> &on_path, txn_id_t *abort_txn_id) -> bool {
  if (waits_for_.count(source_txn) == 0) {
    return false;
  }
  visited.insert(source_txn);
  on_path.insert(source_txn);
  for (auto &txn2 : waits_for_[source_txn]) {
    if (on_path.count(txn2) != 0) {
      *abort_txn_id = youngest_txn_id;
      return true;
    }
    bool res = FindCycle(txn2, std::max(youngest_txn_id, txn2), visited, on_path, abort_txn_id);
    if (res) {
      return res;
    }
  }
  on_path.erase(source_txn);
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::vector<txn_id_t> txn_ids;
  for (auto &p : waits_for_) {
    txn_ids.push_back(p.first);
  }
  for (auto &txn_id : txn_ids) {
    auto it = waits_for_.find(txn_id);
    std::sort(it->second.begin(), it->second.end());
  }
  // Break cycles
  std::sort(txn_ids.begin(), txn_ids.end());
  std::unordered_set<txn_id_t> visited;
  for (auto &source_txn_id : txn_ids) {
    if (visited.count(source_txn_id) != 0) {
      continue;
    }
    std::unordered_set<txn_id_t> on_path;
    auto res = FindCycle(source_txn_id, -1, visited, on_path, txn_id);
    if (res) {
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (auto &it : waits_for_) {
    for (auto &t2 : it.second) {
      edges.emplace_back(it.first, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::lock_guard<std::mutex> table_lg(table_lock_map_latch_);
      std::lock_guard<std::mutex> row_lg(row_lock_map_latch_);
      std::lock_guard<std::mutex> waits_for_lg(waits_for_latch_);
      std::unordered_map<txn_id_t, std::vector<std::shared_ptr<LockRequestQueue>>> txn_wake_up_map;
      // Build Waits-for graph representation
      for (auto &p : table_lock_map_) {
        std::lock_guard<std::mutex> queue_lg(p.second->latch_);
        // TODO(1ycheen): find those really blocks waiting lock requests
        for (auto &txn1_req : p.second->request_queue_) {
          auto txn1 = txn1_req->txn_id_;
          txn_wake_up_map[txn1].push_back(p.second);
          for (auto [txn2, _] : p.second->granted_lock_req_map_) {
            AddEdge(txn1, txn2);
          }
        }
      }
      for (auto &p : row_lock_map_) {
        std::lock_guard<std::mutex> queue_lg(p.second->latch_);
        // TODO(1ycheen): find those really blocks waiting lock requests
        for (auto &txn1_req : p.second->request_queue_) {
          auto txn1 = txn1_req->txn_id_;
          txn_wake_up_map[txn1].push_back(p.second);
          for (auto [txn2, _] : p.second->granted_lock_req_map_) {
            AddEdge(txn1, txn2);
          }
        }
      }
      // Break cycles
      txn_id_t abort_txn_id;
      while (HasCycle(&abort_txn_id)) {
        printf("Has cycle: %d\n", abort_txn_id);
        waits_for_.erase(abort_txn_id);
        // Set abort state of break_txn_id
        auto txn = txn_manager_->GetTransaction(abort_txn_id);
        txn->SetState(TransactionState::ABORTED);
        for (auto &q : txn_wake_up_map[abort_txn_id]) {
          q->cv_.notify_all();
        }
      }
      // Clear
      waits_for_.clear();
    }
  }
}

}  // namespace bustub
