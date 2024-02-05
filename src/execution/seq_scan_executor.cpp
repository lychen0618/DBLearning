//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_oid_(plan_->GetTableOid()) {}

void SeqScanExecutor::Init() {
  auto level = exec_ctx_->GetTransaction()->GetIsolationLevel();
  auto txn = exec_ctx_->GetTransaction();
  if (exec_ctx_->IsDelete() || level != IsolationLevel::READ_UNCOMMITTED) {
    auto lock_mode = exec_ctx_->IsDelete()
                         ? LockManager::LockMode::INTENTION_EXCLUSIVE
                         : (level == IsolationLevel::READ_UNCOMMITTED ? LockManager::LockMode::INTENTION_EXCLUSIVE
                                                                      : LockManager::LockMode::INTENTION_SHARED);
    bool locked =
        (lock_mode == LockManager::LockMode::INTENTION_SHARED && txn->IsTableIntentionExclusiveLocked(table_oid_));
    if (!locked) {
      bool res = exec_ctx_->GetLockManager()->LockTable(txn, lock_mode, table_oid_);
      if (!res) {
        throw ExecutionException("Failed to lock table in SeqScanExecutor.");
      }
    }
  }
  iter_ = std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(table_oid_)->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  bool found = false;
  while (!iter_->IsEnd()) {
    auto txn = exec_ctx_->GetTransaction();
    auto iter_rid = iter_->GetRID();
    bool locked = false;
    if (exec_ctx_->IsDelete() || txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      auto lock_mode = (exec_ctx_->IsDelete() ? LockManager::LockMode::EXCLUSIVE : LockManager::LockMode::SHARED);
      locked = (lock_mode == LockManager::LockMode::SHARED && txn->IsRowExclusiveLocked(table_oid_, iter_rid));
      if (!locked) {
        bool res = exec_ctx_->GetLockManager()->LockRow(txn, lock_mode, table_oid_, iter_rid);
        if (!res) {
          throw ExecutionException("Failed to lock row in SeqScanExecutor.");
        }
      }
    }
    auto tuple_pair = iter_->GetTuple();
    ++(*iter_);
    if (!tuple_pair.first.is_deleted_) {
      found = true;
      *tuple = tuple_pair.second;
      *rid = tuple->GetRid();
      if (!exec_ctx_->IsDelete() && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !locked) {
        bool res = exec_ctx_->GetLockManager()->UnlockRow(txn, table_oid_, iter_rid);
        if (!res) {
          throw ExecutionException("Failed to unlock row in SeqScanExecutor.");
        }
      }
      break;
    }
    if ((exec_ctx_->IsDelete() || txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) && !locked) {
      bool res = exec_ctx_->GetLockManager()->UnlockRow(txn, table_oid_, iter_rid, true);
      if (!res) {
        throw ExecutionException("Failed to unlock row in SeqScanExecutor.");
      }
    }
  }
  return found;
}

}  // namespace bustub
