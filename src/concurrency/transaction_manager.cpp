//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"

namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  txn->LockTxn();
  auto index_write_set = txn->GetIndexWriteSet();
  auto table_write_set = txn->GetWriteSet();
  for (auto &write_record : *index_write_set) {
    auto index_info = write_record.catalog_->GetIndex(write_record.index_oid_);
    if (write_record.wtype_ == WType::INSERT) {
      auto key = write_record.tuple_.KeyFromTuple(write_record.catalog_->GetTable(write_record.table_oid_)->schema_,
                                                  index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, write_record.rid_, txn);
    } else if (write_record.wtype_ == WType::DELETE) {
      auto key = write_record.tuple_.KeyFromTuple(write_record.catalog_->GetTable(write_record.table_oid_)->schema_,
                                                  index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, write_record.rid_, txn);
    }
  }
  for (auto &write_record : *table_write_set) {
    auto table = write_record.table_heap_;
    TupleMeta tuple_meta = table->GetTupleMeta(write_record.rid_);
    tuple_meta.is_deleted_ = !tuple_meta.is_deleted_;
    table->UpdateTupleMeta(tuple_meta, write_record.rid_);
  }
  index_write_set->clear();
  table_write_set->clear();
  txn->UnlockTxn();

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
