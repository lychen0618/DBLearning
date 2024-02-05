//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      schema_({Column{"#", TypeId::INTEGER}}) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  bool res = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(),
                                                    LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
  if (!res) {
    throw ExecutionException("Failed to lock table in InsertExecutor.");
  }
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_arr_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  Tuple child_tuple{};
  int count = 0;
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      *tuple = Tuple{{Value{TypeId::INTEGER, count}}, &schema_};
      table_info_ = nullptr;
      return true;
    }
    auto txn = exec_ctx_->GetTransaction();
    auto new_rid = table_info_->table_->InsertTuple(TupleMeta{txn->GetTransactionId(), INVALID_TXN_ID, false},
                                                    child_tuple, exec_ctx_->GetLockManager(), txn, table_info_->oid_);
    if (new_rid == std::nullopt) {
      break;
    }
    bool flag = true;
    for (auto &index_info : index_info_arr_) {
      Tuple key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      bool res = index_info->index_->InsertEntry(key, *new_rid, txn);
      if (!res) {
        flag = false;
        break;
      }
    }
    if (!flag) {
      break;
    }
    txn->LockTxn();
    txn->AppendTableWriteRecord(TableWriteRecord{table_info_->oid_, *new_rid, table_info_->table_.get()});
    for (auto &index_info : index_info_arr_) {
      txn->AppendIndexWriteRecord(IndexWriteRecord(*new_rid, table_info_->oid_, WType::INSERT, child_tuple,
                                                   index_info->index_oid_, exec_ctx_->GetCatalog()));
    }
    txn->UnlockTxn();
    count++;
  }
  return false;
}

}  // namespace bustub
