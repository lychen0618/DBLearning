//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      schema_({Column{"#", TypeId::INTEGER}}) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  index_info_arr_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  finished_ = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  Tuple child_tuple{};
  int count = 0;
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&child_tuple, rid);
    if (!status) {
      *tuple = Tuple{{Value{TypeId::INTEGER, count}}, &schema_};
      finished_ = true;
      return true;
    }
    // Delete then insert
    auto crid = child_tuple.GetRid();
    TupleMeta tuple_meta = table_info_->table_->GetTupleMeta(crid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, crid);
    for (auto &index_info : index_info_arr_) {
      Tuple key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, crid, nullptr);
    }
    std::vector<Value> values{};
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple updated = Tuple{values, &child_executor_->GetOutputSchema()};
    auto new_rid = table_info_->table_->InsertTuple(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, updated);
    if (new_rid == std::nullopt) {
      break;
    }
    bool flag = true;
    for (auto &index_info : index_info_arr_) {
      Tuple key =
          updated.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      bool res = index_info->index_->InsertEntry(key, *new_rid, nullptr);
      if (!res) {
        flag = false;
        break;
      }
    }
    if (!flag) {
      break;
    }
    count++;
  }
  return false;
}

}  // namespace bustub
