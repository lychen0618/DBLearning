//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      schema_({Column{"#", TypeId::INTEGER}}) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_arr_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  finished_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
    // Delete
    TupleMeta tuple_meta = table_info_->table_->GetTupleMeta(*rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    for (auto &index_info : index_info_arr_) {
      Tuple key =
          child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, nullptr);
    }
    count++;
  }
  return false;
}

}  // namespace bustub
