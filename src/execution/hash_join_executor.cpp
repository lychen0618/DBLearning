//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      hht_(plan_->LeftJoinKeyExpressions(), plan_->RightJoinKeyExpressions()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  l_status_ = left_executor_->Next(&left_tuple_, &l_rid_);
  matched = false;
  if (!l_status_) {
    return;
  }
  right_executor_->Init();
  Tuple right_tuple{};
  RID r_rid;
  hht_.Clear();
  while (true) {
    const auto status = right_executor_->Next(&right_tuple, &r_rid);
    if (!status) {
      return;
    }
    hht_.InsertCombine(&right_tuple, right_executor_->GetOutputSchema());
  }
  value_idx_ = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!l_status_) {
    return false;
  }
  while (l_status_) {
    auto iter = hht_.Find(&left_tuple_, left_executor_->GetOutputSchema());
    if (iter == hht_.End()) {
      if (!matched && plan_->GetJoinType() == JoinType::LEFT) {
        // merge left_tuple and right_tuple
        std::vector<Value> l_values = left_tuple_.GetValues(&left_executor_->GetOutputSchema());
        std::vector<Value> r_values{};
        for (auto &col : right_executor_->GetOutputSchema().GetColumns()) {
          r_values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
        l_values.insert(l_values.end(), r_values.begin(), r_values.end());
        *tuple = Tuple{l_values, &GetOutputSchema()};
        matched = true;
        return true;
      }
    } else {
      if (value_idx_ < iter->second.size()) {
        // merge left_tuple and right_tuple
        std::vector<Value> l_values = left_tuple_.GetValues(&left_executor_->GetOutputSchema());
        const std::vector<Value> &r_values = iter->second[value_idx_].values_;
        l_values.insert(l_values.end(), r_values.begin(), r_values.end());
        *tuple = Tuple{l_values, &GetOutputSchema()};
        ++value_idx_;
        return true;
      }
    }
    l_status_ = left_executor_->Next(&left_tuple_, &l_rid_);
    matched = false;
    value_idx_ = 0;
  }
  return false;
}

}  // namespace bustub
