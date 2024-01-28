//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  l_status_ = left_executor_->Next(&left_tuple_, &l_rid_);
  matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID r_rid;
  while (l_status_) {
    auto r_status = right_executor_->Next(&right_tuple, &r_rid);
    if (!r_status) {
      if (!matched_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> l_values = left_tuple_.GetValues(&left_executor_->GetOutputSchema());
        std::vector<Value> r_values{};
        for (auto &col : right_executor_->GetOutputSchema().GetColumns()) {
          r_values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
        }
        l_values.insert(l_values.end(), r_values.begin(), r_values.end());
        *tuple = Tuple{l_values, &GetOutputSchema()};
        matched_ = true;
        return true;
      }
      right_executor_->Init();
      r_status = right_executor_->Next(&right_tuple, &r_rid);
      l_status_ = left_executor_->Next(&left_tuple_, &l_rid_);
      matched_ = false;
      if (!l_status_) {
        return false;
      }
      if (!r_status) {
        continue;
      }
    }
    auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                  right_executor_->GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      std::vector<Value> l_values = left_tuple_.GetValues(&left_executor_->GetOutputSchema());
      std::vector<Value> r_values = right_tuple.GetValues(&right_executor_->GetOutputSchema());
      l_values.insert(l_values.end(), r_values.begin(), r_values.end());
      *tuple = Tuple{l_values, &GetOutputSchema()};
      matched_ = true;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
