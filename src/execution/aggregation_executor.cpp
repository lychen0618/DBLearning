//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  has_out_ = plan_->GetGroupBys().empty();
  aht_.Clear();
  // Get the next tuple
  Tuple child_tuple{};
  RID rid;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &rid);
    if (!status) {
      break;
    }
    // Construct agg key
    std::vector<Value> kvalues;
    kvalues.reserve(plan_->GetGroupBys().size());
    for (const auto &expr : plan_->GetGroupBys()) {
      kvalues.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    // Construct agg value
    std::vector<Value> vvalues;
    vvalues.reserve(plan_->GetAggregates().size());
    for (const auto &expr : plan_->GetAggregates()) {
      vvalues.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    aht_.InsertCombine(AggregateKey{kvalues}, AggregateValue{vvalues});
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    if (has_out_) {
      *tuple = Tuple{aht_.GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
      has_out_ = false;
      return true;
    }
    return false;
  }
  has_out_ = false;
  std::vector<Value> values = aht_iterator_.Key().group_bys_;
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
