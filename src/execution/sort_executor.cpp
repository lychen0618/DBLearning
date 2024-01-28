#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  const auto &order_bys = plan_->GetOrderBy();
  Tuple child_tuple;
  RID rid;
  tuples_.clear();
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &rid);
    if (!status) {
      break;
    }
    tuples_.push_back(child_tuple);
  }
  const auto &schema = child_executor_->GetOutputSchema();
  auto cmp = [&order_bys, &schema](const Tuple &a, const Tuple &b) -> bool {
    for (auto &order_by : order_bys) {
      Value va = order_by.second->Evaluate(&a, schema);
      Value vb = order_by.second->Evaluate(&b, schema);
      if (va.CompareNotEquals(vb) == CmpBool::CmpTrue) {
        return (order_by.first == OrderByType::DESC) ? (va.CompareGreaterThan(vb) == CmpBool::CmpTrue)
                                                     : (va.CompareLessThan(vb) == CmpBool::CmpTrue);
      }
    }
    return false;
  };
  std::sort(tuples_.begin(), tuples_.end(), cmp);
  idx_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[idx_];
  *rid = tuple->GetRid();
  ++idx_;
  return true;
}

}  // namespace bustub
