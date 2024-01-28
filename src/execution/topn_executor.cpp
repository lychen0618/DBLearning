#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  const auto &order_bys = plan_->GetOrderBy();
  const auto &schema = plan_->OutputSchema();
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
  heap_ = HeapType(cmp);
}

void TopNExecutor::Init() {
  child_executor_->Init();
  const auto topn = plan_->GetN();
  Tuple child_tuple;
  RID rid;
  while (true) {
    const auto status = child_executor_->Next(&child_tuple, &rid);
    if (!status) {
      break;
    }
    heap_.push(child_tuple);
    if (heap_.size() > topn) {
      heap_.pop();
    }
  }
  num_in_heap_ = 0;
  tuples_.clear();
  while (!heap_.empty()) {
    tuples_.push_back(heap_.top());
    heap_.pop();
  }
  std::reverse(tuples_.begin(), tuples_.end());
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (num_in_heap_ >= tuples_.size()) {
    return false;
  }
  *tuple = tuples_[num_in_heap_++];
  *rid = tuple->GetRid();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return num_in_heap_; };

}  // namespace bustub
