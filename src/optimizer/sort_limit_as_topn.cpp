#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit && optimized_plan->GetChildren().size() == 1) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto child = limit_plan.GetChildAt(0);
    if (child->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child);
      auto topn_plan = std::make_unique<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0),
                                                      sort_plan.GetOrderBy(), limit_plan.GetLimit());
      optimized_plan = std::move(topn_plan);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
