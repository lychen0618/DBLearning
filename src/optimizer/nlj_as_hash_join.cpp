#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
auto IsColumnEqualExpr(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_key_exprs,
                       std::vector<AbstractExpressionRef> &right_key_exprs) -> bool {
  if (const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      cmp_expr != nullptr && cmp_expr->GetChildren().size() == 2) {
    const auto *col_expr0 = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
    const auto *col_expr1 = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(1).get());
    if (col_expr0 != nullptr && col_expr1 != nullptr && col_expr0->GetTupleIdx() != col_expr1->GetTupleIdx()) {
      if (col_expr0->GetTupleIdx() == 0) {
        left_key_exprs.emplace_back(cmp_expr->GetChildAt(0));
        right_key_exprs.emplace_back(cmp_expr->GetChildAt(1));
      } else {
        left_key_exprs.emplace_back(cmp_expr->GetChildAt(1));
        right_key_exprs.emplace_back(cmp_expr->GetChildAt(0));
      }
      return true;
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Check opt condition
    auto expr = nlj_plan.Predicate();
    bool cond = false;
    std::vector<AbstractExpressionRef> left_key_exprs;
    std::vector<AbstractExpressionRef> right_key_exprs;
    // 1. <column expr> = <column expr>
    if (IsColumnEqualExpr(expr, left_key_exprs, right_key_exprs)) {
      cond = true;
    }
    // 2. <column expr> = <column expr> AND <column expr> = <column expr>
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get());
        !cond && logic_expr != nullptr && logic_expr->GetChildren().size() == 2) {
      if (IsColumnEqualExpr(logic_expr->GetChildAt(0), left_key_exprs, right_key_exprs) &&
          IsColumnEqualExpr(logic_expr->GetChildAt(1), left_key_exprs, right_key_exprs)) {
        cond = true;
      }
    }
    // Construct opt plan node
    if (cond) {
      auto node =
          std::make_unique<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                             left_key_exprs, right_key_exprs, nlj_plan.GetJoinType());
      optimized_plan = node->CloneWithChildren(optimized_plan->GetChildren());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
