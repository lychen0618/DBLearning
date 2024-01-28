//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  std::vector<Value> keys_;
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  std::vector<Value> values_;
};
}  // namespace bustub

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {
auto HJGetValuesHelper(const Tuple *tuple, const Schema *schema) -> std::vector<Value>;

/**
 * A simplified hash table that has all the necessary functionality for hash join.
 */
class SimpleHashJoinHashTable {
 public:
  using HashJoinIterator = std::unordered_map<HashJoinKey, std::vector<HashJoinValue>>::const_iterator;
  SimpleHashJoinHashTable(const std::vector<AbstractExpressionRef> &left_key_exprs,
                          const std::vector<AbstractExpressionRef> &right_key_exprs)
      : left_key_expressions_(left_key_exprs), right_key_expressions_(right_key_exprs) {}

  void InsertCombine(const Tuple *tuple, const Schema &schema) {
    auto right_join_key = GetHashJoinKey(tuple, schema, right_key_expressions_);
    if (ht_.count(right_join_key) == 0) {
      ht_[right_join_key] = {};
    }
    ht_[right_join_key].emplace_back(HashJoinValue{HJGetValuesHelper(tuple, &schema)});
  }

  auto Find(const Tuple *tuple, const Schema &schema) const -> HashJoinIterator {
    auto left_join_key = GetHashJoinKey(tuple, schema, left_key_expressions_);
    return ht_.find(left_join_key);
  }

  void Clear() { ht_.clear(); }

  auto End() -> HashJoinIterator { return ht_.cend(); }

 private:
  auto GetHashJoinKey(const Tuple *tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &exprs) const
      -> HashJoinKey {
    std::vector<Value> keys;
    keys.reserve(exprs.size());
    for (const auto &expr : exprs) {
      keys.emplace_back(expr->Evaluate(tuple, schema));
    }
    return {keys};
  }
  const std::vector<AbstractExpressionRef> &left_key_expressions_;
  const std::vector<AbstractExpressionRef> &right_key_expressions_;
  std::unordered_map<HashJoinKey, std::vector<HashJoinValue>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  SimpleHashJoinHashTable hht_;
  uint32_t value_idx_ = 0;
  Tuple left_tuple_;
  RID l_rid_;
  bool l_status_;
  bool matched_;
};

}  // namespace bustub
