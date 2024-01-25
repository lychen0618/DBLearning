//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t cur_page_id, int idx, BufferPoolManager *bpm);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return cur_page_id_ == itr.cur_page_id_ && idx_ == itr.idx_ && bpm_ == itr.bpm_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return cur_page_id_ != itr.cur_page_id_ || idx_ != itr.idx_ || bpm_ != itr.bpm_;
  }

 private:
  // add your own private member variables here
  page_id_t cur_page_id_ = INVALID_PAGE_ID;
  int idx_ = 0;
  BufferPoolManager *bpm_ = nullptr;
};

}  // namespace bustub
