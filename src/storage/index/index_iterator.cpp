/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t cur_page_id, int idx, BufferPoolManager *bpm)
    : cur_page_id_(cur_page_id), idx_(idx), bpm_(bpm) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return cur_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BasicPageGuard guard = bpm_->FetchPageBasic(cur_page_id_);
  auto leaf_page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return leaf_page->PairAt(idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BasicPageGuard guard = bpm_->FetchPageBasic(cur_page_id_);
  auto leaf_page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  idx_++;
  if (idx_ >= leaf_page->GetSize()) {
    cur_page_id_ = leaf_page->GetNextPageId();
    idx_ = 0;
    if (cur_page_id_ == INVALID_PAGE_ID) {
      bpm_ = nullptr;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
