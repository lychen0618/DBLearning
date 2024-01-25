#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafBinarySearch(const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page,
                                      const KeyType &key) -> int {
  if (page->GetSize() == 0) {
    return 0;
  }
  int left = 0;
  int right = page->GetSize() - 1;
  int pos = page->GetSize();
  while (left <= right) {
    int mid = (left + right) / 2;
    int cmp = comparator_(page->KeyAt(mid), key);
    if (cmp < 0) {
      left = mid + 1;
    } else {
      pos = mid;
      right = mid - 1;
    }
  }
  return pos;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalBinarySearch(const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *page,
                                          const KeyType &key) -> int {
  if (page->GetSize() <= 1) {
    return 0;
  }
  int left = 1;
  int right = page->GetSize() - 1;
  int pos = 0;
  while (left <= right) {
    int mid = (left + right) / 2;
    int cmp = comparator_(page->KeyAt(mid), key);
    if (cmp <= 0) {
      pos = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return pos;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, [[maybe_unused]] Transaction *txn)
    -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  page_id_t cur_page_id = header_page->root_page_id_;
  ReadPageGuard root_guard = bpm_->FetchPageRead(cur_page_id);
  guard = std::move(root_guard);
  while (cur_page_id != INVALID_PAGE_ID) {
    // Fetch page
    if (guard.IsEmpty()) {
      break;
    }
    auto cur_page = guard.As<BPlusTreePage>();
    // Search key
    if (cur_page->IsLeafPage()) {
      auto leaf_page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
      int pos = LeafBinarySearch(leaf_page, key);
      if (pos < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(pos), key) == 0) {
        result->emplace_back(leaf_page->ValueAt(pos));
        return true;
      }
      cur_page_id = INVALID_PAGE_ID;
    } else {
      auto page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      int pos = InternalBinarySearch(page, key);
      cur_page_id = page->ValueAt(pos);
    }
    // Latch coupling
    if (cur_page_id != INVALID_PAGE_ID) {
      ReadPageGuard child_guard = bpm_->FetchPageRead(cur_page_id);
      guard = std::move(child_guard);
    }
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // New root page
    page_id_t root_page_id;
    BasicPageGuard guard = bpm_->NewPageGuarded(&root_page_id);
    header_page->root_page_id_ = root_page_id;
    auto root_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    root_page->Init(leaf_max_size_);
  }
  // Declaration of context instance.
  Context ctx;
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  // Find place to insert
  page_id_t cur_page_id = ctx.root_page_id_;
  bool root_flag = true;
  while (cur_page_id != INVALID_PAGE_ID) {
    WritePageGuard guard = bpm_->FetchPageWrite(cur_page_id);
    if (guard.IsEmpty()) {
      return false;
    }
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      // Insert
      auto leaf_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
      char *src = reinterpret_cast<char *>(leaf_page) + LEAF_PAGE_HEADER_SIZE +
                  static_cast<int>(sizeof(MappingType)) * leaf_page->GetSize();
      int pos = LeafBinarySearch(leaf_page, key);
      if (pos < leaf_page->GetSize() && comparator_(key, leaf_page->KeyAt(pos)) == 0) {
        return false;
      }
      if (pos < leaf_page->GetSize()) {
        src = reinterpret_cast<char *>(leaf_page) + LEAF_PAGE_HEADER_SIZE + static_cast<int>(sizeof(MappingType)) * pos;
        memmove(src + sizeof(MappingType), src, sizeof(MappingType) * (leaf_page->GetSize() - pos));
      }
      *(reinterpret_cast<MappingType *>(src)) = {key, value};
      leaf_page->IncreaseSize(1);
    } else {
      // Search
      auto page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      int pos = InternalBinarySearch(page, key);
      cur_page_id = page->ValueAt(pos);
    }
    if (page->GetSize() < page->GetMaxSize()) {
      if (root_flag) {
        ctx.header_page_ = std::nullopt;
      } else {
        ctx.write_set_.clear();
      }
    }
    root_flag = false;
    ctx.write_set_.emplace_front(std::move(guard));
    if (page->IsLeafPage()) {
      break;
    }
  }

  // Check if break rules
  size_t idx = 0;
  for (auto &guard : ctx.write_set_) {
    // Check
    auto page = guard.As<BPlusTreePage>();
    auto cur_page_id = guard.PageId();
    if (page->GetSize() <= page->GetMaxSize()) {
      break;
    }
    // Split
    auto m_page = guard.AsMut<BPlusTreePage>();
    int m = m_page->GetSize();
    page_id_t new_page_id;
    auto new_page_guard = bpm_->NewPageGuarded(&new_page_id);
    auto new_page = new_page_guard.AsMut<BPlusTreePage>();
    KeyType *mid_key;
    if (m_page->IsLeafPage()) {
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_page)->Init(leaf_max_size_);
      page_id_t n_page_id =
          reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(m_page)->GetNextPageId();
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_page)->SetNextPageId(n_page_id);
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(m_page)->SetNextPageId(new_page_id);
      new_page->SetSize((m + 1) / 2);
      char *src = reinterpret_cast<char *>(m_page) + LEAF_PAGE_HEADER_SIZE + (m / 2) * sizeof(MappingType);
      memmove(reinterpret_cast<char *>(new_page) + LEAF_PAGE_HEADER_SIZE, src, (m + 1) / 2 * sizeof(MappingType));
      mid_key = reinterpret_cast<KeyType *>(src);
      m_page->SetSize(m / 2);
    } else {
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page)->Init(internal_max_size_);
      new_page->SetSize((m + 1) / 2);
      char *src = reinterpret_cast<char *>(m_page) + INTERNAL_PAGE_HEADER_SIZE +
                  (m / 2) * sizeof(std::pair<KeyType, page_id_t>);
      memmove(reinterpret_cast<char *>(new_page) + INTERNAL_PAGE_HEADER_SIZE, src,
              (m + 1) / 2 * sizeof(std::pair<KeyType, page_id_t>));
      mid_key = reinterpret_cast<KeyType *>(src);
      m_page->SetSize(m / 2);
    }
    if (ctx.IsRootPage(guard.PageId())) {
      // New root page
      page_id_t root_page_id;
      BasicPageGuard guard = bpm_->NewPageGuarded(&root_page_id);
      header_page->root_page_id_ = root_page_id;
      auto root_page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      root_page->Init(internal_max_size_);
      root_page->SetSize(2);
      root_page->SetKeyAt(1, *mid_key);
      root_page->SetValueAt(0, cur_page_id);
      root_page->SetValueAt(1, new_page_id);
    } else {
      auto &par_guard = ctx.write_set_[idx + 1];
      auto par_page = par_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      int pos = InternalBinarySearch(par_page, *mid_key) + 1;
      if (pos < par_page->GetSize()) {
        size_t pair_size = sizeof(std::pair<KeyType, page_id_t>);
        char *src = reinterpret_cast<char *>(par_page) + INTERNAL_PAGE_HEADER_SIZE + pos * pair_size;
        memmove(src + pair_size, src, pair_size * (par_page->GetSize() - pos));
      }
      par_page->IncreaseSize(1);
      par_page->SetKeyAt(pos, *mid_key);
      par_page->SetValueAt(pos - 1, cur_page_id);
      par_page->SetValueAt(pos, new_page_id);
    }
    idx++;
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  // Declaration of context instance.
  Context ctx;
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  // Find the delete key.
  page_id_t cur_page_id = ctx.root_page_id_;
  bool root_flag = true;
  while (cur_page_id != INVALID_PAGE_ID) {
    WritePageGuard guard = bpm_->FetchPageWrite(cur_page_id);
    if (guard.IsEmpty()) {
      return;
    }
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      auto leaf_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
      int pos = LeafBinarySearch(leaf_page, key);
      if (pos < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(pos), key) == 0) {
        char *src =
            reinterpret_cast<char *>(leaf_page) + LEAF_PAGE_HEADER_SIZE + static_cast<int>(sizeof(MappingType)) * pos;
        memmove(src, src + sizeof(MappingType), sizeof(MappingType) * (leaf_page->GetSize() - pos - 1));
      } else {
        return;
      }
      leaf_page->IncreaseSize(-1);
    } else {
      // Search
      auto page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      int pos = InternalBinarySearch(page, key);
      cur_page_id = page->ValueAt(pos);
    }
    if (page->GetSize() > page->GetMinSize()) {
      if (root_flag) {
        ctx.header_page_ = std::nullopt;
      } else {
        ctx.write_set_.clear();
      }
    }
    root_flag = false;
    ctx.write_set_.emplace_front(std::move(guard));
    if (page->IsLeafPage()) {
      break;
    }
  }

  // Check if break rules.
  size_t idx = 0;
  for (auto &guard : ctx.write_set_) {
    // Check
    auto page = guard.As<BPlusTreePage>();
    auto cur_page_id = guard.PageId();
    if (page->GetSize() >= page->GetMinSize()) {
      break;
    }

    // Deal with breaks
    if (!ctx.IsRootPage(guard.PageId())) {
      // Find brother page
      auto &par_guard = ctx.write_set_[idx + 1];
      auto par_page = par_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      int i = 0;
      for (; i < par_page->GetSize(); i++) {
        if (par_page->ValueAt(i) == cur_page_id) {
          break;
        }
      }

      if (page->IsLeafPage()) {
        auto cur_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
        // Distribute
        if (i != 0) {
          WritePageGuard guard = bpm_->FetchPageWrite(par_page->ValueAt(i - 1));
          auto bro_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
          if (bro_page->GetSize() > bro_page->GetMinSize()) {
            par_page->SetKeyAt(i, bro_page->KeyAt(bro_page->GetSize() - 1));
            char *src = reinterpret_cast<char *>(cur_page) + LEAF_PAGE_HEADER_SIZE;
            memmove(src + sizeof(MappingType), src, cur_page->GetSize() * sizeof(MappingType));
            char *b_src = reinterpret_cast<char *>(bro_page) + LEAF_PAGE_HEADER_SIZE +
                          (bro_page->GetSize() - 1) * sizeof(MappingType);
            memmove(src, b_src, sizeof(MappingType));
            bro_page->IncreaseSize(-1);
            cur_page->IncreaseSize(1);
            break;
          }
        }
        if ((i + 1) != par_page->GetSize()) {
          WritePageGuard guard = bpm_->FetchPageWrite(par_page->ValueAt(i + 1));
          auto bro_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
          if (bro_page->GetSize() > bro_page->GetMinSize()) {
            par_page->SetKeyAt(i + 1, bro_page->KeyAt(1));
            char *src =
                reinterpret_cast<char *>(cur_page) + LEAF_PAGE_HEADER_SIZE + cur_page->GetSize() * sizeof(MappingType);
            char *b_src = reinterpret_cast<char *>(bro_page) + LEAF_PAGE_HEADER_SIZE;
            memmove(src, b_src, sizeof(MappingType));
            memmove(b_src, b_src + sizeof(MappingType), (bro_page->GetSize() - 1) * sizeof(MappingType));
            bro_page->IncreaseSize(-1);
            cur_page->IncreaseSize(1);
            break;
          }
        }
        // Merge
        page_id_t front_page_id;
        page_id_t back_page_id;
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *front_page;
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *back_page;
        size_t pair_size = sizeof(std::pair<KeyType, page_id_t>);
        if (i != 0) {
          front_page_id = par_page->ValueAt(i - 1);
          back_page_id = cur_page_id;
          back_page = cur_page;
          front_page =
              bpm_->FetchPageWrite(front_page_id).AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
          char *dst = reinterpret_cast<char *>(par_page) + INTERNAL_PAGE_HEADER_SIZE + i * pair_size;
          memmove(dst, dst + pair_size, (par_page->GetSize() - i - 1) * pair_size);
          par_page->IncreaseSize(-1);
        } else {
          front_page_id = cur_page_id;
          back_page_id = par_page->ValueAt(1);
          front_page = cur_page;
          back_page = bpm_->FetchPageWrite(back_page_id).AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
          char *dst = reinterpret_cast<char *>(par_page) + INTERNAL_PAGE_HEADER_SIZE;
          memmove(dst, dst + pair_size, (par_page->GetSize() - 1) * pair_size);
          par_page->SetValueAt(0, front_page_id);
          par_page->IncreaseSize(-1);
        }
        char *dst =
            reinterpret_cast<char *>(front_page) + LEAF_PAGE_HEADER_SIZE + front_page->GetSize() * sizeof(MappingType);
        char *src = reinterpret_cast<char *>(back_page) + LEAF_PAGE_HEADER_SIZE;
        memmove(dst, src, back_page->GetSize() * sizeof(MappingType));
        front_page->IncreaseSize(back_page->GetSize());
        front_page->SetNextPageId(back_page->GetNextPageId());
        if (back_page_id == cur_page_id) {
          guard.Drop();
        }
        bpm_->UnpinPage(back_page_id, true);
        BUSTUB_ASSERT(bpm_->DeletePage(back_page_id), "Page cannot be deleted.");
      } else {
        auto cur_page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        // Distribute
        if (i != 0) {
          WritePageGuard guard = bpm_->FetchPageWrite(par_page->ValueAt(i - 1));
          auto bro_page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          if (bro_page->GetSize() > bro_page->GetMinSize()) {
            char *src = reinterpret_cast<char *>(cur_page) + INTERNAL_PAGE_HEADER_SIZE;
            memmove(src + sizeof(std::pair<KeyType, page_id_t>), src,
                    cur_page->GetSize() * sizeof(std::pair<KeyType, page_id_t>));
            cur_page->SetKeyAt(1, par_page->KeyAt(i));
            par_page->SetKeyAt(i, bro_page->KeyAt(bro_page->GetSize() - 1));
            char *b_src = reinterpret_cast<char *>(bro_page) + INTERNAL_PAGE_HEADER_SIZE +
                          (bro_page->GetSize() - 1) * sizeof(std::pair<KeyType, page_id_t>);
            memmove(src, b_src, sizeof(std::pair<KeyType, page_id_t>));
            bro_page->IncreaseSize(-1);
            cur_page->IncreaseSize(1);
            break;
          }
        }
        if ((i + 1) != par_page->GetSize()) {
          WritePageGuard guard = bpm_->FetchPageWrite(par_page->ValueAt(i + 1));
          auto bro_page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          if (bro_page->GetSize() > bro_page->GetMinSize()) {
            char *src = reinterpret_cast<char *>(cur_page) + INTERNAL_PAGE_HEADER_SIZE +
                        cur_page->GetSize() * sizeof(std::pair<KeyType, page_id_t>);
            char *b_src = reinterpret_cast<char *>(bro_page) + INTERNAL_PAGE_HEADER_SIZE;
            memmove(src, b_src, sizeof(std::pair<KeyType, page_id_t>));
            cur_page->IncreaseSize(1);
            cur_page->SetKeyAt(cur_page->GetSize() - 1, par_page->KeyAt(i + 1));
            par_page->SetKeyAt(i + 1, bro_page->KeyAt(1));
            memmove(b_src, b_src + sizeof(std::pair<KeyType, page_id_t>),
                    (bro_page->GetSize() - 1) * sizeof(std::pair<KeyType, page_id_t>));
            bro_page->IncreaseSize(-1);
            break;
          }
        }
        // Merge
        page_id_t front_page_id;
        page_id_t back_page_id;
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *front_page;
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *back_page;
        if (i != 0) {
          front_page_id = par_page->ValueAt(i - 1);
          back_page_id = cur_page_id;
          back_page = cur_page;
          front_page =
              bpm_->FetchPageWrite(front_page_id).AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          back_page->SetKeyAt(0, par_page->KeyAt(i));
          char *dst = reinterpret_cast<char *>(par_page) + INTERNAL_PAGE_HEADER_SIZE +
                      i * sizeof(std::pair<KeyType, page_id_t>);
          memmove(dst, dst + sizeof(std::pair<KeyType, page_id_t>),
                  (par_page->GetSize() - i - 1) * sizeof(std::pair<KeyType, page_id_t>));
          par_page->IncreaseSize(-1);
        } else {
          front_page_id = cur_page_id;
          back_page_id = par_page->ValueAt(1);
          front_page = cur_page;
          back_page =
              bpm_->FetchPageWrite(back_page_id).AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
          back_page->SetKeyAt(0, par_page->KeyAt(1));
          char *dst = reinterpret_cast<char *>(par_page) + INTERNAL_PAGE_HEADER_SIZE;
          memmove(dst, dst + sizeof(std::pair<KeyType, page_id_t>),
                  (par_page->GetSize() - 1) * sizeof(std::pair<KeyType, page_id_t>));
          par_page->SetValueAt(0, front_page_id);
          par_page->IncreaseSize(-1);
        }
        char *dst = reinterpret_cast<char *>(front_page) + INTERNAL_PAGE_HEADER_SIZE +
                    front_page->GetSize() * sizeof(std::pair<KeyType, page_id_t>);
        char *src = reinterpret_cast<char *>(back_page) + INTERNAL_PAGE_HEADER_SIZE;
        memmove(dst, src, back_page->GetSize() * sizeof(std::pair<KeyType, page_id_t>));
        front_page->IncreaseSize(back_page->GetSize());
        if (back_page_id == cur_page_id) {
          guard.Drop();
        }
        bpm_->UnpinPage(back_page_id, true);
        BUSTUB_ASSERT(bpm_->DeletePage(back_page_id), "Page cannot be deleted.");
      }
    } else {
      if (!page->IsLeafPage() && page->GetSize() == 1) {
        header_page->root_page_id_ = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>()->ValueAt(0);
        guard.Drop();
        bpm_->UnpinPage(cur_page_id, true);
        BUSTUB_ASSERT(bpm_->DeletePage(cur_page_id), "Page cannot be deleted.");
      }
    }
    idx++;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // find the leftmost leaf page
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t cur_page_id = header_page->root_page_id_;
  ReadPageGuard root_guard = bpm_->FetchPageRead(cur_page_id);
  guard = std::move(root_guard);
  while (cur_page_id != INVALID_PAGE_ID) {
    // Fetch page
    if (guard.IsEmpty()) {
      return INDEXITERATOR_TYPE();
    }
    auto cur_page = guard.As<BPlusTreePage>();
    if (cur_page->IsLeafPage()) {
      break;
    }
    auto page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    cur_page_id = page->ValueAt(0);
    // Latch coupling
    if (cur_page_id != INVALID_PAGE_ID) {
      ReadPageGuard child_guard = bpm_->FetchPageRead(cur_page_id);
      guard = std::move(child_guard);
    }
  }

  // construct index iterator
  return INDEXITERATOR_TYPE(cur_page_id, 0, bpm_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  page_id_t cur_page_id = header_page->root_page_id_;
  ReadPageGuard root_guard = bpm_->FetchPageRead(cur_page_id);
  guard = std::move(root_guard);
  int idx = -1;
  while (cur_page_id != INVALID_PAGE_ID) {
    // Fetch page
    if (guard.IsEmpty()) {
      return INDEXITERATOR_TYPE();
    }
    auto cur_page = guard.As<BPlusTreePage>();
    if (cur_page->IsLeafPage()) {
      auto leaf_page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
      int pos = LeafBinarySearch(leaf_page, key);
      if (pos < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(pos), key) == 0) {
        idx = pos;
      }
      break;
    }
    auto page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int pos = InternalBinarySearch(page, key);
    cur_page_id = page->ValueAt(pos);
    // Latch coupling
    if (cur_page_id != INVALID_PAGE_ID) {
      ReadPageGuard child_guard = bpm_->FetchPageRead(cur_page_id);
      guard = std::move(child_guard);
    }
  }
  if (idx == -1 || cur_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  return INDEXITERATOR_TYPE(cur_page_id, idx, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
