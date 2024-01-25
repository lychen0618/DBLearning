//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::GetFreeFrame() -> frame_id_t {
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    BUSTUB_ENSURE(replacer_->Evict(&frame_id), "Evict page should succeed");
    if (pages_[frame_id].IsDirty()) {
      BUSTUB_ENSURE(FlushPage(pages_[frame_id].GetPageId()), "Flush page should succeed");
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }
  return frame_id;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  // Check if free frame exists
  if (free_list_.empty() && replacer_->Size() == 0) {
    *page_id = INVALID_PAGE_ID;
    latch_.unlock();
    return nullptr;
  }
  // New page
  frame_id_t frame_id = GetFreeFrame();
  pages_[frame_id].ResetMemory();
  page_id_t new_page_id = AllocatePage();
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  page_table_[new_page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  *page_id = new_page_id;
  latch_.unlock();
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  // Check if page needs to be fetched from disk
  if (page_table_.find(page_id) == page_table_.end() && free_list_.empty() && replacer_->Size() == 0) {
    latch_.unlock();
    return nullptr;
  }
  // Fetch page
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_[page_id];
  } else {
    frame_id = GetFreeFrame();
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
    page_table_[page_id] = frame_id;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  }
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  latch_.unlock();
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  // Check
  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || pages_[it->second].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  // Unpin page
  pages_[it->second].pin_count_--;
  if (is_dirty) {
    pages_[it->second].is_dirty_ = is_dirty;
  }
  if (pages_[it->second].pin_count_ == 0) {
    replacer_->SetEvictable(it->second, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // Check
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // Flush page
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto &it : page_table_) {
    if (pages_[it.second].is_dirty_) {
      FlushPage(it.first);
    }
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  // Check if can delete
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  // Delete page
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  free_list_.emplace_back(frame_id);
  latch_.unlock();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id, AccessType::Scan);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id, AccessType::Get);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
