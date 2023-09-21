//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/lru_k_replacer.h"
#include <fmt/format.h>
#include "common/exception.h"

namespace bustub {
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_size_ == 0) {
    latch_.unlock();
    return false;
  }
  std::unordered_map<frame_id_t, LRUKNode>::iterator evict = node_store_.end();
  for (auto cur = node_store_.begin(); cur != node_store_.end(); ++cur) {
    if (!cur->second.GetEvictable()) {
      continue;
    }
    if (evict == node_store_.end()) {
      evict = cur;
      continue;
    }
    if (evict->second.GetBackwardKDis() == cur->second.GetBackwardKDis()) {
      if (evict->second.GetLeastRecent() > cur->second.GetLeastRecent()) {
        evict = cur;
      }
    } else if (evict->second.GetBackwardKDis() > cur->second.GetBackwardKDis()) {
      evict = cur;
    }
  }
  *frame_id = evict->first;
  latch_.unlock();
  Remove(*frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT((size_t)frame_id < replacer_size_, fmt::format("frame id {} is invalid", frame_id).c_str());
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(k_, frame_id);
    node_store_[frame_id].Access(++current_timestamp_);
  } else {
    it->second.Access(++current_timestamp_);
  }
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    bool diff = (it->second.GetEvictable() != set_evictable);
    it->second.SetEvictable(set_evictable);
    if (diff) {
      if (set_evictable) {
        ++curr_size_;
      } else {
        --curr_size_;
      }
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end() && it->second.GetEvictable()) {
    --curr_size_;
  }
  node_store_.erase(frame_id);
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
