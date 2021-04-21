//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capcity_ = num_pages;
  size_ = 0;
}

LRUReplacer::~LRUReplacer() = default;



bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(m_lock);
  if(size_ == 0) return false;
  auto p = frame_list_.back();
  frame_list_.pop_back();
  frame_hash_.erase(p.first);
  *frame_id = p.first;
  size_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(m_lock);
  if(frame_hash_.count(frame_id)) {
    auto p_it = frame_hash_[frame_id];
    frame_list_.erase(p_it);
    frame_hash_.erase(frame_id);
    size_--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(m_lock);
  if(frame_hash_.count(frame_id))  {
    // 与get不同的是，这里找到frame_id后不做操作
    return;
  }
  else if(size_ == capcity_) {
      auto p = frame_list_.back();
      frame_list_.pop_back();
      frame_hash_.erase(p.first);
  }
  else {
    size_++;
  }
  frame_list_.push_front({frame_id, frame_id});
  frame_hash_[frame_id] = frame_list_.begin();
}

size_t LRUReplacer::Size() {
  return size_;
}
}  // namespace bustub
