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
  capcity = num_pages;
  size = 0;
  L = new Node(-1);
  R = new Node(-1);
  L->right = R;
  R->left = L;
}

LRUReplacer::~LRUReplacer() = default;

void LRUReplacer::remove(LRUReplacer::Node *p) {
  p->right->left = p->left;
  p->left->right = p->right;
  size--;
}
void LRUReplacer::insert(LRUReplacer::Node *p) {
  p->right = L->right;
  p->left = L;
  L->right->left = p;
  L->right = p;
  size++;
}


bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(m_lock);
  if(Size() == 0) return false;
  auto p = R->left;
  remove(p);
  hash.erase(p->key);
  *frame_id = p->key;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(m_lock);
  if(hash.count(frame_id)) {
    auto p = hash[frame_id];
    remove(p);
    hash.erase(p->key);
    delete p;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(m_lock);
  if(hash.count(frame_id))  {
    // 与get不同的是，这里找到frame_id后不做操作
  } else {
    if(hash.size() == capcity) {
      auto p = R->left;
      remove(p);
      hash.erase(p->key);
      delete p;
    }
    auto p = new Node(frame_id);
    hash[frame_id] = p;
    insert(p);
  }
}

size_t LRUReplacer::Size() {
  return size;
}
}  // namespace bustub
