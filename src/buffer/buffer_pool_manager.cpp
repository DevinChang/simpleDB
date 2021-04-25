//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer
  std::lock_guard<std::mutex> lock(latch_);
  Page *p = nullptr;
  frame_id_t frame_id = -1;
  if (page_table_.count(page_id)) {
    frame_id = page_table_[page_id];
    p = &pages_[frame_id];
    replacer_->Pin(frame_id);
    p->pin_count_++;
    return p;
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    auto isExit = replacer_->Victim(&frame_id);
    if (!isExit) return nullptr;
    page_table_.erase(pages_[frame_id].page_id_);
  } else {
    return nullptr;
  }
  p = &pages_[frame_id];
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  if (p->IsDirty()) {
    disk_manager_->WritePage(p->GetPageId(), p->GetData());
    p->is_dirty_ = false;
  }
  p->pin_count_ = 0;
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(p->GetPageId());
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  p->page_id_ = page_id;
  p->ResetMemory();
  p->pin_count_++;
  disk_manager_->ReadPage(page_id, p->GetData());
  return p;
}

/**
 * Unpin the target page from the buffer pool.
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page pin count is <= 0 before this call, true otherwise
 */
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  if (!page_table_.count(page_id)) return false;
  auto fid = page_table_[page_id];
  Page *page = &pages_[fid];
  if (page->pin_count_ <= 0) return false;
  if (is_dirty) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
  page->pin_count_--;
  if (page->pin_count_ <= 0) {
    replacer_->Unpin(fid);
  }
  page->is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> lock(latch_);
  if (!page_table_.count(page_id)) return false;
  auto fid = page_table_[page_id];
  auto page = &pages_[fid];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  std::lock_guard<std::mutex> lock(latch_);
  Page *p = nullptr;
  frame_id_t frame_id = -1;
  *page_id = disk_manager_->AllocatePage();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (free_list_.size()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    auto isExit = replacer_->Victim(&frame_id);
    if (!isExit) return nullptr;
    page_table_.erase(pages_[frame_id].page_id_);
  } else {
    return nullptr;
  }
  p = &pages_[frame_id];
  page_table_.erase(p->GetPageId());
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  p->page_id_ = *page_id;
  p->pin_count_++;
  p->is_dirty_ = true;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_table_[*page_id] = frame_id;
  return p;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  if (!page_table_.count(page_id)) return true;
  auto fid = page_table_[page_id];
  Page *page = &pages_[fid];
  if (page->GetPinCount() != 0) return false;
  page_table_.erase(fid);
  page->ResetMemory();
  free_list_.push_back(fid);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  for (auto [pid, fid] : page_table_) {
    auto page = &pages_[fid];
    if (page->is_dirty_) continue;
    disk_manager_->WritePage(pid, page->GetData());
  }
  page_table_.clear();
}

}  // namespace bustub
