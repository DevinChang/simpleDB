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
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    Page *p = nullptr;
    frame_id_t frameId;
    if (page_table_.count(page_id)) {
      frameId = page_table_[page_id];
      p = pages_ + frameId;
      replacer_->Pin(frameId);
      p->pin_count_++;
      return p;
    }
    if (free_list_.empty()) {
      frameId = free_list_.back();
      free_list_.pop_back();
    } else {
      auto isExit = replacer_->Victim(&frameId);
      if (!isExit) return nullptr;
    }
    p = pages_ + frameId;
    //        Note that pages are always found from the free list first.
    // 2.     If R is dirty, write it back to the disk.
    if (p->IsDirty()) {
      disk_manager_->WritePage(p->GetPageId(), p->GetData());
      p->is_dirty_ = false;
    }
    p->pin_count_ = 0;
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(p->GetPageId());
    page_table_[page_id] = frameId;
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    p->page_id_ = page_id;
    p->ResetMemory();
    disk_manager_->ReadPage(page_id, p->GetData());
    p->pin_count_++;
    return p;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { return false; }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

}  // namespace bustub
