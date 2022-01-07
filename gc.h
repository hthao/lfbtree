// Implementation of Epoch based reclaimation.

#pragma once

#include <stdlib.h>
#include <assert.h>
#include <atomic>
#include <vector>
#include <list>
#include <algorithm>
#include <functional>
#include <array>
#include "node.h"

namespace ebr {

// The garbage node that record the removed physical node and the remove epoch.
struct GarbageNode {

    // The epoch that the physical B-Tree node was removed.
    int64_t deleted_epoch_;

    // The physical B-Tree Node pointer.
    olcbtree::OlcNode* data_node_;

    GarbageNode(int64_t delete_epoch, olcbtree::OlcNode* data_node):
        deleted_epoch_(delete_epoch),
        data_node_(data_node)
    {
    }

    GarbageNode():
        deleted_epoch_(-1),
        data_node_(nullptr)
    {
    }
};

// Global epoch 
struct GlobalEpoch {

    std::atomic<int64_t> epoch_;

    GlobalEpoch():
        epoch_{-1}
    {
    }

    void increase() {
        epoch_++;
    }

};


// GC Meta data for performing reclaimation per thread basis.
struct ThreadGCMeta {
    // Last active epoch of this thread.
    int64_t last_epoch_;

    // The reclaimation is done in the local thread, so it's
    // thread safe naturally.
    std::list<GarbageNode> node_list_;

    ThreadGCMeta():
        last_epoch_(-1),
        node_list_{}
    {
    }
};

// @Singleton
// Global GC meta data manager for all threads.
// TODO, align with cacheline.
class GCManager {

public:
    ~GCManager() {}

    // No copy
    GCManager(const GCManager&) = delete;
    GCManager& operator=(const GCManager&) = delete;

    // No move
    GCManager(const GCManager&&) = delete;
    GCManager&& operator=(const GCManager&&) = delete;

    // @ThreadSafe
    // Singleton instance.
    static GCManager& instance() {
        static GCManager s_instance;
        return s_instance;
    }

    // Max number of threads to be managed.
    // must be called at the start.
    void set_max_threads_num(uint64_t thread_num) {
        meta_.resize(thread_num);
    }

    void set_gc_interval_ms(uint64_t gc_interval_ms) {

    }

    void start_gc_thread() {

    }

    // Get thread's GC meta data by gc_id.
    ThreadGCMeta& gc_meta(uint64_t gc_id) {
        assert(gc_id <= meta_.size());
        return meta_[gc_id - 1];
    }

    // @ThreadSafe
    // Register thread to GCManager and return the gc_id.
    uint64_t register_thread() {
        uint64_t old_thread_num = total_threads_num_.fetch_add(1);
        uint64_t gc_id = old_thread_num + 1;
        if (gc_id > meta_.size()) {
            // total thread num has exceeded the max limitation.
            // abort here.
            abort();
        }
        return gc_id;
    }

    // @ThreadSafe
    // Get the min epoch that is safe to perform GC
    int64_t safe_gc_epoch() {
        assert(meta_.size() > 0);
        int64_t min_epoch = meta_[0].last_epoch_;

        for (size_t i = 1; i < meta_.size(); i++) {
            // Note: to be thread safe, we need to get a copy of thread's 
            // last_epoch_ to perform the compare, otherwise the epoch
            // value may happening be updated before returning back while 
            // after the compare finished, so a larger value may be returned
            // back which is not expected. 
            int64_t epoch = meta_[i].last_epoch_;
            min_epoch = std::min(min_epoch, epoch);
        }

        return min_epoch;
    }

private:
    GCManager():
        total_threads_num_{0}
    {
    }

private:
    std::vector<ThreadGCMeta> meta_;
    std::atomic<uint64_t> total_threads_num_;
};

typedef std::function<void(olcbtree::OlcNode*)> NodeFreeFunc;

void gc(uint64_t gc_id, NodeFreeFunc free_func); 

}
