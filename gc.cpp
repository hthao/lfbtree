#include "gc.h"

namespace ebr {

void gc(int64_t gc_id, NodeFreeFunc free_func)
{
    int64_t min_epoch = GCManager::instance().safe_gc_epoch();
    std::list<GarbageNode> &node_list 
            = GCManager::instance().gc_meta(gc_id).node_list_;

    for (auto i = node_list.begin();  i != node_list.end(); ) {
        if (i->deleted_epoch_ < min_epoch) {
            // release data node here.
            free_func(i->data_node_);
            i = node_list.erase(i);
        }
        else {
            ++i;
        }
    }
}

}