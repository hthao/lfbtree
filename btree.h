#pragma once

#include <stdio.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <atomic>
#include <sched.h>
#include <queue>
#include <thread>
#include <assert.h>
#include <stdint.h>
#include <immintrin.h>
#include <cstring>
#include "gc.h"

namespace olcbtree {

template <typename KeyType,
          typename ValueType,
          typename KeyLessComparator = std::less<KeyType>,
          typename KeyEqualComarator = std::equal_to<KeyType>>
class Tree {
public:
    Tree() {
        root_ = new LeafNode(&comparator_);
    }

    ~Tree() {
        //TODO, free the tree.
    }

    // No copy
    Tree(const Tree&) = delete;
    Tree& operator=(const Tree&) = delete;

    // No move
    Tree(const Tree&&) = delete;
    Tree&& operator=(const Tree&&) = delete;

    struct Comparator {
        KeyLessComparator less_;
        KeyEqualComarator equal_;
    };

    // Inner node of B+Tree.
    class InnerNode : public OlcNode {
    public:
        struct Entry {
            KeyType key;
            OlcNode* child;
        };

        InnerNode(Comparator* cmp):comparator_(cmp)
        {
        }

        virtual ~InnerNode() {}

        // Return the TreeNode's type.
        OlcNode::Type type() final { return OlcNode::Type::EnumInner; }

        // Return the first Entry's position which the Entry's key is
        // bigger than `k` or equal with `k`. Because the count of key
        // is 1 less than the count of children for InnerNode, if there
        // is only 1 Entry in the node, the key is indicating bigger
        // than all other Entries, and return this Entry's position: 0.
        uint32_t lower_bound(KeyType k) {
            uint32_t lower = 0;
            // For InnerNode, count of keys is 1 less than count of children.
            uint32_t upper = this->count_ - 1;
            while(lower < upper) {
                uint32_t mid = ((upper - lower) / 2) + lower;
                //if (k < entries_[mid].key) {
                if (comparator_->less_(k, entries_[mid].key)) {
                    upper = mid;
                }
                else if (comparator_->equal_(k, entries_[mid].key)){
                    return mid;
                }
                else {
                    lower = mid + 1;
                }
            };

            return lower;
        }

        // Insert child into InnerNode because of splitting.
        // Lazy splitting.
        void insert(KeyType k, OlcNode* child) {
            assert(this->count_ < BtreeMaxDegree);

            unsigned int pos = lower_bound(k);
            std::memmove(entries_ + pos + 1, entries_ + pos,
                         sizeof(Entry) * (this->count_ - pos));

            entries_[pos].key = k;
            entries_[pos].child = child;

            // The new insert child's parent is the key of `pos + 1`,
            // and the old child's parent is the new inserted key.
            std::swap(entries_[pos].child, entries_[pos + 1].child);
            this->count_++;
        }


        // InnerNode split.
        // return the new splitted node.
        InnerNode* split(KeyType& seperate_key) {
            // create new node.
            InnerNode *new_node = new InnerNode(comparator_);

            new_node->count_ = this->count_/2;

            // update old node.
            this->count_ = this->count_ - new_node->count_;

            // seperate key.
            seperate_key = entries_[this->count_ - 1].key;
            std::memcpy(new_node->entries_, entries_ + this->count_,
                        sizeof(Entry) * new_node->count_);

            return new_node;
        }

        void remove_at(uint32_t pos) {
            assert(pos < this->count_);
            std::memmove(entries_ + pos, entries_ + pos + 1,
                         sizeof(Entry) * (this->count_ - pos - 1));
            this->count_--;
        }

        void borrow_from_left(InnerNode* parent, uint32_t pos_in_parent, InnerNode* left) {
            assert(this->count_ < BtreeMaxDegree);
            // Right shift 1 entry.
            std::memmove(entries_ + 1, entries_,
                         sizeof(Entry) * this->count_);
            // Copy 1 entry from left.
            std::memcpy(entries_, left->entries_ + left->count_ - 1,
                         sizeof(Entry) * 1);
            this->count_++;
            // Transfer parent key.
            entries_[0].key = parent->entries_[pos_in_parent].key;

            // Update left.
            left->count_--;

            // Update parent.
            parent->entries_[pos_in_parent].key = left->entries_[left->count_ - 1].key;
        }

        void borrow_from_right(InnerNode* parent, uint32_t pos_in_parent, InnerNode* right) {
            assert(this->count_ < BtreeMaxDegree);
            // Copy 1 entry from right.
            std::memcpy(entries_ + this->count_, right->entries_,
                         sizeof(Entry) * 1);
            // Transfer parent key.
            entries_[this->count_ - 1].key = parent->entries_[pos_in_parent].key;
            this->count_++;

            // Update parent's key.
            parent->entries_[pos_in_parent].key = right->entries_[0].key;
            // Left shift 1 entry.
            std::memmove(right->entries_, right->entries_ + 1,
                         sizeof(Entry) * (right->count_ - 1));
            right->count_--;
        }

        void merge_into_left(InnerNode* parent, uint32_t pos_in_parent, InnerNode* left) {
            // Transfer parent key.
            left->entries_[left->count_ - 1].key = parent->entries_[pos_in_parent - 1].key;
            std::memcpy(left->entries_ + left->count_, entries_,
                        sizeof(Entry) * this->count_);
            left->count_ += this->count_;

            // Parent shrink 1 entry.
            // NOTICE!!! pos_in_parent - 1;
            parent->entries_[pos_in_parent].child = left;
            parent->remove_at(pos_in_parent - 1);

            // Node is deleted, should be put into GC list.
            this->count_ = 0;
        }

        void merge_with_right(InnerNode* parent, uint32_t pos_in_parent, InnerNode* right) {
            // Transfer parent key.
            entries_[this->count_ - 1].key = parent->entries_[pos_in_parent].key;
            // Copy right node.
            std::memcpy(entries_ + this->count_, right->entries_,
                        sizeof(Entry) * right->count_);

            this->count_ += right->count_;
            // Parent shrink 1 entry.
            parent->entries_[pos_in_parent + 1].child = this;
            parent->remove_at(pos_in_parent);

            // Right Node is deleted, should be put into GC list.
            right->count_ = 0;
        }

        // Entries.
        // reserve 1 entry for max.
        Entry entries_[BtreeMaxDegree];
        Comparator* comparator_;
    };

    // Leaf node of B+Tree.
    class LeafNode : public OlcNode {
    public:
        struct Entry {
            KeyType key;
            ValueType val;
        };

        LeafNode(Comparator* cmp):
            left_(nullptr),
            right_(nullptr),
            comparator_(cmp)
        {
        }

        virtual ~LeafNode() {}

        // Return the TreeNode's type.
        OlcNode::Type type() final { return OlcNode::Type::EnumLeaf; }

        // Return the first Entry's position which the Entry key
        // is bigger than or equal with `k`.
        uint32_t lower_bound(KeyType k) {
            uint32_t lower = 0;
            uint32_t upper = this->count_;
            do {
                uint32_t mid = ((upper - lower) / 2) + lower;
                // if (k < entries_[mid].key) {
                if (comparator_->less_(k, entries_[mid].key)) {
                    upper = mid;
                }
                else if (comparator_->equal_(k, entries_[mid].key)){
                    return mid;
                }
                else {
                    lower = mid + 1;
                }
            } while (lower < upper);

            return lower;
        }

        // @DiffWithInner
        // Insert child into LeafNode.
        // Note: be careful about the change of max entry.
        void insert(KeyType k, ValueType val) {
            assert(this->count_ < BtreeMaxDegree);

            if(this->count_ == 0) {
                entries_[0].key = k;
                entries_[0].val = val;
                this->count_++;
                return;
            }

            unsigned int pos = lower_bound(k);

            // if (pos < this->count_ && entries_[pos].key == k) {
            if (pos < this->count_ && comparator_->equal_(k, entries_[pos].key)) {
                // Upsert.
                // TODO, MVCC.
                entries_[pos].val = val;
                return;
            }

            std::memmove(entries_ + pos + 1, entries_ + pos,
                     sizeof(Entry) * (this->count_ - pos));

            entries_[pos].key = k;
            entries_[pos].val = val;
            this->count_++;
        }


        // LeafNode split.
        // return the new splitted node.
        // the seperate_key is the max entry of origin
        LeafNode* split(KeyType& seperate_key) {
            // create new node.
            LeafNode *new_node = new LeafNode(comparator_);

            new_node->count_ = this->count_ / 2;

            // update old node.
            this->count_ = this->count_ - new_node->count_;

            // seperate key.
            seperate_key = entries_[this->count_ - 1].key;
            std::memcpy(new_node->entries_, entries_ + this->count_,
                        sizeof(Entry) * new_node->count_);
            // Link
            if (right_) {
                new_node->right_ = right_;
                right_->left_ = new_node;
            }
            right_ = new_node;
            new_node->left_ = this;

            return new_node;
        }

        void remove(KeyType k) {
            unsigned int pos = lower_bound(k);

            // Not found.
            if (pos >= this->count_ || !comparator_->equal_(k, entries_[pos].key)) {
                return;
            }

            std::memmove(entries_ + pos, entries_ + pos + 1,
                         sizeof(Entry) * (this->count_ - pos - 1));

            this->count_--;
        }

        void remove_at(uint32_t pos) {
            assert(pos < this->count_);
            std::memmove(entries_ + pos, entries_ + pos + 1,
                         sizeof(Entry) * (this->count_ - pos - 1));
            this->count_--;
        }

        void borrow_from_left(
                InnerNode* parent,
                uint32_t pos_in_parent,
                LeafNode* left)
        {
            assert(this->count_ < BtreeMaxDegree);
            // Right shift 1 entry.
            std::memmove(entries_ + 1, entries_,
                         sizeof(Entry) * this->count_);
            // Copy 1 entry from left.
            std::memcpy(entries_, left->entries_ + left->count_ - 1,
                         sizeof(Entry) * 1);
            // Transfer parent key.
            // entries_[0].key = left->entries_[left->count_ - 1].key;
            this->count_++;

            left->count_--;
            // Update parent's key.
            parent->entries_[pos_in_parent].key = left->entries_[left->count_ - 1].key;
        }

        void borrow_from_right(
                InnerNode* parent,
                uint32_t pos_in_parent,
                LeafNode* right)
        {
            // Copy 1 entry from right.
            std::memcpy(entries_ + this->count_, right->entries_,
                         sizeof(Entry) * 1);
            // Transfer parent key.
            // entries_[count_].key = parent->entries_[pos_in_parent].key;
            this->count_++;

            // Update parent's key.
            parent->entries_[pos_in_parent].key = entries_[this->count_ - 1].key;
            // Left shift 1 entry.
            std::memmove(right->entries_, right->entries_ + 1,
                         sizeof(Entry) * (right->count_ - 1));
            right->count_--;
        }

        void merge_into_left(
                InnerNode* parent,
                uint32_t pos_in_parent,
                LeafNode* left)
        {
            // Transfer parent key.
            std::memcpy(left->entries_ + left->count_, entries_,
                        sizeof(Entry) * this->count_);
            left->count_ += this->count_;

            this->count_ = 0;

            // Update parent's key.
            // parent->entries_[pos_in_parent].key = entries_[count_ - 1].key;
            // NOTICE !!!
            parent->entries_[pos_in_parent].child = left;
            parent->remove_at(pos_in_parent - 1);


            // Re-link.
            left->right_ = right_;
            if (right_) {
                right_->left_ = left;
            }

            // TODO, Node is deleted, should be put into GC list.
        }

        void merge_with_right(
                InnerNode* parent,
                uint32_t pos_in_parent,
                LeafNode* right) {
            // Copy right node.
            std::memcpy(entries_ + this->count_, right->entries_,
                        sizeof(Entry) * (right->count_));

            this->count_ += right->count_;

            // Update parent's key.
            // parent->entries_[pos_in_parent].key = entries_[count_ - 1].key;
            parent->entries_[pos_in_parent + 1].child = this;
            parent->remove_at(pos_in_parent);


            right->count_ = 0;

            right_ = right->right_;
            if (right->right_) {
                right->right_->left_ = this;
            }

            // TODO, Right Node is deleted, should be put into GC list.
        }

        Entry entries_[BtreeMaxDegree];
        LeafNode* left_;
        LeafNode* right_;
        Comparator* comparator_;
    };

    void insert(KeyType key, ValueType val) {
        int restart_count = 0;
RESTART:
        if (restart_count++) {
            yield(restart_count);
        }
        bool need_restart = false;

        // Current node
        OlcNode* node = root_;
        uint64_t node_version = node->readlock_or_restart(need_restart);
        if (need_restart || (node!=root_)) goto RESTART;

        // Parent of current node
        InnerNode* parent = nullptr;
        uint64_t parent_version;

        while (node->type() == OlcNode::Type::EnumInner) {
            auto inner = static_cast<InnerNode*>(node);

            // Optimized lock coupling
            //
            if (inner->is_full()) {
                // Lock
                if (parent) {
                    parent->upgrade_to_writelock_or_restart(parent_version, need_restart);
                    if (need_restart) goto RESTART;
                }
                node->upgrade_to_writelock_or_restart(node_version, need_restart);
                if (need_restart) {
                    if (parent) parent->write_unlock();
                    goto RESTART;
                }

                // Another thread may changed the root at this time.
                // ----------------------------------------------------------------------------------------
                //          Thread-0            |           Thread-1              |          Thread-2
                // ----------------------------------------------------------------------------------------
                // Start the insert and get     |  Start the insert and get       |
                // the pointer of `root_`,      |  the pointer of `root_`,        |
                // we named it as node_t0 e.g.  |  we name it as node_t1          |
                // ----------------------------------------------------------------------------------------
                //                              |  Finished the insert operation, |
                //                              |  and there is new `root_` now.  |
                // ----------------------------------------------------------------------------------------
                //                              |                                 |  Finished another insert
                //                              |                                 |  which make the node_t0
                //                              |                                 |  full now.
                // ----------------------------------------------------------------------------------------
                // Coupling lock node_t0, now   |                                 |
                // the node_t0 is full, and the |                                 |
                // parent is nullptr but the    |                                 |
                // node_t0 is not the `root_`   |                                 |
                // anymore.                     |                                 |
                // ----------------------------------------------------------------------------------------
                if (!parent && (node != root_)) {
                    node->write_unlock();
                    goto RESTART;
                }

                // Split
                KeyType seperate_key;
                InnerNode* newInner = inner->split(seperate_key);
                if (parent) {
                    parent->insert(seperate_key, newInner);
                }
                else {
                    make_root(seperate_key, inner, newInner);
                }
                // Split complete, we need restart again.
                // Unlock and restart.
                node->write_unlock();
                if (parent) parent->write_unlock();
                goto RESTART;
            }

            if (parent) {
                parent->read_unlock_or_restart(parent_version, need_restart);
                if (need_restart) goto RESTART;
            }

            parent = inner;
            parent_version = node_version;

            node = inner->entries_[inner->lower_bound(key)].child;
            inner->check_or_restart(node_version, need_restart);
            if (need_restart) goto RESTART;
            node_version = node->readlock_or_restart(need_restart);
            if (need_restart) goto RESTART;
        }

        auto leaf = static_cast<LeafNode*>(node);

        // Split leaf if full
        if (leaf->is_full()) {
            // Lock
            if (parent) {
                parent->upgrade_to_writelock_or_restart(parent_version, need_restart);
                if (need_restart) goto RESTART;
            }
            node->upgrade_to_writelock_or_restart(node_version, need_restart);
            if (need_restart) {
                if (parent) parent->write_unlock();
                goto RESTART;
            }

            // Another thread may changed the root.
            if (!parent && (node != root_)) {  // there's a new parent
                node->write_unlock();
                goto RESTART;
            }
            // Split
            KeyType seperate_key;
            LeafNode* newLeaf = leaf->split(seperate_key);
            if (parent) {
                parent->insert(seperate_key, newLeaf);
            }
            else {
                make_root(seperate_key, leaf, newLeaf);
            }

            // Split complete, we need restart again.
            // Unlock and restart.
            node->write_unlock();
            if (parent) parent->write_unlock();
            goto RESTART;
        } else {
            // only lock leaf node
            node->upgrade_to_writelock_or_restart(node_version, need_restart);
            if (need_restart) goto RESTART;
            if (parent) {
                parent->read_unlock_or_restart(parent_version, need_restart);
                if (need_restart) {
                    node->write_unlock();
                    goto RESTART;
                }
            }
            leaf->insert(key, val);
            node->write_unlock();
            return;
        }

    }

    bool lookup(KeyType key, ValueType& value) {
      int restart_count = 0;
RESTART:
      if (restart_count++) {
        yield(restart_count);
      }
      bool need_restart = false;

      OlcNode* node = root_;
      uint64_t node_version = node->readlock_or_restart(need_restart);
      if (need_restart || (node != root_)) goto RESTART;

      // Parent of current node
      InnerNode* parent = nullptr;
      uint64_t parent_version;

      while (node->type() == OlcNode::Type::EnumInner) {
        auto inner = static_cast<InnerNode*>(node);

        if (parent) {
            parent->read_unlock_or_restart(parent_version, need_restart);
            if (need_restart) goto RESTART;
        }

        parent = inner;
        parent_version = node_version;

        node = inner->entries_[inner->lower_bound(key)].child;
        inner->check_or_restart(node_version, need_restart);
        if (need_restart) goto RESTART;
        node_version = node->readlock_or_restart(need_restart);
        if (need_restart) goto RESTART;
      }

      auto leaf = static_cast<LeafNode*>(node);
      unsigned pos = leaf->lower_bound(key);
      bool success;
      // if ((pos < leaf->count()) && (leaf->entries_[pos].key == key)) {
      if ((pos < leaf->count()) && (comparator_.equal_(key, leaf->entries_[pos].key))) {
        success = true;
        value = leaf->entries_[pos].val;
      }
      if (parent) {
        parent->read_unlock_or_restart(parent_version, need_restart);
        if (need_restart) goto RESTART;
      }
      node->read_unlock_or_restart(node_version, need_restart);
      if (need_restart) goto RESTART;

      return success;
    }

    uint64_t scan(KeyType key, int range, ValueType* output) {
      int restart_count = 0;
RESTART:
      if (restart_count++) {
        yield(restart_count);
      }
      bool need_restart = false;

      OlcNode* node = root_;
      uint64_t node_version = node->readlock_or_restart(need_restart);
      if (need_restart || (node!=root_)) goto RESTART;

      // Parent of current node
      InnerNode* parent = nullptr;
      uint64_t parent_version;

      while (node->type() == OlcNode::Type::EnumInner) {
        auto inner = static_cast<InnerNode*>(node);

        if (parent) {
          parent->read_unlock_or_restart(parent_version, need_restart);
          if (need_restart) goto RESTART;
        }

        parent = inner;
        parent_version = node_version;

        node = inner->entries_[inner->lower_bound(key)].child;
        inner->check_or_restart(node_version, need_restart);
        if (need_restart) goto RESTART;
        node_version = node->readlock_or_restart(need_restart);
        if (need_restart) goto RESTART;
      }

      auto leaf = static_cast<LeafNode*>(node);
      unsigned pos = leaf->lower_bound(key);
      int count = 0;
      for (unsigned i = pos; i < leaf->count(); i++) {
        if (count == range) break;
        output[count++] = leaf->entries_[i].val;
      }

      if (parent) {
        parent->read_unlock_or_restart(parent_version, need_restart);
        if (need_restart) goto RESTART;
      }
      node->read_unlock_or_restart(node_version, need_restart);
      if (need_restart) goto RESTART;

      return count;
    }

    void remove(KeyType key, uint64_t gc_id) {
        int restart_count = 0;
RESTART:
        if (restart_count++) {
            yield(restart_count);
        }
        bool need_restart = false;

        // Current node
        OlcNode* node = root_;
        uint64_t node_version = node->readlock_or_restart(need_restart);
        if (need_restart || (node!=root_)) goto RESTART;

        // Parent of current node
        InnerNode* parent = nullptr;
        uint64_t parent_version;
        OlcNode *left = nullptr;
        uint64_t left_version;
        OlcNode *right = nullptr;
        uint64_t right_version;

        uint32_t entry_pos_in_parent;

        while (node->type() == OlcNode::Type::EnumInner) {
            auto inner = static_cast<InnerNode*>(node);

            do {
                // Optimized lock coupling
                if (inner->is_less_than_half()) {

                    // We don't merge for the root node.
                    if (!parent) {
                        break;
                    }

                    // Lock parent.
                    if (parent) {
                        parent->upgrade_to_writelock_or_restart(parent_version, need_restart);
                        if (need_restart) goto RESTART;
                    }

                    // Lock left node.
                    if (left) {
                        left->upgrade_to_writelock_or_restart(left_version, need_restart);
                        if (need_restart) {
                            if (parent) parent->write_unlock();
                            goto RESTART;
                        }
                    }

                    // Lock right node.
                    if (right) {
                        right->upgrade_to_writelock_or_restart(right_version, need_restart);
                        if (need_restart) {
                            if (parent) parent->write_unlock();
                            if (left) left->write_unlock();
                            goto RESTART;
                        }
                    }

                    // Lock self.
                    node->upgrade_to_writelock_or_restart(node_version, need_restart);
                    if (need_restart) {
                        if (left) left->write_unlock();
                        if (right) right->write_unlock();
                        if (parent) parent->write_unlock();
                        goto RESTART;
                    }

                    if (!parent && (node != root_)) {
                        if (left) left->write_unlock();
                        if (right) right->write_unlock();
                        node->write_unlock();
                        goto RESTART;
                    }

                    // Alway try borrow first.
                    if (left && left->is_more_than_half()) {
                        auto left_inner = static_cast<InnerNode*>(left);
                        // Brrow from left.
                        inner->borrow_from_left(parent, entry_pos_in_parent, left_inner);
                    }
                    else if (right && right->is_more_than_half()) {
                        auto right_inner = static_cast<InnerNode*>(right);
                        // Brrow from right.
                        inner->borrow_from_right(parent, entry_pos_in_parent, right_inner);
                    }
                    // Both left and right are less than half, try merging.
                    else if(left) {
                        auto left_inner = static_cast<InnerNode*>(left);
                        // Merge with left.
                        inner->merge_into_left(parent, entry_pos_in_parent, left_inner);

                        if (parent && parent == root_ && parent->count_ == 1) {
                            root_ = left_inner;
                        }

                        // TODO, put inner into GC list.
                        ebr::GarbageNode gcnode(-1, inner);
                        ebr::GCManager::instance().gc_meta(gc_id).node_list_.emplace_back(gcnode);

                    } else if (right) {
                        auto right_inner = static_cast<InnerNode*>(right);
                        // Merge with right.
                        inner->merge_with_right(parent, entry_pos_in_parent, right_inner);
                        if (parent && parent == root_ && parent->count_ == 1) {
                            root_ = node;
                        }

                        // TODO, put right node into GC list.
                        ebr::GarbageNode gcnode(-1, right_inner);
                        ebr::GCManager::instance().gc_meta(gc_id).node_list_.emplace_back(gcnode);
                    } else {
                        // It is the root node, do nothing.
                    }

                    // Merge complete, unlock and restart
                    node->write_unlock();
                    if (left) left->write_unlock();
                    if (right) right->write_unlock();
                    if (parent) parent->write_unlock();
                    goto RESTART;
                }
            } while(0);

            if (parent) {
                parent->read_unlock_or_restart(parent_version, need_restart);
                if (need_restart) goto RESTART;
            }

            /*
            if (left) {
                left->read_unlock_or_restart(left_version, need_restart);
                if (need_restart) goto RESTART;
            }

            if (right) {
                right->read_unlock_or_restart(right_version, need_restart);
                if (need_restart) goto RESTART;
            }
            */

            // Update parent.
            parent = inner;
            parent_version = node_version;

            entry_pos_in_parent = inner->lower_bound(key);
            node = inner->entries_[entry_pos_in_parent].child;

            // Update left node and right node.
            // Left node.
            if (entry_pos_in_parent == 0) {
                left = nullptr;
            } else {
                left = inner->entries_[entry_pos_in_parent - 1].child;
            }

            // Right node.
            if (entry_pos_in_parent >= inner->count() - 1) {
                right = nullptr;
            } else {
                right = inner->entries_[entry_pos_in_parent + 1].child;
            }


            // Check whether the inner node was changed.
            inner->check_or_restart(node_version, need_restart);
            if (need_restart) goto RESTART;

            // Read lock for next iteration.
            node_version = node->readlock_or_restart(need_restart);
            if (need_restart) goto RESTART;

            if (left) {
                left_version = left->readlock_or_restart(need_restart);
                if (need_restart) goto RESTART;
            }
            if (right) {
                right_version = right->readlock_or_restart(need_restart);
                if (need_restart) goto RESTART;
            }
        }

        auto leaf = static_cast<LeafNode*>(node);

        do {
            // Split leaf if full
            if (leaf->is_less_than_half()) {

                if (!parent) {
                    break;
                }

                // Lock parent.
                if (parent) {
                    parent->upgrade_to_writelock_or_restart(parent_version, need_restart);
                    if (need_restart) goto RESTART;
                }

                // Lock left node.
                if (left) {
                    left->upgrade_to_writelock_or_restart(left_version, need_restart);
                    if (need_restart) {
                        if (parent) parent->write_unlock();
                        goto RESTART;
                    }
                }

                // Lock right node.
                if (right) {
                    right->upgrade_to_writelock_or_restart(right_version, need_restart);
                    if (need_restart) {
                        if (parent) parent->write_unlock();
                        if (left) left->write_unlock();
                        goto RESTART;
                    }
                }

                // Lock self.
                leaf->upgrade_to_writelock_or_restart(node_version, need_restart);
                if (need_restart) {
                    if (left) left->write_unlock();
                    if (right) right->write_unlock();
                    if (parent) parent->write_unlock();
                    goto RESTART;
                }

                if (!parent && (leaf != root_)) {
                    if (left) left->write_unlock();
                    if (right) right->write_unlock();
                    leaf->write_unlock();
                    goto RESTART;
                }

                // Alway try borrow first.
                if (left && left->is_more_than_half()) {
                    auto left_leaf = static_cast<LeafNode*>(left);
                    // Brrow from left.
                    leaf->borrow_from_left(parent, entry_pos_in_parent, left_leaf);
                }
                else if (right && right->is_more_than_half()) {
                    auto right_leaf = static_cast<LeafNode*>(right);
                    // Brrow from right.
                    leaf->borrow_from_right(parent, entry_pos_in_parent, right_leaf);
                }
                // Both left and right are less than half, try merging.
                else if(left) {
                    auto left_leaf = static_cast<LeafNode*>(left);
                    // Merge with left.
                    leaf->merge_into_left(parent, entry_pos_in_parent, left_leaf);
                    if (parent && parent == root_ && parent->count_ == 1) {
                        root_ = left_leaf;
                    }

                    // TODO, put leaf into GC list.
                    ebr::GarbageNode gcnode(-1, leaf);
                    ebr::GCManager::instance().gc_meta(gc_id).node_list_.emplace_back(gcnode);

                } else if (right) {
                    auto right_leaf = static_cast<LeafNode*>(right);
                    // Merge with right.
                    leaf->merge_with_right(parent, entry_pos_in_parent, right_leaf);
                    if (parent && parent == root_ && parent->count_ == 1) {
                        root_ = node;
                    }

                    // TODO, put right node into GC list.
                    ebr::GarbageNode gcnode(-1, right_leaf);
                    ebr::GCManager::instance().gc_meta(gc_id).node_list_.emplace_back(gcnode);
                } else {
                    // It is the root node, do nothing.
                }

                // Merge complete, unlock and restart
                leaf->write_unlock();
                if (left) left->write_unlock();
                if (right) right->write_unlock();
                if (parent) parent->write_unlock();
                goto RESTART;
            }
        } while(0);

        // only lock leaf node
        leaf->upgrade_to_writelock_or_restart(node_version, need_restart);
        if (need_restart) goto RESTART;
        if (parent) {
            parent->read_unlock_or_restart(parent_version, need_restart);
            if (need_restart) {
                leaf->write_unlock();
                goto RESTART;
            }
        }
        leaf->remove(key);
        leaf->write_unlock();
        return;  // success
    }

    void graphviz_print_tree() {
        OlcNode *node = root_;
        if (!root_) return;

        std::stringstream ss;
        ss << "digraph btree { \n" << "node [shape = record,height=.1];\n";
        ss << "rankdir=\"TB\";\n";
        graphviz_visit(node, ss);
        ss << "}";

        std::ofstream dot_file("btree.dot");
        if (!dot_file.is_open()) {
            return;
        }

        dot_file << ss.str();
        dot_file.close();
    }

private:

    void yield(int count) {
        if (count > 3) {
            sched_yield();
        } else {
            _mm_pause();
        }
    }

    // Introduced by split.
    void make_root(KeyType& seperate_key,
                   OlcNode *left_node,
                   OlcNode *right_node
                   )
    {
        InnerNode *inner_node = new InnerNode(&comparator_);
        inner_node->entries_[0].key = seperate_key;
        inner_node->entries_[0].child = left_node;
        inner_node->entries_[1].child = right_node;
        inner_node->count_ = 2;
        root_ = inner_node;
    }

    // Visit btree to print graphviz format text.
    void graphviz_visit(OlcNode *n, std::stringstream& ss) {

        if (n->type() == OlcNode::Type::EnumInner) {
            auto node = static_cast<InnerNode*>(n);
            ss << get_graphviz_node_desc(n);
            for (int i = 0; i < node->count(); i++) {
                ss << get_graphviz_node_desc(node->entries_[i].child);
                ss << "\"node" << node << "\":f" << i << "->\"node"
                    << node->entries_[i].child << "\"\n";
            }

            for (int i = 0; i < n->count(); i++) {
                graphviz_visit(node->entries_[i].child, ss);
            }
        } else {
            auto node = static_cast<LeafNode*>(n);
            ss << get_graphviz_node_desc(node);
            /*
            int node_max = node->count_;
            int right_min = 0;
            if (node->right_) {
                ss << "\"node" << node << "\":f" << node_max << "->\"node"
                    << node->right_ << "\":f" << right_min <<"\n";
            }
            */
        }
    }

    // Get graphviz format description of node.
    std::string get_graphviz_node_desc(OlcNode *n) {
      std::stringstream node_str;
      node_str << "node" << n << "[label=\" ";

      if (n->type() == OlcNode::Type::EnumInner) {
          auto node = static_cast<InnerNode*>(n);
          for (int i = 0; i < node->count() - 1; i++) {
              node_str << "<f" << i << ">"
                 << " |" << node->entries_[i].key << "| ";
          }
          node_str << "<f" << node->count() - 1 << ">\"];\n";
      } else {
          auto node = static_cast<LeafNode*>(n);
          for (int i = 0; i < node->count(); i++) {
              node_str << "<f" << i << ">"
                 << " |" << node->entries_[i].key << "| ";
          }
          node_str << "<f" << node->count() << ">\"];\n";
      }

      return node_str.str();
    }

private:
    Comparator comparator_;
    static thread_local uint64_t gc_id_;
    std::atomic<OlcNode*> root_;
};

}

