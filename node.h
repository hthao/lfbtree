#pragma once

#include <assert.h>
#include <stdint.h>
#include <immintrin.h>
#include <atomic>
#include <cstring>
#include <array>

namespace olcbtree {

// @Abstract
// Optimized Lock Coupling Node.
class OlcNode {
public:

    static const uint32_t BtreeMaxDegree = 8;

    enum class Type : uint32_t {
        EnumInner = 1,
        EnumLeaf = 2
    };

    OlcNode():
        version_{0b100},
        count_(0)
    {
        assert(BtreeMaxDegree > 3);
    }

    ~OlcNode() {}

    // No copy
    OlcNode(const OlcNode&) = delete;
    OlcNode& operator=(const OlcNode&) = delete;

    // No move
    OlcNode(const OlcNode&&) = delete;
    OlcNode&& operator=(const OlcNode&&) = delete;

    // Node type.
    virtual Type type() = 0;

    virtual uint32_t count() {
        return count_;
    }

    bool is_full() {
        return count_ == BtreeMaxDegree;
    }

    bool is_less_than_half() {
        return count_ < (BtreeMaxDegree + 1) / 2;
    }

    bool is_more_than_half() {
        return count_ > (BtreeMaxDegree + 1) / 2;
    }

    bool is_locked(uint64_t version) {
        return ((version & 0b10) == 0b10);
    }

    uint64_t readlock_or_restart(bool &need_restart) {
        uint64_t version;
        version = version_.load();
        if (is_locked(version) || is_obsolete(version)) {
            _mm_pause();
            need_restart = true;
        }
        return version;
    }

    void writelock_or_restart(bool &need_restart) {
        uint64_t version;
        version = readlock_or_restart(need_restart);
        if (need_restart) return;

        upgrade_to_writelock_or_restart(version, need_restart);
        if (need_restart) return;
    }

    void upgrade_to_writelock_or_restart(uint64_t &version, bool &need_restart) {
        if (version_.compare_exchange_strong(version, version + 0b10)) {
            version = version + 0b10;
        } else {
            _mm_pause();
            need_restart = true;
        }
    }

    void write_unlock() {
        version_.fetch_add(0b10);
    }

    bool is_obsolete(uint64_t version) {
        return (version & 1) == 1;
    }

    void check_or_restart(uint64_t version, bool &need_restart) const {
        read_unlock_or_restart(version, need_restart);
    }

    void read_unlock_or_restart(uint64_t version, bool &need_restart) const {
        need_restart = (version != version_.load());
    }

    void write_unlock_obsolete() {
        version_.fetch_add(0b11);
    }

    // Version based lock.
    // uint64_t version
    // | reserved (2 bit) | version (60 bit) | lock (1 bit) | obsolete (1 bit) |
    // Default: version = 1, lock = 0, obsolete = 0;
    std::atomic<uint64_t> version_;
    uint32_t count_;
};

}
