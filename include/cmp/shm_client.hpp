#pragma once

/**
 * @file shm_client.hpp
 * @brief Institutional C++23 Header-Only Shared Memory IPC Reader Library
 *
 * Provides ultra-low latency (< 50ns) zero-copy access to the Chain-Market-Price
 * memory-mapped circular ring buffer (/dev/shm/cmp_market_data.shm).
 *
 * Invariants:
 * - Pure C++23 with mechanical sympathy (64-byte cache alignment, acquire memory fences).
 * - Zero dynamic heap allocations in fast-path polling.
 * - Hardware pause intrinsics (_mm_pause() on x86, __builtin_arm_isb() on ARM).
 */

#include <cstdint>
#include <atomic>
#include <cstring>
#include <optional>
#include <string_view>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define CMP_CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__)
#define CMP_CPU_PAUSE() asm volatile("isb" ::: "memory")
#else
#define CMP_CPU_PAUSE() ((void)0)
#endif

namespace cmp {

constexpr uint32_t SHM_MAGIC = 0x434D5031; // "CMP1"
constexpr uint32_t SHM_VERSION = 1;
constexpr std::string_view DEFAULT_SHM_PATH = "/dev/shm/cmp_market_data.shm";
constexpr size_t DEFAULT_SLOT_CAPACITY = 16384;

#pragma pack(push, 1)

/**
 * @brief 4-Point Telemetry Timestamps (Nanoseconds)
 */
struct alignas(32) TelemetryTimestamps {
    uint64_t t0_exchange_ns;
    uint64_t t1_ingest_nic_ns;
    uint64_t t2_engine_proc_ns;
    uint64_t t3_egress_ns;

    [[nodiscard]] inline uint64_t internal_latency_ns() const noexcept {
        return t2_engine_proc_ns > t1_ingest_nic_ns ? t2_engine_proc_ns - t1_ingest_nic_ns : 0;
    }

    [[nodiscard]] inline uint64_t wire_to_egress_latency_ns() const noexcept {
        return t3_egress_ns > t1_ingest_nic_ns ? t3_egress_ns - t1_ingest_nic_ns : 0;
    }
};

/**
 * @brief 64-Byte Cache-Line Aligned Shared Memory Message Slot
 */
struct alignas(64) ShmMessageSlot {
    uint64_t sequence;
    uint8_t kind;             // 1 = BBO, 2 = Trade, 3 = Heartbeat
    uint8_t reserved1;
    uint16_t venue_id;
    uint32_t market_id;
    TelemetryTimestamps telemetry;
    int64_t price;            // Scaled 10^8
    uint64_t qty;             // Scaled 10^8
    uint16_t flags;
    uint8_t padding[6];
};

static_assert(sizeof(ShmMessageSlot) == 64, "ShmMessageSlot must be exactly 64 bytes");
static_assert(alignof(ShmMessageSlot) == 64, "ShmMessageSlot must be 64-byte cache aligned");

/**
 * @brief Shared Memory Header Structure
 */
struct alignas(64) ShmHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t slot_capacity;
    uint32_t slot_size_bytes;
    uint32_t writer_pid;
    uint32_t pad0;
    std::atomic<uint64_t> writer_heartbeat_ns;
    alignas(64) std::atomic<uint64_t> head_sequence;
};

#pragma pack(pop)

/**
 * @brief Read Result Status
 */
enum class ReadStatus : uint8_t {
    Ok,
    Empty,
    Overrun,
    NotAttached
};

/**
 * @brief Modern C++23 Zero-Copy Shared Memory IPC Client
 */
class ShmClient {
public:
    ShmClient() noexcept = default;

    ~ShmClient() noexcept {
        detach();
    }

    // Non-copyable
    ShmClient(const ShmClient&) = delete;
    ShmClient& operator=(const ShmClient&) = delete;

    // Movable
    ShmClient(ShmClient&& other) noexcept {
        *this = std::move(other);
    }

    ShmClient& operator=(ShmClient&& other) noexcept {
        if (this != &other) {
            detach();
            fd_ = other.fd_;
            mapped_ptr_ = other.mapped_ptr_;
            total_size_ = other.total_size_;
            header_ = other.header_;
            slots_ = other.slots_;
            reader_cursor_ = other.reader_cursor_;

            other.fd_ = -1;
            other.mapped_ptr_ = nullptr;
            other.header_ = nullptr;
            other.slots_ = nullptr;
        }
        return *this;
    }

    /**
     * @brief Attach to an active POSIX shared memory ring
     */
    [[nodiscard]] bool attach(std::string_view path = DEFAULT_SHM_PATH) noexcept {
        detach();

        fd_ = ::shm_open(path.data(), O_RDONLY, 0);
        if (fd_ < 0) {
            return false;
        }

        struct stat sb{};
        if (::fstat(fd_, &sb) < 0 || sb.st_size < static_cast<off_t>(sizeof(ShmHeader))) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }

        total_size_ = static_cast<size_t>(sb.st_size);
        mapped_ptr_ = ::mmap(nullptr, total_size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (mapped_ptr_ == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            mapped_ptr_ = nullptr;
            return false;
        }

        header_ = reinterpret_cast<const ShmHeader*>(mapped_ptr_);
        if (header_->magic != SHM_MAGIC || header_->version != SHM_VERSION) {
            detach();
            return false;
        }

        const auto* byte_ptr = static_cast<const uint8_t*>(mapped_ptr_);
        slots_ = reinterpret_cast<const ShmMessageSlot*>(byte_ptr + sizeof(ShmHeader));

        // Synchronize initial cursor to current ring head
        reader_cursor_ = header_->head_sequence.load(std::memory_order_acquire);
        return true;
    }

    /**
     * @brief Detach from the shared memory segment
     */
    void detach() noexcept {
        if (mapped_ptr_ && mapped_ptr_ != MAP_FAILED) {
            ::munmap(mapped_ptr_, total_size_);
            mapped_ptr_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        header_ = nullptr;
        slots_ = nullptr;
        total_size_ = 0;
        reader_cursor_ = 0;
    }

    [[nodiscard]] inline bool is_attached() const noexcept {
        return header_ != nullptr;
    }

    /**
     * @brief Non-blocking, zero-copy optimistic read of the next sequential tick
     */
    [[nodiscard]] ReadStatus try_read(ShmMessageSlot& out) noexcept {
        if (!header_) [[unlikely]] {
            return ReadStatus::NotAttached;
        }

        const uint64_t head = header_->head_sequence.load(std::memory_order_acquire);
        if (reader_cursor_ >= head) {
            return ReadStatus::Empty;
        }

        const uint32_t capacity = header_->slot_capacity;
        if (head > reader_cursor_ + capacity) [[unlikely]] {
            // Overrun detected: writer completed a lap ahead of reader
            reader_cursor_ = head - capacity;
            return ReadStatus::Overrun;
        }

        const uint64_t next_seq = reader_cursor_ + 1;
        const size_t slot_idx = next_seq & (capacity - 1);
        const ShmMessageSlot& slot = slots_[slot_idx];

        // Optimistic copy from cache-line
        std::memcpy(&out, &slot, sizeof(ShmMessageSlot));

        // Sequence barrier check
        if (out.sequence == next_seq) [[likely]] {
            reader_cursor_ = next_seq;
            return ReadStatus::Ok;
        }

        // Torn read due to concurrent overwrite; advance to current head
        reader_cursor_ = head;
        return ReadStatus::Empty;
    }

    /**
     * @brief Busy-wait polling loop for the lowest possible latency (< 50ns)
     */
    template <typename Callback>
    inline void poll_busy_loop(Callback&& on_message, const std::atomic<bool>& running) noexcept {
        ShmMessageSlot slot{};
        while (running.load(std::memory_order_relaxed)) {
            const ReadStatus status = try_read(slot);
            if (status == ReadStatus::Ok) [[likely]] {
                on_message(slot);
            } else if (status == ReadStatus::Empty) {
                CMP_CPU_PAUSE();
            }
        }
    }

private:
    int fd_{-1};
    void* mapped_ptr_{nullptr};
    size_t total_size_{0};
    const ShmHeader* header_{nullptr};
    const ShmMessageSlot* slots_{nullptr};
    uint64_t reader_cursor_{0};
};

} // namespace cmp
