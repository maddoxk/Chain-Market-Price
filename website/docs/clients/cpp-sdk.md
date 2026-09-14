---
id: cpp-sdk
title: Modern C++23 Header-Only Reader Library
sidebar_label: C++23 SDK
---

# Modern C++23 Header-Only Reader Library

For quantitative developers building execution algorithms and automated market makers in Modern C++23, `Chain-Market-Price` provides [`include/cmp/shm_client.hpp`](https://github.com/maddoxk/Chain-Market-Price/blob/main/include/cmp/shm_client.hpp).

---

## 1. Features & Invariants

- **Header-Only:** Single include with zero external dependencies beyond the C++ standard library.
- **Sub-50ns Access:** Direct pointer arithmetic over `/dev/shm/cmp_market_data.shm`.
- **Zero Dynamic Allocations:** Never allocates heap memory on the hot polling path.
- **Hardware Pause Instructions:** Issues `_mm_pause()` (x86-64) or `isb` (ARM64) during idle wait states.

---

## 2. Integration Example

```cpp
#include "cmp/shm_client.hpp"
#include <iostream>
#include <iomanip>
#include <atomic>

int main() {
    cmp::ShmClient client;
    
    // Attach to the local memory-mapped circular ring
    if (!client.attach("/dev/shm/cmp_market_data.shm")) {
        std::cerr << "Failed to attach to Shared Memory!" << std::endl;
        return 1;
    }

    std::cout << "Attached to Chain-Market-Price Shared Memory!" << std::endl;

    std::atomic<bool> running{true};

    // Sub-50ns low-latency busy-wait polling loop
    client.poll_busy_loop([&](const cmp::ShmMessageSlot& slot) noexcept {
        const double price = static_cast<double>(slot.price) / 1e8;
        const double qty = static_cast<double>(slot.qty) / 1e8;
        const uint64_t latency_ns = slot.telemetry.wire_to_egress_latency_ns();

        std::cout << "Seq: " << slot.sequence
                  << " | Venue: " << slot.venue_id
                  << " | Market: " << slot.market_id
                  << " | Price: $" << std::fixed << std::setprecision(2) << price
                  << " | Wire->Egress: " << latency_ns << " ns\n";
    }, running);

    return 0;
}
```

### Compilation
```bash
g++ -O3 -std=c++23 -march=native -Iinclude examples/cpp/strategy_consumer.cpp -o strategy_consumer
```
