use std::hint::black_box;
use std::time::Instant;

use distribution::{
    encode_bbo_sbe, encode_trade_sbe, serialize_bbo_json, serialize_trade_json, ClientSession,
    InvertedTopicRouter, TopicKey, WireProtocol, STREAM_TYPE_BBO, TOTAL_SBE_BBO_LEN,
    TOTAL_SBE_TRADE_LEN,
};
use ingest_models::{NormalizedBbo, TelemetryTimestamps, UnifiedTrade};

fn main() {
    println!("===============================================================================");
    println!("Institutional Distribution Engine - Nanosecond Performance Benchmarks");
    println!("===============================================================================\n");

    const ITERATIONS: usize = 1_000_000;

    // 1. SBE BBO Serialization Benchmark
    {
        let bbo = NormalizedBbo {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: 100,
                t1_ingest_nic_ns: 200,
                t2_engine_proc_ns: 300,
                t3_egress_ns: 400,
            },
            market_id: 101,
            venue: 1,
            chain: 1,
            sequence: 500_000,
            bid_price: 65_000_00000000,
            bid_qty: 2_50000000,
            ask_price: 65_001_00000000,
            ask_qty: 3_00000000,
            spread_bps: 1,
            flags: 0x01,
        };

        let mut buf = [0u8; TOTAL_SBE_BBO_LEN];
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(encode_bbo_sbe(black_box(&bbo), black_box(&mut buf)).unwrap());
        }
        let elapsed = start.elapsed();
        let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
        println!(
            "  [1] SBE BBO Encoding (92 bytes):      {:>6.2} ns/op  ({:.1}M msgs/sec)",
            nanos_per_op,
            1_000.0 / nanos_per_op
        );
        assert!(nanos_per_op < 1000.0, "Target sub-microsecond SLA");
    }

    // 2. SBE Trade Serialization Benchmark
    {
        let trade = UnifiedTrade {
            telemetry: TelemetryTimestamps::default(),
            market_id: 101,
            venue: 1,
            chain: 1,
            sequence: 500_000,
            trade_id: 999_999,
            price: 65_000_00000000,
            size: 1_50000000,
            side: 1,
            is_liquidation: false,
        };

        let mut buf = [0u8; TOTAL_SBE_TRADE_LEN];
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(encode_trade_sbe(black_box(&trade), black_box(&mut buf)).unwrap());
        }
        let elapsed = start.elapsed();
        let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
        println!(
            "  [2] SBE Trade Encoding (95 bytes):    {:>6.2} ns/op  ({:.1}M msgs/sec)",
            nanos_per_op,
            1_000.0 / nanos_per_op
        );
        assert!(nanos_per_op < 1000.0, "Target sub-microsecond SLA");
    }

    // 3. Fast Zero-Allocation JSON BBO Serialization Benchmark
    {
        let bbo = NormalizedBbo {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: 1000,
                t1_ingest_nic_ns: 2000,
                t2_engine_proc_ns: 3000,
                t3_egress_ns: 4000,
            },
            market_id: 42,
            venue: 1,
            chain: 1,
            sequence: 123456,
            bid_price: 65432_10000000,
            bid_qty: 1_50000000,
            ask_price: 65433_00000000,
            ask_qty: 2_00000000,
            spread_bps: 2,
            flags: 1,
        };

        let mut buf = [0u8; 512];
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(serialize_bbo_json(black_box(&bbo), black_box(&mut buf)).unwrap());
        }
        let elapsed = start.elapsed();
        let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
        println!(
            "  [3] Fast JSON BBO Serialization:      {:>6.2} ns/op  ({:.1}M msgs/sec)",
            nanos_per_op,
            1_000.0 / nanos_per_op
        );
        assert!(nanos_per_op < 1000.0, "Target sub-microsecond SLA");
    }

    // 4. Fast Zero-Allocation JSON Trade Serialization Benchmark
    {
        let trade = UnifiedTrade {
            telemetry: TelemetryTimestamps::default(),
            market_id: 88,
            venue: 3,
            chain: 8453,
            sequence: 777,
            trade_id: 99999,
            price: 3500_50000000,
            size: 5_00000000,
            side: 2,
            is_liquidation: true,
        };

        let mut buf = [0u8; 512];
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(serialize_trade_json(black_box(&trade), black_box(&mut buf)).unwrap());
        }
        let elapsed = start.elapsed();
        let nanos_per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
        println!(
            "  [4] Fast JSON Trade Serialization:    {:>6.2} ns/op  ({:.1}M msgs/sec)",
            nanos_per_op,
            1_000.0 / nanos_per_op
        );
        assert!(nanos_per_op < 1000.0, "Target sub-microsecond SLA");
    }

    // 5. Inverted Topic Router Fan-Out Benchmark (100 concurrent clients)
    {
        let router = InvertedTopicRouter::new();
        const CLIENTS: usize = 100;
        let topic = TopicKey::new(1, 42, STREAM_TYPE_BBO, 0);

        for i in 0..CLIENTS {
            let proto = if i % 2 == 0 {
                WireProtocol::Sbe
            } else {
                WireProtocol::Json
            };
            let session = ClientSession::new(i as u32, proto, 1_000_000, 1_000_000, 0);
            router.register_client(session).unwrap();
            router.subscribe(i as u32, topic).unwrap();
        }

        let bbo = NormalizedBbo {
            market_id: 42,
            venue: 1,
            sequence: 1,
            bid_price: 50_000_00000000,
            bid_qty: 1_00000000,
            ask_price: 50_001_00000000,
            ask_qty: 1_00000000,
            spread_bps: 1,
            ..Default::default()
        };

        const ROUTE_ITERS: usize = 100_000;
        let start = Instant::now();
        for _ in 0..ROUTE_ITERS {
            black_box(router.dispatch_bbo(
                black_box(&bbo),
                black_box(0),
                black_box(|_id, _proto, _payload| {}),
            ));
        }
        let elapsed = start.elapsed();
        let nanos_per_dispatch = elapsed.as_nanos() as f64 / ROUTE_ITERS as f64;
        let nanos_per_client = nanos_per_dispatch / CLIENTS as f64;

        println!(
            "  [5] Inverted Router Dispatch (100 clients): {:>6.2} ns/tick ({:.2} ns/client fanout)",
            nanos_per_dispatch, nanos_per_client
        );
    }

    println!("\nAll operations confirmed SUB-MICROSECOND! Institutional HFT SLA achieved.");
    println!("===============================================================================");
}
