//! # Normalized CeFi / DeFi Market Data Models
//!
//! Provides unified data models across Centralized Exchanges (Binance, OKX, Bybit)
//! and Decentralized Finance AMMs (Uniswap v2/v3/v4, Raydium CLMM, Curve).

/// 4-Point Telemetry Timestamps for nanosecond latency tracing
#[repr(C)]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct TelemetryTimestamps {
    /// T0: Venue matching engine timestamp or on-chain block/mempool arrival
    pub t0_exchange_ns: u64,
    /// T1: Solarflare NIC packet reception hardware timestamp (IEEE 1588 PTP)
    pub t1_ingest_nic_ns: u64,
    /// T2: Core normalization & book update completion timestamp
    pub t2_engine_proc_ns: u64,
    /// T3: Outbound wire dispatch timestamp
    pub t3_egress_ns: u64,
}

impl TelemetryTimestamps {
    #[inline(always)]
    pub fn internal_processing_latency_ns(&self) -> u64 {
        self.t2_engine_proc_ns.saturating_sub(self.t1_ingest_nic_ns)
    }

    #[inline(always)]
    pub fn wire_to_egress_latency_ns(&self) -> u64 {
        self.t3_egress_ns.saturating_sub(self.t1_ingest_nic_ns)
    }
}

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VenueId {
    Binance = 1,
    Coinbase = 2,
    Bybit = 3,
    Okx = 4,
    UniswapV2 = 101,
    UniswapV3 = 102,
    UniswapV4 = 104,
    Curve = 103,
    Raydium = 201,
    Orca = 202,
}

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainId {
    OffChain = 0,
    Ethereum = 1,
    Optimism = 10,
    Bsc = 56,
    Solana = 501,
    Base = 8453,
    Arbitrum = 42161,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy = 1,
    Sell = 2,
    TwoSided = 3,
}

/// Normalized Best Bid & Offer (L1 BBO)
#[repr(C)]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct NormalizedBbo {
    pub telemetry: TelemetryTimestamps,
    pub market_id: u32,
    pub venue: u16,
    pub chain: u16,
    pub sequence: u64,
    pub bid_price: i64,  // Scaled 10^8
    pub bid_qty: u64,    // Scaled 10^8
    pub ask_price: i64,  // Scaled 10^8
    pub ask_qty: u64,    // Scaled 10^8
    pub spread_bps: u16,
    pub flags: u16,      // 0x01 = Synthetic AMM, 0x02 = Mempool Inferred
}

/// Unified Trade Execution (CeFi Match or AMM Swap)
#[repr(C)]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct UnifiedTrade {
    pub telemetry: TelemetryTimestamps,
    pub market_id: u32,
    pub venue: u16,
    pub chain: u16,
    pub sequence: u64,
    pub trade_id: u64,
    pub price: i64,      // Scaled 10^8
    pub size: u64,       // Scaled 10^8
    pub side: u8,
    pub is_liquidation: bool,
}
