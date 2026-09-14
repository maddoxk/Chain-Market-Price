//! # Simple Binary Encoding (SBE) Wire Framing
//!
//! Provides zero-copy, zero-heap SBE serialization and deserialization
//! based on `schemas/market_data.sbe.xml`.
//!
//! Header: 8 bytes (`blockLength`: u16, `templateId`: u16, `schemaId`: u16, `version`: u16)
//!
//! Message 101: `BBOReport` (84 bytes payload, 92 bytes framed)
//! Message 104: `TradeExecutionReport` (85 bytes payload + 2 bytes varData len, 95 bytes framed)

use ingest_models::{NormalizedBbo, TelemetryTimestamps, UnifiedTrade};

pub const SBE_HEADER_LEN: usize = 8;
pub const SBE_SCHEMA_ID: u16 = 1;
pub const SBE_SCHEMA_VERSION: u16 = 1;

pub const TEMPLATE_ID_BBO: u16 = 101;
pub const BLOCK_LENGTH_BBO: u16 = 84;
pub const TOTAL_SBE_BBO_LEN: usize = SBE_HEADER_LEN + BLOCK_LENGTH_BBO as usize; // 92 bytes

pub const TEMPLATE_ID_TRADE: u16 = 104;
pub const BLOCK_LENGTH_TRADE: u16 = 85;
pub const TOTAL_SBE_TRADE_LEN: usize = SBE_HEADER_LEN + BLOCK_LENGTH_TRADE as usize + 2; // 95 bytes (with 0 varData)

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SbeError {
    BufferTooSmall { required: usize, provided: usize },
    InvalidHeader,
    UnsupportedTemplateId(u16),
    InvalidSchemaId(u16),
    InvalidSchemaVersion(u16),
}

/// SBE Standard Framing Header (8 bytes)
#[repr(C, packed)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SbeMessageHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

impl SbeMessageHeader {
    #[inline(always)]
    pub fn new(block_length: u16, template_id: u16) -> Self {
        Self {
            block_length,
            template_id,
            schema_id: SBE_SCHEMA_ID,
            version: SBE_SCHEMA_VERSION,
        }
    }

    #[inline(always)]
    pub fn write_to(&self, buf: &mut [u8]) {
        buf[0..2].copy_from_slice(&self.block_length.to_le_bytes());
        buf[2..4].copy_from_slice(&self.template_id.to_le_bytes());
        buf[4..6].copy_from_slice(&self.schema_id.to_le_bytes());
        buf[6..8].copy_from_slice(&self.version.to_le_bytes());
    }

    #[inline(always)]
    pub fn read_from(buf: &[u8]) -> Result<Self, SbeError> {
        if buf.len() < SBE_HEADER_LEN {
            return Err(SbeError::BufferTooSmall {
                required: SBE_HEADER_LEN,
                provided: buf.len(),
            });
        }
        let block_length = u16::from_le_bytes([buf[0], buf[1]]);
        let template_id = u16::from_le_bytes([buf[2], buf[3]]);
        let schema_id = u16::from_le_bytes([buf[4], buf[5]]);
        let version = u16::from_le_bytes([buf[6], buf[7]]);

        if schema_id != SBE_SCHEMA_ID {
            return Err(SbeError::InvalidSchemaId(schema_id));
        }

        Ok(Self {
            block_length,
            template_id,
            schema_id,
            version,
        })
    }
}

/// Zero-copy, zero-heap SBE encoder for `NormalizedBbo` (Message 101)
#[inline(always)]
pub fn encode_bbo_sbe(bbo: &NormalizedBbo, out: &mut [u8]) -> Result<usize, SbeError> {
    if out.len() < TOTAL_SBE_BBO_LEN {
        return Err(SbeError::BufferTooSmall {
            required: TOTAL_SBE_BBO_LEN,
            provided: out.len(),
        });
    }

    // Header (8 bytes)
    let header = SbeMessageHeader::new(BLOCK_LENGTH_BBO, TEMPLATE_ID_BBO);
    header.write_to(&mut out[0..8]);

    // Telemetry (32 bytes)
    out[8..16].copy_from_slice(&bbo.telemetry.t0_exchange_ns.to_le_bytes());
    out[16..24].copy_from_slice(&bbo.telemetry.t1_ingest_nic_ns.to_le_bytes());
    out[24..32].copy_from_slice(&bbo.telemetry.t2_engine_proc_ns.to_le_bytes());
    out[32..40].copy_from_slice(&bbo.telemetry.t3_egress_ns.to_le_bytes());

    // Venue & Chain (4 bytes)
    out[40..42].copy_from_slice(&bbo.venue.to_le_bytes());
    out[42..44].copy_from_slice(&bbo.chain.to_le_bytes());

    // Market ID (4 bytes)
    out[44..48].copy_from_slice(&bbo.market_id.to_le_bytes());

    // Sequence (8 bytes)
    out[48..56].copy_from_slice(&bbo.sequence.to_le_bytes());

    // Prices and Quantities (32 bytes)
    out[56..64].copy_from_slice(&bbo.bid_price.to_le_bytes());
    out[64..72].copy_from_slice(&bbo.bid_qty.to_le_bytes());
    out[72..80].copy_from_slice(&bbo.ask_price.to_le_bytes());
    out[80..88].copy_from_slice(&bbo.ask_qty.to_le_bytes());

    // Spread & Flags (4 bytes)
    out[88..90].copy_from_slice(&bbo.spread_bps.to_le_bytes());
    out[90..92].copy_from_slice(&bbo.flags.to_le_bytes());

    Ok(TOTAL_SBE_BBO_LEN)
}

/// Zero-copy, zero-heap SBE decoder for `NormalizedBbo` (Message 101)
#[inline(always)]
pub fn decode_bbo_sbe(buf: &[u8]) -> Result<NormalizedBbo, SbeError> {
    if buf.len() < TOTAL_SBE_BBO_LEN {
        return Err(SbeError::BufferTooSmall {
            required: TOTAL_SBE_BBO_LEN,
            provided: buf.len(),
        });
    }

    let header = SbeMessageHeader::read_from(&buf[0..8])?;
    if header.template_id != TEMPLATE_ID_BBO {
        return Err(SbeError::UnsupportedTemplateId(header.template_id));
    }

    let t0 = u64::from_le_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);
    let t1 = u64::from_le_bytes([
        buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
    ]);
    let t2 = u64::from_le_bytes([
        buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
    ]);
    let t3 = u64::from_le_bytes([
        buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
    ]);

    let venue = u16::from_le_bytes([buf[40], buf[41]]);
    let chain = u16::from_le_bytes([buf[42], buf[43]]);
    let market_id = u32::from_le_bytes([buf[44], buf[45], buf[46], buf[47]]);
    let sequence = u64::from_le_bytes([
        buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55],
    ]);

    let bid_price = i64::from_le_bytes([
        buf[56], buf[57], buf[58], buf[59], buf[60], buf[61], buf[62], buf[63],
    ]);
    let bid_qty = u64::from_le_bytes([
        buf[64], buf[65], buf[66], buf[67], buf[68], buf[69], buf[70], buf[71],
    ]);
    let ask_price = i64::from_le_bytes([
        buf[72], buf[73], buf[74], buf[75], buf[76], buf[77], buf[78], buf[79],
    ]);
    let ask_qty = u64::from_le_bytes([
        buf[80], buf[81], buf[82], buf[83], buf[84], buf[85], buf[86], buf[87],
    ]);

    let spread_bps = u16::from_le_bytes([buf[88], buf[89]]);
    let flags = u16::from_le_bytes([buf[90], buf[91]]);

    Ok(NormalizedBbo {
        telemetry: TelemetryTimestamps {
            t0_exchange_ns: t0,
            t1_ingest_nic_ns: t1,
            t2_engine_proc_ns: t2,
            t3_egress_ns: t3,
        },
        venue,
        chain,
        market_id,
        sequence,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
        spread_bps,
        flags,
    })
}

/// Zero-copy, zero-heap SBE encoder for `UnifiedTrade` (Message 104)
#[inline(always)]
pub fn encode_trade_sbe(trade: &UnifiedTrade, out: &mut [u8]) -> Result<usize, SbeError> {
    if out.len() < TOTAL_SBE_TRADE_LEN {
        return Err(SbeError::BufferTooSmall {
            required: TOTAL_SBE_TRADE_LEN,
            provided: out.len(),
        });
    }

    // Header (8 bytes)
    let header = SbeMessageHeader::new(BLOCK_LENGTH_TRADE, TEMPLATE_ID_TRADE);
    header.write_to(&mut out[0..8]);

    // Telemetry (32 bytes)
    out[8..16].copy_from_slice(&trade.telemetry.t0_exchange_ns.to_le_bytes());
    out[16..24].copy_from_slice(&trade.telemetry.t1_ingest_nic_ns.to_le_bytes());
    out[24..32].copy_from_slice(&trade.telemetry.t2_engine_proc_ns.to_le_bytes());
    out[32..40].copy_from_slice(&trade.telemetry.t3_egress_ns.to_le_bytes());

    // Venue & Chain (4 bytes)
    out[40..42].copy_from_slice(&trade.venue.to_le_bytes());
    out[42..44].copy_from_slice(&trade.chain.to_le_bytes());

    // Market ID (4 bytes)
    out[44..48].copy_from_slice(&trade.market_id.to_le_bytes());

    // Sequence & TradeId (16 bytes)
    out[48..56].copy_from_slice(&trade.sequence.to_le_bytes());
    out[56..64].copy_from_slice(&trade.trade_id.to_le_bytes());

    // Side (1 byte)
    out[64] = trade.side;

    // Price & Size (base qty) (16 bytes)
    out[65..73].copy_from_slice(&trade.price.to_le_bytes());
    out[73..81].copy_from_slice(&trade.size.to_le_bytes());

    // Quote Qty: price * size / 10^8 (8 bytes)
    let quote_qty = if trade.price > 0 {
        ((trade.price as u128 * trade.size as u128) / 100_000_000) as u64
    } else {
        0
    };
    out[81..89].copy_from_slice(&quote_qty.to_le_bytes());

    // Fee Bps (2 bytes)
    out[89..91].copy_from_slice(&0u16.to_le_bytes());

    // isLiquidation (1 byte)
    out[91] = if trade.is_liquidation { 1 } else { 0 };

    // executionVenueType (1 byte) -> 1 = CLOB
    out[92] = 1;

    // varData length (2 bytes) = 0 bytes of protocol context
    out[93..95].copy_from_slice(&0u16.to_le_bytes());

    Ok(TOTAL_SBE_TRADE_LEN)
}

/// Zero-copy, zero-heap SBE decoder for `UnifiedTrade` (Message 104)
#[inline(always)]
pub fn decode_trade_sbe(buf: &[u8]) -> Result<UnifiedTrade, SbeError> {
    if buf.len() < TOTAL_SBE_TRADE_LEN {
        return Err(SbeError::BufferTooSmall {
            required: TOTAL_SBE_TRADE_LEN,
            provided: buf.len(),
        });
    }

    let header = SbeMessageHeader::read_from(&buf[0..8])?;
    if header.template_id != TEMPLATE_ID_TRADE {
        return Err(SbeError::UnsupportedTemplateId(header.template_id));
    }

    let t0 = u64::from_le_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);
    let t1 = u64::from_le_bytes([
        buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
    ]);
    let t2 = u64::from_le_bytes([
        buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
    ]);
    let t3 = u64::from_le_bytes([
        buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
    ]);

    let venue = u16::from_le_bytes([buf[40], buf[41]]);
    let chain = u16::from_le_bytes([buf[42], buf[43]]);
    let market_id = u32::from_le_bytes([buf[44], buf[45], buf[46], buf[47]]);
    let sequence = u64::from_le_bytes([
        buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55],
    ]);
    let trade_id = u64::from_le_bytes([
        buf[56], buf[57], buf[58], buf[59], buf[60], buf[61], buf[62], buf[63],
    ]);

    let side = buf[64];
    let price = i64::from_le_bytes([
        buf[65], buf[66], buf[67], buf[68], buf[69], buf[70], buf[71], buf[72],
    ]);
    let size = u64::from_le_bytes([
        buf[73], buf[74], buf[75], buf[76], buf[77], buf[78], buf[79], buf[80],
    ]);

    let is_liquidation = buf[91] != 0;

    Ok(UnifiedTrade {
        telemetry: TelemetryTimestamps {
            t0_exchange_ns: t0,
            t1_ingest_nic_ns: t1,
            t2_engine_proc_ns: t2,
            t3_egress_ns: t3,
        },
        venue,
        chain,
        market_id,
        sequence,
        trade_id,
        price,
        size,
        side,
        is_liquidation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sbe_bbo_roundtrip() {
        let bbo = NormalizedBbo {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: 1_000_000_000,
                t1_ingest_nic_ns: 1_000_000_100,
                t2_engine_proc_ns: 1_000_000_200,
                t3_egress_ns: 1_000_000_300,
            },
            market_id: 1001,
            venue: 1,
            chain: 1,
            sequence: 424242,
            bid_price: 65_000_00000000,
            bid_qty: 1_50000000,
            ask_price: 65_001_00000000,
            ask_qty: 2_00000000,
            spread_bps: 1,
            flags: 0x01,
        };

        let mut buf = [0u8; 128];
        let bytes_written = encode_bbo_sbe(&bbo, &mut buf).expect("encoding failed");
        assert_eq!(bytes_written, TOTAL_SBE_BBO_LEN);

        let decoded = decode_bbo_sbe(&buf[..bytes_written]).expect("decoding failed");
        assert_eq!(bbo, decoded);
    }

    #[test]
    fn test_sbe_trade_roundtrip() {
        let trade = UnifiedTrade {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: 500,
                t1_ingest_nic_ns: 600,
                t2_engine_proc_ns: 700,
                t3_egress_ns: 800,
            },
            market_id: 2002,
            venue: 2,
            chain: 42161,
            sequence: 88888,
            trade_id: 999999,
            price: 3_500_00000000,
            size: 10_00000000,
            side: 1,
            is_liquidation: false,
        };

        let mut buf = [0u8; 128];
        let bytes_written = encode_trade_sbe(&trade, &mut buf).expect("encoding failed");
        assert_eq!(bytes_written, TOTAL_SBE_TRADE_LEN);

        let decoded = decode_trade_sbe(&buf[..bytes_written]).expect("decoding failed");
        assert_eq!(trade, decoded);
    }

    #[test]
    fn test_sbe_buffer_too_small() {
        let bbo = NormalizedBbo::default();
        let mut buf = [0u8; 50];
        assert!(encode_bbo_sbe(&bbo, &mut buf).is_err());
    }
}
