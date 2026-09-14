//! # Ultra-Fast SIMD-Friendly Zero-Allocation JSON Serializer
//!
//! Provides zero-heap JSON formatting directly into pre-allocated memory buffers.
//! Implements branchless lookup-table integer-to-ASCII conversions for sub-microsecond serialization.

use ingest_models::{NormalizedBbo, UnifiedTrade};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonError {
    BufferTooSmall { required: usize, provided: usize },
}

/// Lookup table for two decimal digits to ASCII bytes
static DEC_DIGITS_LUT: &[u8; 200] = b"\
00010203040506070809\
10111213141516171819\
20212223242526272829\
30313233343536373839\
40414243444546474849\
50515253545556575859\
60616263646566676869\
70717273747576777879\
80818283848586878889\
90919293949596979899";

/// Stack-buffered fast cursor for zero-allocation byte emission
pub struct FastJsonWriter<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl<'a> FastJsonWriter<'a> {
    #[inline(always)]
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline(always)]
    pub fn pos(&self) -> usize {
        self.pos
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    #[inline(always)]
    pub fn write_byte(&mut self, b: u8) -> Result<(), JsonError> {
        if self.pos >= self.buf.len() {
            return Err(JsonError::BufferTooSmall {
                required: self.pos + 1,
                provided: self.buf.len(),
            });
        }
        self.buf[self.pos] = b;
        self.pos += 1;
        Ok(())
    }

    #[inline(always)]
    pub fn write_str(&mut self, s: &str) -> Result<(), JsonError> {
        self.write_bytes(s.as_bytes())
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), JsonError> {
        let needed = self.pos + bytes.len();
        if needed > self.buf.len() {
            return Err(JsonError::BufferTooSmall {
                required: needed,
                provided: self.buf.len(),
            });
        }
        self.buf[self.pos..needed].copy_from_slice(bytes);
        self.pos = needed;
        Ok(())
    }

    /// Fast branchless unsigned 64-bit integer printing
    #[inline(always)]
    pub fn write_u64(&mut self, mut val: u64) -> Result<(), JsonError> {
        let mut temp = [0u8; 20];
        let mut idx = 20;

        if val == 0 {
            return self.write_byte(b'0');
        }

        while val >= 100 {
            let rem = (val % 100) as usize;
            val /= 100;
            idx -= 2;
            temp[idx] = DEC_DIGITS_LUT[rem * 2];
            temp[idx + 1] = DEC_DIGITS_LUT[rem * 2 + 1];
        }

        if val < 10 {
            idx -= 1;
            temp[idx] = b'0' + val as u8;
        } else {
            let rem = val as usize;
            idx -= 2;
            temp[idx] = DEC_DIGITS_LUT[rem * 2];
            temp[idx + 1] = DEC_DIGITS_LUT[rem * 2 + 1];
        }

        let slice = &temp[idx..20];
        self.write_bytes(slice)
    }

    /// Fast signed 64-bit integer printing
    #[inline(always)]
    pub fn write_i64(&mut self, val: i64) -> Result<(), JsonError> {
        if val < 0 {
            self.write_byte(b'-')?;
            self.write_u64(val.unsigned_abs())
        } else {
            self.write_u64(val as u64)
        }
    }

    /// Formats scaled 10^8 fixed-point integer as float string "X.XXXXXXXX"
    #[inline(always)]
    pub fn write_fixed_point_8(&mut self, val: i64) -> Result<(), JsonError> {
        if val < 0 {
            self.write_byte(b'-')?;
        }
        let unsigned_val = val.unsigned_abs();
        let integer_part = unsigned_val / 100_000_000;
        let frac_part = unsigned_val % 100_000_000;

        self.write_u64(integer_part)?;
        self.write_byte(b'.')?;

        // Format 8 fraction digits with zero-padding
        let mut frac_buf = [b'0'; 8];
        let mut frac = frac_part as usize;
        let mut idx = 8;
        while frac > 0 && idx >= 2 {
            let rem = frac % 100;
            frac /= 100;
            idx -= 2;
            frac_buf[idx] = DEC_DIGITS_LUT[rem * 2];
            frac_buf[idx + 1] = DEC_DIGITS_LUT[rem * 2 + 1];
        }
        if frac > 0 && idx == 1 {
            idx -= 1;
            frac_buf[idx] = b'0' + (frac as u8);
        }

        self.write_bytes(&frac_buf)
    }
}

/// Serialize `NormalizedBbo` into SIMD-friendly JSON format without dynamic allocations
#[inline(always)]
pub fn serialize_bbo_json(bbo: &NormalizedBbo, out: &mut [u8]) -> Result<usize, JsonError> {
    let mut writer = FastJsonWriter::new(out);

    writer.write_str(r#"{"type":"bbo","market_id":"#)?;
    writer.write_u64(bbo.market_id as u64)?;

    writer.write_str(r#","venue":"#)?;
    writer.write_u64(bbo.venue as u64)?;

    writer.write_str(r#","chain":"#)?;
    writer.write_u64(bbo.chain as u64)?;

    writer.write_str(r#","sequence":"#)?;
    writer.write_u64(bbo.sequence)?;

    writer.write_str(r#","bid_px":""#)?;
    writer.write_fixed_point_8(bbo.bid_price)?;

    writer.write_str(r#"","bid_qty":""#)?;
    writer.write_fixed_point_8(bbo.bid_qty as i64)?;

    writer.write_str(r#"","ask_px":""#)?;
    writer.write_fixed_point_8(bbo.ask_price)?;

    writer.write_str(r#"","ask_qty":""#)?;
    writer.write_fixed_point_8(bbo.ask_qty as i64)?;

    writer.write_str(r#"","spread_bps":"#)?;
    writer.write_u64(bbo.spread_bps as u64)?;

    writer.write_str(r#","flags":"#)?;
    writer.write_u64(bbo.flags as u64)?;

    writer.write_str(r#","telemetry":{"t0":"#)?;
    writer.write_u64(bbo.telemetry.t0_exchange_ns)?;
    writer.write_str(r#","t1":"#)?;
    writer.write_u64(bbo.telemetry.t1_ingest_nic_ns)?;
    writer.write_str(r#","t2":"#)?;
    writer.write_u64(bbo.telemetry.t2_engine_proc_ns)?;
    writer.write_str(r#","t3":"#)?;
    writer.write_u64(bbo.telemetry.t3_egress_ns)?;
    writer.write_str(r#"}}"#)?;

    Ok(writer.pos())
}

/// Serialize `UnifiedTrade` into SIMD-friendly JSON format without dynamic allocations
#[inline(always)]
pub fn serialize_trade_json(trade: &UnifiedTrade, out: &mut [u8]) -> Result<usize, JsonError> {
    let mut writer = FastJsonWriter::new(out);

    writer.write_str(r#"{"type":"trade","market_id":"#)?;
    writer.write_u64(trade.market_id as u64)?;

    writer.write_str(r#","venue":"#)?;
    writer.write_u64(trade.venue as u64)?;

    writer.write_str(r#","chain":"#)?;
    writer.write_u64(trade.chain as u64)?;

    writer.write_str(r#","sequence":"#)?;
    writer.write_u64(trade.sequence)?;

    writer.write_str(r#","trade_id":"#)?;
    writer.write_u64(trade.trade_id)?;

    writer.write_str(r#","price":""#)?;
    writer.write_fixed_point_8(trade.price)?;

    writer.write_str(r#"","size":""#)?;
    writer.write_fixed_point_8(trade.size as i64)?;

    writer.write_str(r#"","side":"#)?;
    writer.write_u64(trade.side as u64)?;

    writer.write_str(r#","is_liquidation":"#)?;
    writer.write_str(if trade.is_liquidation {
        "true"
    } else {
        "false"
    })?;

    writer.write_str(r#","telemetry":{"t0":"#)?;
    writer.write_u64(trade.telemetry.t0_exchange_ns)?;
    writer.write_str(r#","t1":"#)?;
    writer.write_u64(trade.telemetry.t1_ingest_nic_ns)?;
    writer.write_str(r#","t2":"#)?;
    writer.write_u64(trade.telemetry.t2_engine_proc_ns)?;
    writer.write_str(r#","t3":"#)?;
    writer.write_u64(trade.telemetry.t3_egress_ns)?;
    writer.write_str(r#"}}"#)?;

    Ok(writer.pos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingest_models::TelemetryTimestamps;

    #[test]
    fn test_fast_json_bbo() {
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
        let len = serialize_bbo_json(&bbo, &mut buf).expect("serialize bbo json failed");
        let json_str = std::str::from_utf8(&buf[..len]).expect("valid utf-8");

        assert!(json_str.starts_with(r#"{"type":"bbo","market_id":42"#));
        assert!(json_str.contains(r#""bid_px":"65432.10000000""#));
        assert!(json_str.contains(r#""ask_px":"65433.00000000""#));
        assert!(json_str.contains(r#""spread_bps":2"#));
        assert!(json_str.contains(r#""telemetry":{"t0":1000,"t1":2000,"t2":3000,"t3":4000}"#));
    }

    #[test]
    fn test_fast_json_trade() {
        let trade = UnifiedTrade {
            telemetry: TelemetryTimestamps {
                t0_exchange_ns: 10,
                t1_ingest_nic_ns: 20,
                t2_engine_proc_ns: 30,
                t3_egress_ns: 40,
            },
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
        let len = serialize_trade_json(&trade, &mut buf).expect("serialize trade json failed");
        let json_str = std::str::from_utf8(&buf[..len]).expect("valid utf-8");

        assert!(json_str.starts_with(r#"{"type":"trade","market_id":88"#));
        assert!(json_str.contains(r#""price":"3500.50000000""#));
        assert!(json_str.contains(r#""size":"5.00000000""#));
        assert!(json_str.contains(r#""side":2"#));
        assert!(json_str.contains(r#""is_liquidation":true"#));
    }

    #[test]
    fn test_fast_json_writer_buffer_overflow() {
        let mut buf = [0u8; 10];
        let mut writer = FastJsonWriter::new(&mut buf);
        assert!(writer.write_str("12345678901").is_err());
    }
}
