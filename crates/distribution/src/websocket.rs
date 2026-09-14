//! # High-Performance Zero-Copy WebSocket Protocol & Dispatch Engine
//!
//! Implements RFC 6455 framing with vectorized 64-bit XOR payload unmasking,
//! connection lifecycle, subscription command handling, and ping/pong keepalive.

use ingest_models::{NormalizedBbo, UnifiedTrade};
use std::sync::Arc;

use crate::fast_json::FastJsonWriter;
use crate::router::{DispatchMetrics, InvertedTopicRouter};
use crate::session::{ClientSession, WireProtocol};
use crate::{ClientFilterPredicate, TopicKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOpcode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl WsOpcode {
    #[inline(always)]
    pub fn from_u8(val: u8) -> Option<Self> {
        match val & 0x0F {
            0x0 => Some(WsOpcode::Continuation),
            0x1 => Some(WsOpcode::Text),
            0x2 => Some(WsOpcode::Binary),
            0x8 => Some(WsOpcode::Close),
            0x9 => Some(WsOpcode::Ping),
            0xA => Some(WsOpcode::Pong),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsError {
    BufferTooSmall { required: usize, provided: usize },
    IncompleteFrame,
    InvalidOpcode(u8),
    MaskingRequired,
    InvalidPayloadLength,
    InvalidCommand,
    ClientNotFound(u32),
}

/// Parsed WebSocket Frame Header
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsFrameHeader {
    pub fin: bool,
    pub opcode: WsOpcode,
    pub masked: bool,
    pub payload_len: usize,
    pub mask_key: [u8; 4],
    pub header_len: usize,
}

/// Vectorized 64-bit unmasking routine for incoming client payloads
#[inline(always)]
pub fn unmask_payload(payload: &mut [u8], mask: [u8; 4]) {
    let mask_u32 = u32::from_ne_bytes(mask);
    let mask_u64 = ((mask_u32 as u64) << 32) | (mask_u32 as u64);

    let payload_len = payload.len();
    let mut chunks = payload.chunks_exact_mut(8);
    for chunk in chunks.by_ref() {
        let mut val_bytes = [0u8; 8];
        val_bytes.copy_from_slice(chunk);
        let val = u64::from_ne_bytes(val_bytes);
        chunk.copy_from_slice(&(val ^ mask_u64).to_ne_bytes());
    }

    let rem = chunks.into_remainder();
    let offset = payload_len - rem.len();
    for (i, byte) in rem.iter_mut().enumerate() {
        *byte ^= mask[(offset + i) % 4];
    }
}

/// Decode RFC 6455 frame header from incoming buffer
pub fn decode_ws_frame_header(buf: &[u8]) -> Result<WsFrameHeader, WsError> {
    if buf.len() < 2 {
        return Err(WsError::IncompleteFrame);
    }

    let b0 = buf[0];
    let fin = (b0 & 0x80) != 0;
    let opcode = WsOpcode::from_u8(b0).ok_or(WsError::InvalidOpcode(b0 & 0x0F))?;

    let b1 = buf[1];
    let masked = (b1 & 0x80) != 0;
    let len_byte = b1 & 0x7F;

    let mut header_len = 2;
    let payload_len = match len_byte {
        126 => {
            if buf.len() < 4 {
                return Err(WsError::IncompleteFrame);
            }
            header_len += 2;
            u16::from_be_bytes([buf[2], buf[3]]) as usize
        }
        127 => {
            if buf.len() < 10 {
                return Err(WsError::IncompleteFrame);
            }
            header_len += 8;
            u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]) as usize
        }
        val => val as usize,
    };

    let mut mask_key = [0u8; 4];
    if masked {
        if buf.len() < header_len + 4 {
            return Err(WsError::IncompleteFrame);
        }
        mask_key.copy_from_slice(&buf[header_len..header_len + 4]);
        header_len += 4;
    }

    Ok(WsFrameHeader {
        fin,
        opcode,
        masked,
        payload_len,
        mask_key,
        header_len,
    })
}

/// Encode server-to-client unmasked RFC 6455 WebSocket frame into destination buffer
#[inline(always)]
pub fn encode_ws_frame(opcode: WsOpcode, payload: &[u8], out: &mut [u8]) -> Result<usize, WsError> {
    let payload_len = payload.len();
    let header_len = if payload_len < 126 {
        2
    } else if payload_len <= 0xFFFF {
        4
    } else {
        10
    };

    let total_len = header_len + payload_len;
    if out.len() < total_len {
        return Err(WsError::BufferTooSmall {
            required: total_len,
            provided: out.len(),
        });
    }

    // Byte 0: FIN bit + Opcode
    out[0] = 0x80 | (opcode as u8);

    // Bytes 1..N: Payload length (unmasked for server -> client)
    if payload_len < 126 {
        out[1] = payload_len as u8;
    } else if payload_len <= 0xFFFF {
        out[1] = 126;
        out[2..4].copy_from_slice(&(payload_len as u16).to_be_bytes());
    } else {
        out[1] = 127;
        out[2..10].copy_from_slice(&(payload_len as u64).to_be_bytes());
    }

    // Payload
    out[header_len..total_len].copy_from_slice(payload);
    Ok(total_len)
}

/// Inbound client command representation
#[derive(Debug, PartialEq)]
pub enum ClientCommand {
    Subscribe(TopicKey),
    Unsubscribe(TopicKey),
    Filter(ClientFilterPredicate),
    Ping,
    Pong,
}

/// Lightweight zero-allocation scanner for client JSON commands
pub fn parse_client_command(json_bytes: &[u8]) -> Option<ClientCommand> {
    let s = std::str::from_utf8(json_bytes).ok()?;

    if s.contains(r#""action":"ping""#) || s.contains(r#""action": "ping""#) {
        return Some(ClientCommand::Ping);
    }
    if s.contains(r#""action":"pong""#) || s.contains(r#""action": "pong""#) {
        return Some(ClientCommand::Pong);
    }

    if s.contains(r#""action":"subscribe""#) || s.contains(r#""action": "subscribe""#) {
        let venue = extract_json_u64(s, "venue")? as u8;
        let market_id = extract_json_u64(s, "market_id")? as u32;
        let stream_type = extract_json_u64(s, "stream_type")? as u8;
        let flags = extract_json_u64(s, "flags").unwrap_or(0) as u32;
        return Some(ClientCommand::Subscribe(TopicKey::new(
            venue,
            market_id,
            stream_type,
            flags,
        )));
    }

    if s.contains(r#""action":"unsubscribe""#) || s.contains(r#""action": "unsubscribe""#) {
        let venue = extract_json_u64(s, "venue")? as u8;
        let market_id = extract_json_u64(s, "market_id")? as u32;
        let stream_type = extract_json_u64(s, "stream_type")? as u8;
        return Some(ClientCommand::Unsubscribe(TopicKey::new(
            venue,
            market_id,
            stream_type,
            0,
        )));
    }

    None
}

fn extract_json_u64(s: &str, key: &str) -> Option<u64> {
    let pattern1 = format!("\"{}\":", key);
    let pattern2 = format!("\"{}\" :", key);
    let pos = s.find(&pattern1).or_else(|| s.find(&pattern2))?;
    let start = pos + pattern1.len();
    let slice = s[start..].trim_start();
    let mut end = 0;
    for ch in slice.chars() {
        if ch.is_ascii_digit() {
            end += 1;
        } else {
            break;
        }
    }
    if end > 0 {
        slice[..end].parse::<u64>().ok()
    } else {
        None
    }
}

/// High-performance WebSocket Server Engine coordinating clients and inverted topic routing
pub struct WebSocketServerEngine {
    pub router: Arc<InvertedTopicRouter>,
}

impl Default for WebSocketServerEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketServerEngine {
    pub fn new() -> Self {
        Self {
            router: Arc::new(InvertedTopicRouter::new()),
        }
    }

    /// Register a new incoming WebSocket client connection
    pub fn handle_client_connect(
        &self,
        client_id: u32,
        protocol: WireProtocol,
        rate_per_sec: u32,
        burst_capacity: u32,
        now_ms: u64,
    ) -> Result<u32, WsError> {
        let session = ClientSession::new(client_id, protocol, rate_per_sec, burst_capacity, now_ms);
        self.router
            .register_client(session)
            .map_err(|_| WsError::ClientNotFound(client_id))
    }

    /// Handle client disconnection
    pub fn handle_client_disconnect(&self, client_id: u32) {
        let _ = self.router.unregister_client(client_id);
    }

    /// Handle raw incoming frame bytes from client
    pub fn handle_inbound_frame(
        &self,
        client_id: u32,
        frame_bytes: &[u8],
        now_ms: u64,
        mut send_response: impl FnMut(&[u8]),
    ) -> Result<(), WsError> {
        let header = decode_ws_frame_header(frame_bytes)?;
        let total_required = header.header_len + header.payload_len;
        if frame_bytes.len() < total_required {
            return Err(WsError::IncompleteFrame);
        }

        let mut payload = frame_bytes[header.header_len..total_required].to_vec();
        if header.masked {
            unmask_payload(&mut payload, header.mask_key);
        }

        match header.opcode {
            WsOpcode::Ping => {
                // RFC 6455 §5.5.2: Pong frame MUST have an identical payload to Ping
                let mut out_buf = [0u8; 256];
                let len = encode_ws_frame(WsOpcode::Pong, &payload, &mut out_buf)?;
                send_response(&out_buf[..len]);
            }
            WsOpcode::Pong => {
                // Heartbeat response
            }
            WsOpcode::Close => {
                self.handle_client_disconnect(client_id);
                let mut out_buf = [0u8; 16];
                let len = encode_ws_frame(WsOpcode::Close, &[], &mut out_buf)?;
                send_response(&out_buf[..len]);
            }
            WsOpcode::Text => {
                if let Some(cmd) = parse_client_command(&payload) {
                    match cmd {
                        ClientCommand::Subscribe(topic) => {
                            self.router
                                .subscribe(client_id, topic)
                                .map_err(|_| WsError::ClientNotFound(client_id))?;

                            // Emit subscription confirmation
                            let mut json_buf = [0u8; 128];
                            let mut writer = FastJsonWriter::new(&mut json_buf);
                            let _ = writer.write_str(r#"{"type":"subscribed","topic":"#);
                            let _ = writer.write_u64(topic.0);
                            let _ = writer.write_str(r#"}"#);
                            let json_len = writer.pos();

                            let mut frame_buf = [0u8; 256];
                            let frame_len = encode_ws_frame(
                                WsOpcode::Text,
                                &json_buf[..json_len],
                                &mut frame_buf,
                            )?;
                            send_response(&frame_buf[..frame_len]);
                        }
                        ClientCommand::Unsubscribe(topic) => {
                            self.router
                                .unsubscribe(client_id, topic)
                                .map_err(|_| WsError::ClientNotFound(client_id))?;
                        }
                        ClientCommand::Ping => {
                            let mut json_buf = [0u8; 64];
                            let mut writer = FastJsonWriter::new(&mut json_buf);
                            let _ = writer.write_str(r#"{"type":"pong","ts":"#);
                            let _ = writer.write_u64(now_ms);
                            let _ = writer.write_str(r#"}"#);
                            let json_len = writer.pos();

                            let mut frame_buf = [0u8; 128];
                            let frame_len = encode_ws_frame(
                                WsOpcode::Text,
                                &json_buf[..json_len],
                                &mut frame_buf,
                            )?;
                            send_response(&frame_buf[..frame_len]);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Broadcast NormalizedBbo wrapped in RFC 6455 WebSocket frames
    pub fn broadcast_bbo(
        &self,
        bbo: &NormalizedBbo,
        now_ms: u64,
        mut send_fn: impl FnMut(u32, &[u8]),
    ) -> DispatchMetrics {
        let mut framed_sbe = [0u8; 256];
        let mut framed_json = [0u8; 1024];

        let mut sbe_cached_len = 0;
        let mut json_cached_len = 0;

        self.router.dispatch_bbo(
            bbo,
            now_ms,
            |client_id, protocol, raw_payload| match protocol {
                WireProtocol::Sbe => {
                    if sbe_cached_len == 0 {
                        if let Ok(len) =
                            encode_ws_frame(WsOpcode::Binary, raw_payload, &mut framed_sbe)
                        {
                            sbe_cached_len = len;
                        }
                    }
                    if sbe_cached_len > 0 {
                        send_fn(client_id, &framed_sbe[..sbe_cached_len]);
                    }
                }
                WireProtocol::Json => {
                    if json_cached_len == 0 {
                        if let Ok(len) =
                            encode_ws_frame(WsOpcode::Text, raw_payload, &mut framed_json)
                        {
                            json_cached_len = len;
                        }
                    }
                    if json_cached_len > 0 {
                        send_fn(client_id, &framed_json[..json_cached_len]);
                    }
                }
            },
        )
    }

    /// Broadcast UnifiedTrade wrapped in RFC 6455 WebSocket frames
    pub fn broadcast_trade(
        &self,
        trade: &UnifiedTrade,
        notional_usd: f64,
        now_ms: u64,
        mut send_fn: impl FnMut(u32, &[u8]),
    ) -> DispatchMetrics {
        let mut framed_sbe = [0u8; 256];
        let mut framed_json = [0u8; 1024];

        let mut sbe_cached_len = 0;
        let mut json_cached_len = 0;

        self.router.dispatch_trade(
            trade,
            notional_usd,
            now_ms,
            |client_id, protocol, raw_payload| match protocol {
                WireProtocol::Sbe => {
                    if sbe_cached_len == 0 {
                        if let Ok(len) =
                            encode_ws_frame(WsOpcode::Binary, raw_payload, &mut framed_sbe)
                        {
                            sbe_cached_len = len;
                        }
                    }
                    if sbe_cached_len > 0 {
                        send_fn(client_id, &framed_sbe[..sbe_cached_len]);
                    }
                }
                WireProtocol::Json => {
                    if json_cached_len == 0 {
                        if let Ok(len) =
                            encode_ws_frame(WsOpcode::Text, raw_payload, &mut framed_json)
                        {
                            json_cached_len = len;
                        }
                    }
                    if json_cached_len > 0 {
                        send_fn(client_id, &framed_json[..json_cached_len]);
                    }
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_frame_encode_decode() {
        let payload = b"hello hft engine";
        let mut out = [0u8; 64];
        let len = encode_ws_frame(WsOpcode::Text, payload, &mut out).expect("encode failed");

        let header = decode_ws_frame_header(&out[..len]).expect("decode failed");
        assert!(header.fin);
        assert_eq!(header.opcode, WsOpcode::Text);
        assert!(!header.masked);
        assert_eq!(header.payload_len, payload.len());
        assert_eq!(&out[header.header_len..len], payload);
    }

    #[test]
    fn test_ws_vectorized_unmasking() {
        let original = b"Institutional HFT WebSocket Test Payload 1234567890!";
        let mask = [0xAA, 0xBB, 0xCC, 0xDD];

        let mut masked = original.to_vec();
        for (i, b) in masked.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }

        unmask_payload(&mut masked, mask);
        assert_eq!(&masked[..], &original[..]);
    }

    #[test]
    fn test_ws_server_ping_pong() {
        let engine = WebSocketServerEngine::new();
        engine
            .handle_client_connect(1, WireProtocol::Json, 1000, 10, 0)
            .unwrap();

        // Craft masked Ping frame from client
        let mask = [0x12, 0x34, 0x56, 0x78];
        let ping_payload = b"ping-data";
        let mut masked_payload = ping_payload.to_vec();
        for (i, b) in masked_payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }

        let mut client_frame = vec![0x89, 0x80 | (ping_payload.len() as u8)];
        client_frame.extend_from_slice(&mask);
        client_frame.extend_from_slice(&masked_payload);

        let mut response = Vec::new();
        engine
            .handle_inbound_frame(1, &client_frame, 1000, |resp| {
                response.extend_from_slice(resp);
            })
            .expect("handling ping failed");

        assert!(!response.is_empty());
        let resp_header = decode_ws_frame_header(&response).expect("decode pong header failed");
        assert_eq!(resp_header.opcode, WsOpcode::Pong);
        assert_eq!(
            &response[resp_header.header_len..resp_header.header_len + resp_header.payload_len],
            ping_payload
        );
    }

    #[test]
    fn test_ws_client_subscribe_flow() {
        let engine = WebSocketServerEngine::new();
        engine
            .handle_client_connect(10, WireProtocol::Json, 1000, 10, 0)
            .unwrap();

        let sub_cmd =
            br#"{"action":"subscribe","venue":1,"market_id":42,"stream_type":1,"flags":0}"#;
        let mask = [0x01, 0x02, 0x03, 0x04];
        let mut masked_sub = sub_cmd.to_vec();
        for (i, b) in masked_sub.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }

        let mut client_frame = vec![0x81, 0x80 | (sub_cmd.len() as u8)];
        client_frame.extend_from_slice(&mask);
        client_frame.extend_from_slice(&masked_sub);

        let mut response = Vec::new();
        engine
            .handle_inbound_frame(10, &client_frame, 1000, |resp| {
                response.extend_from_slice(resp);
            })
            .expect("handling subscribe failed");

        let resp_str = std::str::from_utf8(&response[2..]).expect("valid response utf-8");
        assert!(resp_str.contains(r#"{"type":"subscribed""#));

        // Verify that broadcast reaches the newly subscribed client
        let bbo = NormalizedBbo {
            market_id: 42,
            venue: 1,
            ..Default::default()
        };

        let mut delivered_clients = Vec::new();
        let metrics = engine.broadcast_bbo(&bbo, 1000, |client_id, _payload| {
            delivered_clients.push(client_id);
        });

        assert_eq!(metrics.matched_clients, 1);
        assert_eq!(metrics.delivered_clients, 1);
        assert_eq!(delivered_clients, vec![10]);
    }
}
