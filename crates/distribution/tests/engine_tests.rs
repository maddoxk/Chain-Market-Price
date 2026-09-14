use distribution::{
    decode_bbo_sbe, decode_trade_sbe, decode_ws_frame_header, encode_bbo_sbe, encode_trade_sbe,
    encode_ws_frame, serialize_bbo_json, serialize_trade_json, ClientFilterPredicate,
    ClientSession, InvertedTopicRouter, TopicKey, WebSocketServerEngine, WireProtocol, WsOpcode,
    STREAM_TYPE_BBO, STREAM_TYPE_TRADE, TOTAL_SBE_BBO_LEN, TOTAL_SBE_TRADE_LEN,
};
use ingest_models::{NormalizedBbo, TelemetryTimestamps, UnifiedTrade};

#[test]
fn test_end_to_end_sbe_bbo_serialization_and_deserialization() {
    let bbo = NormalizedBbo {
        telemetry: TelemetryTimestamps {
            t0_exchange_ns: 1_700_000_000_000_000_000,
            t1_ingest_nic_ns: 1_700_000_000_000_000_100,
            t2_engine_proc_ns: 1_700_000_000_000_000_250,
            t3_egress_ns: 1_700_000_000_000_000_300,
        },
        market_id: 101,
        venue: 1, // Binance
        chain: 1, // Ethereum
        sequence: 999_999,
        bid_price: 65_500_00000000,
        bid_qty: 12_50000000,
        ask_price: 65_501_00000000,
        ask_qty: 18_00000000,
        spread_bps: 1,
        flags: 0x01,
    };

    let mut buf = [0u8; 128];
    let written = encode_bbo_sbe(&bbo, &mut buf).expect("SBE encoding failed");
    assert_eq!(written, TOTAL_SBE_BBO_LEN);

    let decoded = decode_bbo_sbe(&buf[..written]).expect("SBE decoding failed");
    assert_eq!(bbo.market_id, decoded.market_id);
    assert_eq!(bbo.venue, decoded.venue);
    assert_eq!(bbo.chain, decoded.chain);
    assert_eq!(bbo.sequence, decoded.sequence);
    assert_eq!(bbo.bid_price, decoded.bid_price);
    assert_eq!(bbo.bid_qty, decoded.bid_qty);
    assert_eq!(bbo.ask_price, decoded.ask_price);
    assert_eq!(bbo.ask_qty, decoded.ask_qty);
    assert_eq!(bbo.spread_bps, decoded.spread_bps);
    assert_eq!(bbo.flags, decoded.flags);
    assert_eq!(bbo.telemetry, decoded.telemetry);
}

#[test]
fn test_end_to_end_sbe_trade_serialization_and_deserialization() {
    let trade = UnifiedTrade {
        telemetry: TelemetryTimestamps {
            t0_exchange_ns: 100,
            t1_ingest_nic_ns: 200,
            t2_engine_proc_ns: 300,
            t3_egress_ns: 400,
        },
        market_id: 202,
        venue: 102, // UniswapV3
        chain: 42161, // Arbitrum
        sequence: 555_555,
        trade_id: 1_234_567,
        price: 3_200_50000000,
        size: 5_00000000,
        side: 2, // Sell
        is_liquidation: false,
    };

    let mut buf = [0u8; 128];
    let written = encode_trade_sbe(&trade, &mut buf).expect("SBE trade encoding failed");
    assert_eq!(written, TOTAL_SBE_TRADE_LEN);

    let decoded = decode_trade_sbe(&buf[..written]).expect("SBE trade decoding failed");
    assert_eq!(trade.market_id, decoded.market_id);
    assert_eq!(trade.venue, decoded.venue);
    assert_eq!(trade.chain, decoded.chain);
    assert_eq!(trade.sequence, decoded.sequence);
    assert_eq!(trade.trade_id, decoded.trade_id);
    assert_eq!(trade.price, decoded.price);
    assert_eq!(trade.size, decoded.size);
    assert_eq!(trade.side, decoded.side);
    assert_eq!(trade.is_liquidation, decoded.is_liquidation);
}

#[test]
fn test_fast_json_bbo_validation() {
    let bbo = NormalizedBbo {
        telemetry: TelemetryTimestamps {
            t0_exchange_ns: 1,
            t1_ingest_nic_ns: 2,
            t2_engine_proc_ns: 3,
            t3_egress_ns: 4,
        },
        market_id: 42,
        venue: 1,
        chain: 1,
        sequence: 1001,
        bid_price: 12345_67000000,
        bid_qty: 10_00000000,
        ask_price: 12346_00000000,
        ask_qty: 25_00000000,
        spread_bps: 3,
        flags: 0,
    };

    let mut buf = [0u8; 512];
    let len = serialize_bbo_json(&bbo, &mut buf).expect("JSON serialization failed");
    let json_str = std::str::from_utf8(&buf[..len]).expect("Invalid UTF-8");

    assert!(json_str.contains(r#""type":"bbo""#));
    assert!(json_str.contains(r#""market_id":42"#));
    assert!(json_str.contains(r#""bid_px":"12345.67000000""#));
    assert!(json_str.contains(r#""bid_qty":"10.00000000""#));
    assert!(json_str.contains(r#""ask_px":"12346.00000000""#));
    assert!(json_str.contains(r#""ask_qty":"25.00000000""#));
}

#[test]
fn test_fast_json_trade_validation() {
    let trade = UnifiedTrade {
        telemetry: TelemetryTimestamps::default(),
        market_id: 77,
        venue: 3,
        chain: 8453,
        sequence: 2002,
        trade_id: 8888,
        price: 99_99000000,
        size: 1_25000000,
        side: 1,
        is_liquidation: true,
    };

    let mut buf = [0u8; 512];
    let len = serialize_trade_json(&trade, &mut buf).expect("JSON trade failed");
    let json_str = std::str::from_utf8(&buf[..len]).expect("Invalid UTF-8");

    assert!(json_str.contains(r#""type":"trade""#));
    assert!(json_str.contains(r#""market_id":77"#));
    assert!(json_str.contains(r#""price":"99.99000000""#));
    assert!(json_str.contains(r#""size":"1.25000000""#));
    assert!(json_str.contains(r#""side":1"#));
    assert!(json_str.contains(r#""is_liquidation":true"#));
}

#[test]
fn test_inverted_topic_router_multi_client_fanout() {
    let router = InvertedTopicRouter::new();

    // Register 10 SBE clients and 10 JSON clients
    for i in 0..10 {
        let session = ClientSession::new(i, WireProtocol::Sbe, 10_000, 100, 0);
        router.register_client(session).unwrap();
    }
    for i in 10..20 {
        let session = ClientSession::new(i, WireProtocol::Json, 10_000, 100, 0);
        router.register_client(session).unwrap();
    }

    let bbo_topic = TopicKey::new(1, 100, STREAM_TYPE_BBO, 0);

    // Subscribe all clients
    for i in 0..20 {
        router.subscribe(i, bbo_topic).unwrap();
    }

    let bbo = NormalizedBbo {
        market_id: 100,
        venue: 1,
        chain: 1,
        sequence: 1,
        bid_price: 60_000_00000000,
        bid_qty: 1_00000000,
        ask_price: 60_001_00000000,
        ask_qty: 1_00000000,
        spread_bps: 1,
        ..Default::default()
    };

    let mut sbe_delivered = 0;
    let mut json_delivered = 0;

    let metrics = router.dispatch_bbo(&bbo, 0, |_client_id, protocol, payload| {
        match protocol {
            WireProtocol::Sbe => {
                sbe_delivered += 1;
                assert_eq!(payload.len(), TOTAL_SBE_BBO_LEN);
            }
            WireProtocol::Json => {
                json_delivered += 1;
                assert!(payload.len() > 50);
            }
        }
    });

    assert_eq!(metrics.matched_clients, 20);
    assert_eq!(metrics.delivered_clients, 20);
    assert_eq!(sbe_delivered, 10);
    assert_eq!(json_delivered, 10);
}

#[test]
fn test_predicate_filtering_min_notional_and_spread() {
    let router = InvertedTopicRouter::new();

    // Client 1: max spread 3 bps
    let client1 = ClientSession::new(1, WireProtocol::Json, 1000, 10, 0).with_filter(
        ClientFilterPredicate {
            max_spread_bps: 3,
            ..Default::default()
        },
    );
    // Client 2: max spread 10 bps
    let client2 = ClientSession::new(2, WireProtocol::Json, 1000, 10, 0).with_filter(
        ClientFilterPredicate {
            max_spread_bps: 10,
            ..Default::default()
        },
    );

    router.register_client(client1).unwrap();
    router.register_client(client2).unwrap();

    let topic = TopicKey::new(2, 50, STREAM_TYPE_BBO, 0);
    router.subscribe(1, topic).unwrap();
    router.subscribe(2, topic).unwrap();

    // BBO with 5 bps spread -> Client 1 filters out (max 3), Client 2 accepts (max 10)
    let bbo = NormalizedBbo {
        market_id: 50,
        venue: 2,
        spread_bps: 5,
        ..Default::default()
    };

    let mut delivered = Vec::new();
    let metrics = router.dispatch_bbo(&bbo, 0, |id, _, _| delivered.push(id));

    assert_eq!(metrics.matched_clients, 2);
    assert_eq!(metrics.delivered_clients, 1);
    assert_eq!(metrics.filtered_clients, 1);
    assert_eq!(delivered, vec![2]);
}

#[test]
fn test_ws_server_complete_flow() {
    let ws_server = WebSocketServerEngine::new();

    // Client connects
    ws_server.handle_client_connect(100, WireProtocol::Json, 1000, 50, 0).unwrap();

    // Client sends subscription command
    let sub_cmd = br#"{"action":"subscribe","venue":1,"market_id":500,"stream_type":1,"flags":0}"#;
    let mask = [0x11, 0x22, 0x33, 0x44];
    let mut masked_payload = sub_cmd.to_vec();
    for (i, b) in masked_payload.iter_mut().enumerate() {
        *b ^= mask[i % 4];
    }
    let mut frame = vec![0x81, 0x80 | (sub_cmd.len() as u8)];
    frame.extend_from_slice(&mask);
    frame.extend_from_slice(&masked_payload);

    let mut response_buf = Vec::new();
    ws_server
        .handle_inbound_frame(100, &frame, 0, |resp| response_buf.extend_from_slice(resp))
        .unwrap();

    let resp_hdr = decode_ws_frame_header(&response_buf).unwrap();
    assert_eq!(resp_hdr.opcode, WsOpcode::Text);

    // Broadcast BBO event
    let bbo = NormalizedBbo {
        market_id: 500,
        venue: 1,
        chain: 1,
        sequence: 1,
        bid_price: 100_00000000,
        bid_qty: 1_00000000,
        ask_price: 100_01000000,
        ask_qty: 1_00000000,
        spread_bps: 1,
        ..Default::default()
    };

    let mut sent_frames = Vec::new();
    let metrics = ws_server.broadcast_bbo(&bbo, 0, |client_id, frame_bytes| {
        sent_frames.push((client_id, frame_bytes.to_vec()));
    });

    assert_eq!(metrics.matched_clients, 1);
    assert_eq!(metrics.delivered_clients, 1);
    assert_eq!(sent_frames.len(), 1);
    assert_eq!(sent_frames[0].0, 100);

    let header = decode_ws_frame_header(&sent_frames[0].1).unwrap();
    assert_eq!(header.opcode, WsOpcode::Text);
}
