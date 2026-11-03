//! Offline tests for `Session`. `coinbase_ticker_btc_eth_recorded.jsonl` is a real capture of
//! the public Coinbase Exchange feed (subscribe to ticker + heartbeat for BTC-USD, ETH-USD,
//! ~12 s, one JSON message per line, recorded 2026-11-03). Failure cases are built by editing
//! that recording in the test (dropping or duplicating lines), so they are synthetic and say so.

use live_feed::session::{Action, Event, Reason, Session};

const RECORDED: &str = include_str!("fixtures/coinbase_ticker_btc_eth_recorded.jsonl");

fn lines() -> Vec<&'static str> {
    RECORDED.lines().filter(|l| !l.trim().is_empty()).collect()
}

fn feed(session: &mut Session, msgs: &[&str]) -> (Vec<Event>, Vec<Action>) {
    let mut events = Vec::new();
    let mut actions = Vec::new();
    for (i, m) in msgs.iter().enumerate() {
        if let Some(a) = session.on_message(m.as_bytes(), 1_000 + i as u64, i as u64, &mut events) {
            actions.push(a);
        }
    }
    (events, actions)
}

fn session() -> Session {
    let mut s = Session::new(&["BTC-USD", "ETH-USD"], 10_000);
    s.on_connected(0);
    s
}

#[test]
fn recorded_stream_is_clean() {
    let msgs = lines();
    let mut s = session();
    let (events, actions) = feed(&mut s, &msgs);
    assert!(
        actions.is_empty(),
        "false positive on real data: {actions:?}"
    );
    assert_eq!(s.stats.malformed, 0);
    assert_eq!(s.stats.unknown_product, 0);
    assert!(s.stats.tickers > 100);
    assert!(s.stats.heartbeats > 10);
    // Every ticker yields one BBO; trades are deduplicated but on a clean stream all are new.
    let bbos = events.iter().filter(|e| matches!(e, Event::Bbo(_))).count();
    let trades = events
        .iter()
        .filter(|e| matches!(e, Event::Trade(_)))
        .count();
    assert_eq!(bbos as u64, s.stats.tickers);
    assert_eq!(trades, bbos);
}

#[test]
fn events_carry_sane_values() {
    let msgs = lines();
    let mut s = session();
    let (events, _) = feed(&mut s, &msgs);
    for e in &events {
        if let Event::Bbo(b) = e {
            assert!(b.bid_price > 0 && b.ask_price >= b.bid_price);
            assert!(b.telemetry.t0_exchange_ns > 1_700_000_000_000_000_000);
            assert!(b.market_id == 1 || b.market_id == 2);
        }
    }
}

#[test]
fn dropped_ticker_triggers_trade_id_gap() {
    let msgs = lines();
    // Remove one BTC ticker from the middle of the stream.
    let victim = msgs
        .iter()
        .enumerate()
        .filter(|(_, m)| m.contains("\"type\":\"ticker\"") && m.contains("\"BTC-USD\""))
        .nth(20)
        .map(|(i, _)| i)
        .unwrap();
    let mut edited = msgs.clone();
    edited.remove(victim);
    let mut s = session();
    let (_, actions) = feed(&mut s, &edited);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::Resubscribe(Reason::TradeIdGap { .. }))),
        "{actions:?}"
    );
    assert_eq!(s.stats.trade_id_gaps, 1);
}

#[test]
fn duplicated_ticker_triggers_sequence_regression() {
    let msgs = lines();
    let idx = msgs
        .iter()
        .position(|m| m.contains("\"type\":\"ticker\"") && m.contains("\"BTC-USD\""))
        .unwrap()
        + 5;
    // Find a BTC ticker at/after idx and repeat it right after itself.
    let idx = (idx..msgs.len())
        .find(|&i| msgs[i].contains("\"type\":\"ticker\"") && msgs[i].contains("\"BTC-USD\""))
        .unwrap();
    let mut edited = msgs.clone();
    edited.insert(idx + 1, msgs[idx]);
    let mut s = session();
    let (_, actions) = feed(&mut s, &edited);
    assert!(actions
        .iter()
        .any(|a| matches!(a, Action::Resubscribe(Reason::SequenceRegression { .. }))));
}

#[test]
fn heartbeat_ahead_of_ticker_detects_lost_tail() {
    let msgs = lines();
    // Cut the stream right before the last BTC ticker that precedes a BTC heartbeat, so the
    // heartbeat reports a newer trade than we have seen.
    let mut last_ticker = None;
    let mut cut = None;
    for (i, m) in msgs.iter().enumerate() {
        if m.contains("\"BTC-USD\"") && m.contains("\"type\":\"ticker\"") {
            last_ticker = Some(i);
        }
        if m.contains("\"BTC-USD\"")
            && m.contains("\"type\":\"heartbeat\"")
            && last_ticker.is_some()
        {
            cut = last_ticker;
            if i > msgs.len() / 2 {
                break;
            }
        }
    }
    let cut = cut.unwrap();
    let mut edited = msgs.clone();
    edited.remove(cut);
    let mut s = session();
    let (_, actions) = feed(&mut s, &edited);
    // Either the next ticker (gap) or the heartbeat (behind) must notice.
    assert!(!actions.is_empty());
}

#[test]
fn snapshot_after_resubscribe_is_not_a_gap_and_not_a_duplicate_trade() {
    let msgs = lines();
    let first_btc = msgs
        .iter()
        .position(|m| m.contains("\"type\":\"ticker\"") && m.contains("\"BTC-USD\""))
        .unwrap();
    let mut s = session();
    let mut events = Vec::new();
    assert!(s
        .on_message(msgs[first_btc].as_bytes(), 1, 1, &mut events)
        .is_none());
    let before = events.len();
    // Runner resubscribes; Coinbase replies with a snapshot repeating the same ticker.
    s.note_action_taken(Action::Resubscribe(Reason::Stale));
    assert!(s
        .on_message(msgs[first_btc].as_bytes(), 2, 2, &mut events)
        .is_none());
    // New BBO is emitted, the repeated trade is not.
    assert_eq!(events.len(), before + 1);
    assert!(matches!(events.last(), Some(Event::Bbo(_))));
    assert_eq!(s.stats.resubscribes, 1);
}

#[test]
fn silence_triggers_reconnect() {
    let s = session();
    assert_eq!(s.on_tick(5_000), None);
    assert_eq!(s.on_tick(10_001), Some(Action::Reconnect(Reason::Stale)));
}

#[test]
fn malformed_and_unknown_messages_do_not_panic_or_act() {
    let mut s = session();
    let mut ev = Vec::new();
    for m in [
        "",
        "not json",
        "{}",
        r#"{"type":"ticker"}"#,
        r#"{"type":"ticker","product_id":"DOGE-USD","sequence":1}"#,
        r#"{"type":"subscriptions","channels":[]}"#,
    ] {
        assert_eq!(s.on_message(m.as_bytes(), 1, 1, &mut ev), None, "{m}");
    }
    assert!(ev.is_empty());
    assert!(s.stats.malformed >= 2);
    assert_eq!(s.stats.unknown_product, 1);
}

#[test]
fn server_error_requests_reconnect() {
    let mut s = session();
    let mut ev = Vec::new();
    let a = s.on_message(
        br#"{"type":"error","message":"Failed to subscribe"}"#,
        1,
        1,
        &mut ev,
    );
    assert_eq!(a, Some(Action::Reconnect(Reason::ServerError)));
}
