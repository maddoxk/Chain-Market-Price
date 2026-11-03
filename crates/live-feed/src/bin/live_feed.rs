//! Live Coinbase Exchange ingest: WebSocket -> `Session` -> SPSC ring -> SBE encode.
//!
//! ```text
//! cargo run --release -p live-feed --bin live_feed -- --products BTC-USD,ETH-USD --seconds 30
//! ```
//!
//! No API key is used: the `ticker` and `heartbeat` channels are public. All connection
//! handling (backoff, ping/pong, watchdog, resubscribe) lives here; all message-level
//! decisions live in `live_feed::session::Session`, which is tested offline.

use std::error::Error;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use core_engine::SpscRingBuffer;
use distribution::{encode_bbo_sbe, TOTAL_SBE_BBO_LEN};
use futures_util::{SinkExt, StreamExt};
use ingest_models::NormalizedBbo;
use live_feed::session::{Action, Event, Reason, Session};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const DEFAULT_URL: &str = "wss://ws-feed.exchange.coinbase.com";
/// Coinbase sends a heartbeat per product roughly every second; 10 s of silence is a dead link.
const STALE_AFTER_MS: u64 = 10_000;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

struct Args {
    url: String,
    products: Vec<String>,
    seconds: u64,
    min_messages: u64,
}

fn parse_args() -> Args {
    let mut a = Args {
        url: DEFAULT_URL.to_string(),
        products: vec!["BTC-USD".into(), "ETH-USD".into()],
        seconds: 30,
        min_messages: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let val = it.next().unwrap_or_default();
        match flag.as_str() {
            "--url" => a.url = val,
            "--products" => a.products = val.split(',').map(|s| s.trim().to_string()).collect(),
            "--seconds" => a.seconds = val.parse().unwrap_or(30),
            "--min-messages" => a.min_messages = val.parse().unwrap_or(0),
            other => {
                eprintln!(
                    "unknown flag {other}; usage: --url U --products A,B --seconds N (0 = forever) --min-messages N"
                );
                std::process::exit(2);
            }
        }
    }
    a
}

fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn sub_msg(kind: &str, products: &[String]) -> String {
    let ids: Vec<String> = products.iter().map(|p| format!("\"{p}\"")).collect();
    format!(
        "{{\"type\":\"{kind}\",\"product_ids\":[{}],\"channels\":[\"ticker\",\"heartbeat\"]}}",
        ids.join(",")
    )
}

/// Downstream of the session: hand the BBO across an SPSC ring and SBE-encode it, timing
/// only our own processing (not the network).
struct Sink {
    ring: SpscRingBuffer<NormalizedBbo, 1024>,
    buf: [u8; TOTAL_SBE_BBO_LEN],
    bbos: u64,
    trades: u64,
    process_ns: Vec<u64>,
    exchange_to_local_ns: Vec<i64>,
}

impl Sink {
    fn new() -> Self {
        Sink {
            ring: SpscRingBuffer::new(),
            buf: [0u8; TOTAL_SBE_BBO_LEN],
            bbos: 0,
            trades: 0,
            process_ns: Vec::new(),
            exchange_to_local_ns: Vec::new(),
        }
    }

    fn handle(&mut self, ev: &Event) {
        match ev {
            Event::Bbo(b) => {
                let t = Instant::now();
                // Single-threaded here, so push-then-pop just exercises the ring API.
                let _ = self.ring.try_push(*b);
                if let Some(x) = self.ring.try_pop() {
                    let _ = encode_bbo_sbe(&x, &mut self.buf);
                }
                if self.process_ns.len() < 1_000_000 {
                    self.process_ns.push(t.elapsed().as_nanos() as u64);
                    self.exchange_to_local_ns.push(
                        b.telemetry.t1_ingest_nic_ns as i64 - b.telemetry.t0_exchange_ns as i64,
                    );
                }
                self.bbos += 1;
            }
            Event::Trade(_) => self.trades += 1,
        }
    }
}

enum Outcome {
    Finished,
    Reconnect(Reason),
}

async fn run_connection(
    args: &Args,
    session: &mut Session,
    sink: &mut Sink,
    start: Instant,
    deadline: Option<Instant>,
    got_message: &mut bool,
) -> Result<Outcome, Box<dyn Error>> {
    let (mut ws, _resp) =
        tokio::time::timeout(Duration::from_secs(10), connect_async(args.url.as_str()))
            .await
            .map_err(|_| "connect timed out")??;
    println!("connected to {}", args.url);
    session.on_connected(start.elapsed().as_millis() as u64);
    ws.send(Message::Text(sub_msg("subscribe", &args.products)))
        .await?;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut events = Vec::with_capacity(4);

    loop {
        let sleep_for = deadline
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(3600));
        tokio::select! {
            _ = tokio::time::sleep(sleep_for), if deadline.is_some() => return Ok(Outcome::Finished),
            _ = tokio::signal::ctrl_c() => return Ok(Outcome::Finished),
            _ = tick.tick() => {
                if let Some(action) = session.on_tick(start.elapsed().as_millis() as u64) {
                    session.note_action_taken(action);
                    if let Action::Reconnect(r) = action { return Ok(Outcome::Reconnect(r)); }
                }
            }
            msg = ws.next() => {
                let msg = match msg {
                    None => return Ok(Outcome::Reconnect(Reason::Disconnected)),
                    Some(m) => m?,
                };
                match msg {
                    Message::Text(text) => {
                        *got_message = true;
                        events.clear();
                        let action = session.on_message(
                            text.as_bytes(), wall_ns(), start.elapsed().as_millis() as u64, &mut events);
                        for ev in &events { sink.handle(ev); }
                        match action {
                            Some(Action::Resubscribe(r)) => {
                                println!("resubscribing: {r:?}");
                                session.note_action_taken(Action::Resubscribe(r));
                                ws.send(Message::Text(sub_msg("unsubscribe", &args.products))).await?;
                                ws.send(Message::Text(sub_msg("subscribe", &args.products))).await?;
                            }
                            Some(Action::Reconnect(r)) => {
                                session.note_action_taken(Action::Reconnect(r));
                                return Ok(Outcome::Reconnect(r));
                            }
                            None => {}
                        }
                    }
                    Message::Ping(p) => ws.send(Message::Pong(p)).await?,
                    Message::Close(_) => return Ok(Outcome::Reconnect(Reason::Disconnected)),
                    _ => {}
                }
            }
        }
    }
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        0
    } else {
        sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // tokio-tungstenite needs a process-wide rustls crypto provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = parse_args();
    let product_refs: Vec<&str> = args.products.iter().map(|s| s.as_str()).collect();
    let mut session = Session::new(&product_refs, STALE_AFTER_MS);
    let mut sink = Sink::new();
    let start = Instant::now();
    let deadline = if args.seconds == 0 {
        None
    } else {
        Some(start + Duration::from_secs(args.seconds))
    };
    let mut backoff = Duration::from_secs(1);

    loop {
        let mut got_message = false;
        let res = run_connection(
            &args,
            &mut session,
            &mut sink,
            start,
            deadline,
            &mut got_message,
        )
        .await;
        if got_message {
            backoff = Duration::from_secs(1);
        }
        match res {
            Ok(Outcome::Finished) => break,
            Ok(Outcome::Reconnect(r)) => eprintln!("reconnecting after {r:?} in {backoff:?}"),
            Err(e) => eprintln!("connection error: {e}; retrying in {backoff:?}"),
        }
        if deadline.is_some_and(|d| Instant::now() + backoff >= d) {
            break;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }

    let s = &session.stats;
    let mut p = sink.process_ns.clone();
    p.sort_unstable();
    let mut d = sink.exchange_to_local_ns.clone();
    d.sort_unstable();
    println!("--- summary ({:.1}s) ---", start.elapsed().as_secs_f64());
    println!("{s:#?}");
    println!("bbo events {} trade events {}", sink.bbos, sink.trades);
    println!(
        "local processing per BBO (ring hand-off + SBE encode, ns): p50 {} p99 {} (n={})",
        pct(&p, 50),
        pct(&p, 99),
        p.len()
    );
    if !d.is_empty() {
        println!(
            "exchange timestamp -> local receive (ms; includes network and clock skew): p50 {:.1}",
            d[d.len() / 2] as f64 / 1e6
        );
    }
    if s.messages < args.min_messages {
        eprintln!(
            "FAILED: got {} messages, wanted {}",
            s.messages, args.min_messages
        );
        std::process::exit(1);
    }
}
