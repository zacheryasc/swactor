//! Two-process transport demo.
//!
//! Run in two terminals:
//!
//! ```bash
//! # Terminal 1 — starts the receiver (has the actor)
//! cargo run --example tcp_ping_pong --features transport -- receiver
//!
//! # Terminal 2 — sends pings across TCP
//! cargo run --example tcp_ping_pong --features transport -- sender
//! ```

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use swactor::{
    actor::{ActorAddress, ActorInterface},
    runtime::{Ctx, Runtime, RuntimeConfig},
    transport::{
        Codec, CodecRegistry, NetworkMessage, Transport, TransportRouter,
        WireEnvelope,
    },
    Error,
};

// ─── Messages ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Ping {
    value: u32,
    reply_to: ActorAddress,
}

impl NetworkMessage for Ping {
    fn type_tag() -> &'static str {
        "example::Ping"
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Pong {
    value: u32,
}

impl NetworkMessage for Pong {
    fn type_tag() -> &'static str {
        "example::Pong"
    }
}

// ─── Codec (hand-rolled, no serde needed) ───────────────────────────────────

struct ExampleCodec;

impl Codec<Ping> for ExampleCodec {
    fn encode(&self, msg: &Ping) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(&msg.value.to_be_bytes());
        buf.extend_from_slice(&msg.reply_to.0);
        Ok(buf)
    }
    fn decode(&self, bytes: &[u8]) -> Result<Ping, Error> {
        if bytes.len() < 36 {
            return Err(Error::from("Ping: short read"));
        }
        let value = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let mut addr = [0u8; 32];
        addr.copy_from_slice(&bytes[4..36]);
        Ok(Ping {
            value,
            reply_to: ActorAddress(addr),
        })
    }
}

impl Codec<Pong> for ExampleCodec {
    fn encode(&self, msg: &Pong) -> Result<Vec<u8>, Error> {
        Ok(msg.value.to_be_bytes().to_vec())
    }
    fn decode(&self, bytes: &[u8]) -> Result<Pong, Error> {
        if bytes.len() < 4 {
            return Err(Error::from("Pong: short read"));
        }
        Ok(Pong {
            value: u32::from_be_bytes(bytes[0..4].try_into().unwrap()),
        })
    }
}

// ─── TCP Transport ──────────────────────────────────────────────────────────

/// Simple length-prefixed TCP transport.
///
/// Wire format per envelope:
///   [4 bytes: total frame len (BE u32)]
///   [32 bytes: dest address]
///   [4 bytes: type_tag len (BE u32)]
///   [N bytes: type_tag UTF-8]
///   [remaining: payload bytes]
struct TcpTransport {
    stream: Mutex<TcpStream>,
}

impl Transport for TcpTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        let tag_bytes = envelope.type_tag.as_bytes();
        let frame_len: u32 = (32 + 4 + tag_bytes.len() + envelope.payload.len()) as u32;

        let mut buf = Vec::with_capacity(4 + frame_len as usize);
        buf.extend_from_slice(&frame_len.to_be_bytes());
        buf.extend_from_slice(&envelope.dest.0);
        buf.extend_from_slice(&(tag_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(tag_bytes);
        buf.extend_from_slice(&envelope.payload);

        self.stream
            .lock()
            .unwrap()
            .write_all(&buf)
            .map_err(|e| Error::from(format!("TCP send: {e}")))
    }
}

/// Read one WireEnvelope from a TCP stream.
fn read_envelope(stream: &mut TcpStream) -> std::io::Result<WireEnvelope> {
    // Frame length
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;

    // Read entire frame
    let mut frame = vec![0u8; frame_len];
    stream.read_exact(&mut frame)?;

    // Parse
    let mut dest = [0u8; 32];
    dest.copy_from_slice(&frame[0..32]);

    let tag_len = u32::from_be_bytes(frame[32..36].try_into().unwrap()) as usize;
    let type_tag = String::from_utf8_lossy(&frame[36..36 + tag_len]).to_string();
    let payload = frame[36 + tag_len..].to_vec();

    Ok(WireEnvelope {
        dest: ActorAddress(dest),
        type_tag,
        payload,
    })
}

// ─── Actor ──────────────────────────────────────────────────────────────────

struct PongActor;

impl ActorInterface for PongActor {
    type Incoming = Ping;
    type Response = Pong;

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        println!(
            "  PongActor received Ping({}), replying with Pong({})",
            msg.value,
            msg.value + 1
        );
        let _ = ctx.send(msg.reply_to, Pong { value: msg.value + 1 });
    }
}

// ─── Codec registry (shared) ───────────────────────────────────────────────

fn build_codecs() -> CodecRegistry {
    let mut cr = CodecRegistry::new();
    cr.register::<Ping, _>(ExampleCodec);
    cr.register::<Pong, _>(ExampleCodec);
    cr
}

// ─── Main ───────────────────────────────────────────────────────────────────

const ADDR: &str = "127.0.0.1:9100";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    match role {
        "receiver" => run_receiver(),
        "sender" => run_sender(),
        _ => {
            eprintln!("Usage: two_process <receiver|sender>");
            eprintln!();
            eprintln!("  Terminal 1: cargo run --example two_process --features transport -- receiver");
            eprintln!("  Terminal 2: cargo run --example two_process --features transport -- sender");
            std::process::exit(1);
        }
    }
}

/// Receiver process: hosts the PongActor, listens for incoming envelopes on TCP.
fn run_receiver() {
    println!("[receiver] Starting on {ADDR}...");

    let codecs = Arc::new(build_codecs());
    let codecs_recv = codecs.clone();

    // Build runtime with PongActor at the well-known address
    let mut rt = Runtime::new(RuntimeConfig::default());

    // We need the actor at the agreed address. Since spawn() generates a random
    // address, we'll use a workaround: spawn normally, then register a route
    // for replies going back to the sender (those will be Pong messages).
    // But actually, the sender's inbox address is dynamic, so the receiver
    // needs a transport to send Pong back.

    // For this demo: the receiver accepts a TCP connection, and uses that same
    // connection (reversed) to send replies.

    let pong_addr = rt.spawn(PongActor).unwrap();
    rt.tick(); // drain spawn queue

    println!("[receiver] PongActor spawned at {pong_addr}");
    println!("[receiver] Listening for connections...");

    let listener =
        TcpListener::bind(ADDR).expect("failed to bind");

    // Accept one connection
    let (mut stream, peer) = listener.accept().expect("accept failed");
    println!("[receiver] Connection from {peer}");

    // Read the sender's inbox address (first 32 bytes)
    let mut inbox_bytes = [0u8; 32];
    stream.read_exact(&mut inbox_bytes).unwrap();
    let sender_inbox_addr = ActorAddress(inbox_bytes);
    println!("[receiver] Sender inbox: {sender_inbox_addr}");

    // Send back the actual PongActor address (so sender can address messages)
    stream.write_all(&pong_addr.0).unwrap();

    // Set up transport for replies back to sender
    let reply_transport = Arc::new(TcpTransport {
        stream: Mutex::new(stream.try_clone().unwrap()),
    });
    let router = TransportRouter::new();
    router.add_route(sender_inbox_addr, reply_transport);
    rt.set_codec_registry(codecs.clone());
    rt.set_transport_router(Arc::new(router));

    // Event loop: read envelopes from TCP, deliver, tick
    println!("[receiver] Ready — waiting for pings...\n");
    loop {
        match read_envelope(&mut stream) {
            Ok(envelope) => {
                let (addr, msg) = codecs_recv.receive(envelope).unwrap();
                rt.deliver_raw(addr, msg).unwrap();
                rt.tick();
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("\n[receiver] Sender disconnected.");
                break;
            }
            Err(e) => {
                eprintln!("[receiver] Read error: {e}");
                break;
            }
        }
    }
}

/// Sender process: connects to receiver, sends Pings, reads Pong replies.
fn run_sender() {
    println!("[sender] Connecting to {ADDR}...");

    let codecs = Arc::new(build_codecs());
    let codecs_recv = codecs.clone();

    let mut rt = Runtime::new(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let inbox_addr = *inbox.addr();

    // Connect and exchange addresses
    let mut stream =
        TcpStream::connect(ADDR).expect("failed to connect — is the receiver running?");

    // Send our inbox address
    stream.write_all(&inbox_addr.0).unwrap();

    // Read the PongActor's address
    let mut pong_bytes = [0u8; 32];
    stream.read_exact(&mut pong_bytes).unwrap();
    let pong_addr = ActorAddress(pong_bytes);
    println!("[sender] Connected. PongActor is at {pong_addr}\n");

    // Set up transport to send Pings to receiver
    let send_transport = Arc::new(TcpTransport {
        stream: Mutex::new(stream.try_clone().unwrap()),
    });
    let router = TransportRouter::new();
    router.add_route(pong_addr, send_transport);
    rt.set_codec_registry(codecs.clone());
    rt.set_transport_router(Arc::new(router));

    // Send 5 pings
    for i in 1..=5 {
        println!("[sender] Sending Ping({i})...");
        rt.send_to(
            pong_addr,
            Ping {
                value: i,
                reply_to: inbox_addr,
            },
        )
        .unwrap();

        // Read the reply from TCP
        match read_envelope(&mut stream) {
            Ok(envelope) => {
                let (addr, msg) = codecs_recv.receive(envelope).unwrap();
                rt.deliver_raw(addr, msg).unwrap();
            }
            Err(e) => {
                eprintln!("[sender] Read error: {e}");
                break;
            }
        }

        // Check inbox
        if let Some(pong) = inbox.try_recv() {
            println!("[sender] Got Pong({})!", pong.value);
        }

        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    println!("\n[sender] Done.");
}
