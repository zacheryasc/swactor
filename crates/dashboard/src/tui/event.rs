use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crossterm::event::{self, Event, KeyEvent, KeyEventKind};
use swactor::stats::RuntimeStats;

use super::types::RuntimeEndpoint;

pub enum AppEvent {
    Key(KeyEvent),
    Tick,
    StatsUpdate {
        source: RuntimeEndpoint,
        stats: Box<RuntimeStats>,
    },
}

pub struct EventLoop {
    rx: mpsc::Receiver<AppEvent>,
}

impl EventLoop {
    pub fn new(tick_ms: u64) -> Self {
        let (tx, rx) = mpsc::channel();

        // Input thread
        let tx_input = tx.clone();
        thread::spawn(move || {
            loop {
                // Poll with a timeout so the thread can eventually notice if the
                // channel is dropped (tx_input.send will fail).
                if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    if let Ok(Event::Key(key)) = event::read() {
                        // Only handle key press events, not release/repeat
                        if key.kind == KeyEventKind::Press {
                            if tx_input.send(AppEvent::Key(key)).is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });

        // Tick thread
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(tick_ms));
                if tx.send(AppEvent::Tick).is_err() {
                    return;
                }
            }
        });

        Self { rx }
    }

    /// Create an event loop and return a sender for external producers (e.g. SSE client).
    pub fn new_with_sender(tick_ms: u64) -> (Self, mpsc::Sender<AppEvent>) {
        let (tx, rx) = mpsc::channel();

        let tx_input = tx.clone();
        thread::spawn(move || {
            loop {
                if event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    if let Ok(Event::Key(key)) = event::read() {
                        if key.kind == KeyEventKind::Press {
                            if tx_input.send(AppEvent::Key(key)).is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        });

        let tx_tick = tx.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(tick_ms));
                if tx_tick.send(AppEvent::Tick).is_err() {
                    return;
                }
            }
        });

        (Self { rx }, tx)
    }

    pub fn next(&self) -> Result<AppEvent, mpsc::RecvError> {
        self.rx.recv()
    }
}
