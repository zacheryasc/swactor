use std::time::Instant;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use swactor::actor::ActorAddress;
use swactor::stats::{RuntimeStats, TickTiming};

/// Per-worker data prepared for rendering.
pub struct WorkerView {
    pub id: usize,
    pub num_actors: usize,
    pub mailbox_depth: usize,
    pub messages_processed: u64,
    pub msg_rate: f64,
    pub load_pct: f64,
    /// Fraction of bar for each phase group: [processing, delivery, spawns, overhead]
    pub phase_fractions: [f64; 4],
    pub panics: u64,
}

/// One row in the actor table.
pub struct ActorRow {
    pub address: ActorAddress,
    pub worker_id: usize,
    pub mailbox_depth: usize,
    pub last_msg_type: Option<String>,
    pub messages_processed: u64,
    pub poisoned: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SortColumn {
    Address,
    Worker,
    Mailbox,
    LastMsg,
    MsgCount,
}

impl SortColumn {
    pub fn next(self) -> Self {
        match self {
            SortColumn::Address => SortColumn::Worker,
            SortColumn::Worker => SortColumn::Mailbox,
            SortColumn::Mailbox => SortColumn::MsgCount,
            SortColumn::MsgCount => SortColumn::LastMsg,
            SortColumn::LastMsg => SortColumn::Address,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SortColumn::Address => "ADDRESS",
            SortColumn::Worker => "WORKER",
            SortColumn::Mailbox => "MAILBOX",
            SortColumn::MsgCount => "MSGS",
            SortColumn::LastMsg => "LAST MSG",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ViewMode {
    Overview,
    WorkerDetail,
}

pub struct App {
    pub workers: Vec<WorkerView>,
    pub actor_rows: Vec<ActorRow>,
    pub sort_column: SortColumn,
    pub sort_desc: bool,
    pub selected: usize,
    pub should_quit: bool,
    pub total_actors: usize,
    pub total_messages: u64,
    pub total_mailbox: usize,
    pub total_panics: u64,
    pub num_workers: usize,
    pub view_mode: ViewMode,
    pub focused_worker: usize,

    prev_messages: Vec<u64>,
    prev_time: Instant,
    /// Rolling msg rates (smoothed)
    msg_rates: Vec<f64>,
}

impl App {
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
            actor_rows: Vec::new(),
            sort_column: SortColumn::Mailbox,
            sort_desc: true,
            selected: 0,
            should_quit: false,
            total_actors: 0,
            total_messages: 0,
            total_mailbox: 0,
            total_panics: 0,
            num_workers: 0,
            view_mode: ViewMode::Overview,
            focused_worker: 0,
            prev_messages: Vec::new(),
            prev_time: Instant::now(),
            msg_rates: Vec::new(),
        }
    }

    /// Actor rows filtered to the focused worker (for worker detail view).
    pub fn focused_actor_rows(&self) -> Vec<&ActorRow> {
        self.actor_rows
            .iter()
            .filter(|r| r.worker_id == self.focused_worker)
            .collect()
    }

    pub fn update(&mut self, stats: RuntimeStats) {
        let now = Instant::now();
        let dt = now.duration_since(self.prev_time).as_secs_f64();
        self.prev_time = now;

        self.num_workers = stats.num_workers;

        // Ensure rate vectors are sized
        if self.prev_messages.len() != stats.workers.len() {
            self.prev_messages = stats.workers.iter().map(|w| w.messages_processed).collect();
            self.msg_rates = vec![0.0; stats.workers.len()];
        }

        // Compute per-worker views
        self.workers.clear();
        for (i, w) in stats.workers.iter().enumerate() {
            // Message rate
            let rate = if dt > 0.0 {
                let delta = w.messages_processed.saturating_sub(self.prev_messages[i]);
                delta as f64 / dt
            } else {
                0.0
            };
            // Smooth the rate (exponential moving average)
            let smoothed = if self.msg_rates[i] == 0.0 {
                rate
            } else {
                self.msg_rates[i] * 0.6 + rate * 0.4
            };
            self.msg_rates[i] = smoothed;
            self.prev_messages[i] = w.messages_processed;

            // Load % and phase fractions from tick timings
            let timings = stats.tick_timings.get(i).map(|v| v.as_slice()).unwrap_or(&[]);
            let (load_pct, phase_fractions) = compute_load_and_phases(timings);

            self.workers.push(WorkerView {
                id: w.id,
                num_actors: w.num_actors,
                mailbox_depth: w.mailbox_depth,
                messages_processed: w.messages_processed,
                msg_rate: smoothed,
                load_pct,
                phase_fractions,
                panics: w.panics,
            });
        }

        // Aggregate stats
        self.total_actors = stats.actor_details.len();
        self.total_messages = stats.workers.iter().map(|w| w.messages_processed).sum();
        self.total_mailbox = stats.workers.iter().map(|w| w.mailbox_depth).sum();
        self.total_panics = stats.workers.iter().map(|w| w.panics).sum();

        // Build actor table
        self.actor_rows.clear();
        for a in &stats.actor_details {
            self.actor_rows.push(ActorRow {
                address: a.address,
                worker_id: a.worker_id,
                mailbox_depth: a.mailbox_depth,
                last_msg_type: a.last_msg_type.clone(),
                messages_processed: a.messages_processed,
                poisoned: a.poisoned,
            });
        }
        self.sort_actors();

        // Clamp selection
        if !self.actor_rows.is_empty() {
            self.selected = self.selected.min(self.actor_rows.len() - 1);
        } else {
            self.selected = 0;
        }
    }

    fn sort_actors(&mut self) {
        let desc = self.sort_desc;
        match self.sort_column {
            SortColumn::Address => {
                self.actor_rows.sort_by(|a, b| {
                    let cmp = a.address.0.cmp(&b.address.0);
                    if desc { cmp.reverse() } else { cmp }
                });
            }
            SortColumn::Worker => {
                self.actor_rows.sort_by(|a, b| {
                    let cmp = a.worker_id.cmp(&b.worker_id);
                    if desc { cmp.reverse() } else { cmp }
                });
            }
            SortColumn::Mailbox => {
                self.actor_rows.sort_by(|a, b| {
                    let cmp = a.mailbox_depth.cmp(&b.mailbox_depth);
                    if desc { cmp.reverse() } else { cmp }
                });
            }
            SortColumn::MsgCount => {
                self.actor_rows.sort_by(|a, b| {
                    let cmp = a.messages_processed.cmp(&b.messages_processed);
                    if desc { cmp.reverse() } else { cmp }
                });
            }
            SortColumn::LastMsg => {
                self.actor_rows.sort_by(|a, b| {
                    let cmp = a.last_msg_type.cmp(&b.last_msg_type);
                    if desc { cmp.reverse() } else { cmp }
                });
            }
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        // Global keys
        match key.code {
            KeyCode::Char('q') => { self.should_quit = true; return; }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true; return;
            }
            KeyCode::Char('s') => {
                self.sort_column = self.sort_column.next();
                self.sort_actors();
                return;
            }
            KeyCode::Char('r') => {
                self.sort_desc = !self.sort_desc;
                self.sort_actors();
                return;
            }
            KeyCode::Tab => {
                self.view_mode = match self.view_mode {
                    ViewMode::Overview => ViewMode::WorkerDetail,
                    ViewMode::WorkerDetail => ViewMode::Overview,
                };
                return;
            }
            _ => {}
        }

        match self.view_mode {
            ViewMode::Overview => self.handle_key_overview(key),
            ViewMode::WorkerDetail => self.handle_key_worker_detail(key),
        }
    }

    fn handle_key_overview(&mut self, key: KeyEvent) {
        let max = if self.actor_rows.is_empty() { 0 } else { self.actor_rows.len() - 1 };
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = self.selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1).min(max);
            }
            KeyCode::PageUp => {
                self.selected = self.selected.saturating_sub(20);
            }
            KeyCode::PageDown => {
                self.selected = (self.selected + 20).min(max);
            }
            KeyCode::Home => { self.selected = 0; }
            KeyCode::End => { self.selected = max; }
            KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => {
                // Enter worker detail for the selected actor's worker
                if let Some(row) = self.actor_rows.get(self.selected) {
                    self.focused_worker = row.worker_id;
                }
                self.view_mode = ViewMode::WorkerDetail;
            }
            _ => {}
        }
    }

    fn handle_key_worker_detail(&mut self, key: KeyEvent) {
        let max_w = if self.workers.is_empty() { 0 } else { self.workers.len() - 1 };
        match key.code {
            KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => {
                self.view_mode = ViewMode::Overview;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.focused_worker = self.focused_worker.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.focused_worker = (self.focused_worker + 1).min(max_w);
            }
            KeyCode::PageUp => {
                self.focused_worker = self.focused_worker.saturating_sub(20);
            }
            KeyCode::PageDown => {
                self.focused_worker = (self.focused_worker + 20).min(max_w);
            }
            KeyCode::Home => { self.focused_worker = 0; }
            KeyCode::End => { self.focused_worker = max_w; }
            _ => {}
        }
    }

    pub fn visible_table_height(&self) -> usize {
        // Will be set by the UI based on actual chunk size
        20
    }
}

/// Compute load percentage and phase fractions from tick timings.
///
/// Returns (load_pct, [processing, delivery, spawns, overhead]).
fn compute_load_and_phases(timings: &[TickTiming]) -> (f64, [f64; 4]) {
    if timings.is_empty() {
        return (0.0, [0.0; 4]);
    }

    let active = timings.iter().filter(|t| t.did_work).count();
    let load_pct = (active as f64 / timings.len() as f64) * 100.0;

    // Sum phase microseconds across all ticks
    let mut phase_sums = [0u64; 6];
    for t in timings {
        for (i, &us) in t.phase_us.iter().enumerate() {
            phase_sums[i] += us;
        }
    }

    let total_us: u64 = phase_sums.iter().sum();
    if total_us == 0 {
        return (load_pct, [0.25, 0.25, 0.25, 0.25]);
    }

    let total = total_us as f64;
    // Group phases:
    // processing = phase 2 (tick_all)
    // delivery   = phase 1 (transfer drain) + phase 4 (pending local)
    // spawns     = phase 0 (spawn drain) + phase 3 (spawn drain again)
    // overhead   = phase 5 (stats)
    let processing = phase_sums[2] as f64 / total;
    let delivery = (phase_sums[1] + phase_sums[4]) as f64 / total;
    let spawns = (phase_sums[0] + phase_sums[3]) as f64 / total;
    let overhead = phase_sums[5] as f64 / total;

    (load_pct, [processing, delivery, spawns, overhead])
}
