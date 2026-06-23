use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use swactor::actor::ActorAddress;
use swactor::stats::{RuntimeStats, TickTiming};

use crate::layer::{DashboardEvent, EventStore};
use crate::warnings::{Warning, WarningConfig, WarningDetector};

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
    /// Recent message rates for sparkline rendering.
    pub sparkline_rates: Vec<u64>,
    /// Recent mailbox depths for sparkline rendering.
    pub sparkline_mailbox: Vec<u64>,
}

/// One row in the actor table.
pub struct ActorRow {
    pub address: ActorAddress,
    pub worker_id: usize,
    pub mailbox_depth: usize,
    pub last_msg_type: Option<String>,
    pub messages_processed: u64,
    pub poisoned: bool,
    pub name: Option<String>,
    /// Per-actor mailbox sparkline (from local ring buffer).
    pub sparkline_mailbox: Vec<u64>,
    /// Per-actor message rate sparkline.
    pub sparkline_rates: Vec<u64>,
    /// Per-message-type counts, sorted descending.
    pub message_type_counts: Vec<(String, u64)>,
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
    ActorDetail,
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
    pub prev_view_mode: ViewMode,
    pub focused_worker: usize,
    pub focused_actor: Option<ActorAddress>,
    pub search_active: bool,
    pub search_query: String,
    pub search_locked: bool,
    pub warnings: Vec<Warning>,

    prev_messages: Vec<u64>,
    prev_time: Instant,
    /// Rolling msg rates (smoothed)
    msg_rates: Vec<f64>,
    /// Per-worker sparkline history (message rate deltas).
    sparkline_rates: Vec<VecDeque<u64>>,
    /// Per-worker sparkline history (mailbox depths).
    sparkline_mailbox: Vec<VecDeque<u64>>,
    /// Per-actor sparkline history: address → (prev_msgs, rates, mailbox_depths).
    actor_sparklines: HashMap<ActorAddress, (u64, VecDeque<u64>, VecDeque<u64>)>,
    warning_detector: WarningDetector,
    /// Event store for fetching per-actor logs.
    event_store: Option<Arc<EventStore>>,
    /// Cached log entries for the focused actor.
    pub actor_logs: Vec<DashboardEvent>,
    /// Scroll offset for actor log view.
    pub log_scroll: usize,
    /// Active log level filter (all enabled by default).
    pub log_levels: [bool; 5], // ERROR, WARN, INFO, DEBUG, TRACE
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
            prev_view_mode: ViewMode::Overview,
            focused_worker: 0,
            focused_actor: None,
            search_active: false,
            search_query: String::new(),
            search_locked: false,
            warnings: Vec::new(),
            prev_messages: Vec::new(),
            prev_time: Instant::now(),
            msg_rates: Vec::new(),
            sparkline_rates: Vec::new(),
            sparkline_mailbox: Vec::new(),
            actor_sparklines: HashMap::new(),
            warning_detector: WarningDetector::new(WarningConfig::default()),
            event_store: None,
            actor_logs: Vec::new(),
            log_scroll: 0,
            log_levels: [true; 5],
        }
    }

    /// Set the event store for per-actor log retrieval.
    pub fn set_event_store(&mut self, store: Arc<EventStore>) {
        self.event_store = Some(store);
    }

    /// Get visible actor rows (filtered by search query if active).
    pub fn visible_actor_rows(&self) -> Vec<&ActorRow> {
        if self.search_query.is_empty() {
            self.actor_rows.iter().collect()
        } else {
            let q = self.search_query.to_lowercase();
            self.actor_rows
                .iter()
                .filter(|r| {
                    let addr = format!("{}", r.address).to_lowercase();
                    let msg = r.last_msg_type.as_deref().unwrap_or("").to_lowercase();
                    let worker = format!("w{}", r.worker_id);
                    let name = r.name.as_deref().unwrap_or("").to_lowercase();
                    addr.contains(&q)
                        || msg.contains(&q)
                        || worker.contains(&q)
                        || name.contains(&q)
                })
                .collect()
        }
    }

    /// Get the focused actor's data (for actor detail view).
    pub fn focused_actor_row(&self) -> Option<&ActorRow> {
        self.focused_actor
            .as_ref()
            .and_then(|addr| self.actor_rows.iter().find(|r| r.address == *addr))
    }

    /// Refresh the actor log cache from the event store.
    pub fn refresh_actor_logs(&mut self) {
        if let (Some(addr), Some(store)) = (&self.focused_actor, &self.event_store) {
            let hex = format!("{}", addr);
            self.actor_logs = store.read_for_actor(&hex, 200);
        } else {
            self.actor_logs.clear();
        }
    }

    /// Get visible log entries (filtered by level).
    pub fn visible_logs(&self) -> Vec<&DashboardEvent> {
        self.actor_logs
            .iter()
            .filter(|e| match e.level.as_str() {
                "ERROR" => self.log_levels[0],
                "WARN" => self.log_levels[1],
                "INFO" => self.log_levels[2],
                "DEBUG" => self.log_levels[3],
                "TRACE" => self.log_levels[4],
                _ => true,
            })
            .collect()
    }

    /// Toggle a log level filter (0=ERROR, 1=WARN, 2=INFO, 3=DEBUG, 4=TRACE).
    pub fn toggle_log_level(&mut self, idx: usize) {
        if idx < 5 {
            self.log_levels[idx] = !self.log_levels[idx];
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
            self.sparkline_rates
                .resize_with(stats.workers.len(), VecDeque::new);
            self.sparkline_mailbox
                .resize_with(stats.workers.len(), VecDeque::new);
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

            // Track sparkline history
            let delta = w.messages_processed.saturating_sub(self.prev_messages[i]);
            self.prev_messages[i] = w.messages_processed;

            let spark_rates = &mut self.sparkline_rates[i];
            if spark_rates.len() >= 60 {
                spark_rates.pop_front();
            }
            spark_rates.push_back(delta);

            let spark_mbox = &mut self.sparkline_mailbox[i];
            if spark_mbox.len() >= 60 {
                spark_mbox.pop_front();
            }
            spark_mbox.push_back(w.mailbox_depth as u64);

            // Load % and phase fractions from tick timings
            let timings = stats
                .tick_timings
                .get(i)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
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
                sparkline_rates: spark_rates.iter().copied().collect(),
                sparkline_mailbox: spark_mbox.iter().copied().collect(),
            });
        }

        // Aggregate stats
        self.total_actors = stats.actor_details.len();
        self.total_messages = stats.workers.iter().map(|w| w.messages_processed).sum();
        self.total_mailbox = stats.workers.iter().map(|w| w.mailbox_depth).sum();
        self.total_panics = stats.workers.iter().map(|w| w.panics).sum();

        // Build actor table with sparkline history
        self.actor_rows.clear();
        for a in &stats.actor_details {
            let (prev, rates_buf, mbox_buf) = self
                .actor_sparklines
                .entry(a.address)
                .or_insert_with(|| (0, VecDeque::new(), VecDeque::new()));

            let rate_delta = a.messages_processed.saturating_sub(*prev);
            *prev = a.messages_processed;
            if rates_buf.len() >= 60 {
                rates_buf.pop_front();
            }
            rates_buf.push_back(rate_delta);
            if mbox_buf.len() >= 60 {
                mbox_buf.pop_front();
            }
            mbox_buf.push_back(a.mailbox_depth as u64);

            self.actor_rows.push(ActorRow {
                address: a.address,
                worker_id: a.worker_id,
                mailbox_depth: a.mailbox_depth,
                last_msg_type: a.last_msg_type.clone(),
                messages_processed: a.messages_processed,
                poisoned: a.poisoned,
                name: a.name.clone(),
                sparkline_rates: rates_buf.iter().copied().collect(),
                sparkline_mailbox: mbox_buf.iter().copied().collect(),
                message_type_counts: a.message_type_counts.clone(),
            });
        }
        self.sort_actors();

        // Run warning detection
        self.warnings = self.warning_detector.check(&stats);

        // Refresh actor logs if in detail view
        if self.view_mode == ViewMode::ActorDetail {
            self.refresh_actor_logs();
        }

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
        // Search mode input handling
        if self.search_active {
            match key.code {
                KeyCode::Esc => {
                    self.search_active = false;
                    if !self.search_locked {
                        self.search_query.clear();
                    }
                    return;
                }
                KeyCode::Enter => {
                    self.search_active = false;
                    self.search_locked = !self.search_query.is_empty();
                    return;
                }
                KeyCode::Backspace => {
                    self.search_query.pop();
                    return;
                }
                KeyCode::Char(c) => {
                    self.search_query.push(c);
                    return;
                }
                _ => return,
            }
        }

        // Global keys
        match key.code {
            KeyCode::Char('q') => {
                self.should_quit = true;
                return;
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                return;
            }
            KeyCode::Char('/') => {
                self.search_active = true;
                self.search_locked = false;
                self.search_query.clear();
                return;
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
                    ViewMode::ActorDetail => ViewMode::Overview,
                    ViewMode::WorkerDetail => ViewMode::Overview,
                };
                return;
            }
            _ => {}
        }

        match self.view_mode {
            ViewMode::Overview => self.handle_key_overview(key),
            ViewMode::WorkerDetail => self.handle_key_worker_detail(key),
            ViewMode::ActorDetail => self.handle_key_actor_detail(key),
        }
    }

    fn handle_key_overview(&mut self, key: KeyEvent) {
        let visible = self.visible_actor_rows();
        let max = if visible.is_empty() {
            0
        } else {
            visible.len() - 1
        };
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
            KeyCode::Home => {
                self.selected = 0;
            }
            KeyCode::End => {
                self.selected = max;
            }
            KeyCode::Enter | KeyCode::Char('l') | KeyCode::Right => {
                // Enter actor detail for the selected visible actor
                if let Some(&row) = visible.get(self.selected) {
                    self.focused_actor = Some(row.address);
                    self.prev_view_mode = ViewMode::Overview;
                    self.view_mode = ViewMode::ActorDetail;
                    self.log_scroll = 0;
                    self.refresh_actor_logs();
                }
            }
            _ => {}
        }
    }

    fn handle_key_worker_detail(&mut self, key: KeyEvent) {
        let max_w = if self.workers.is_empty() {
            0
        } else {
            self.workers.len() - 1
        };
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
            KeyCode::Home => {
                self.focused_worker = 0;
            }
            KeyCode::End => {
                self.focused_worker = max_w;
            }
            _ => {}
        }
    }

    fn handle_key_actor_detail(&mut self, key: KeyEvent) {
        let max_scroll = self.visible_logs().len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => {
                self.view_mode = self.prev_view_mode;
                self.focused_actor = None;
                self.actor_logs.clear();
                self.log_scroll = 0;
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.log_scroll = (self.log_scroll + 1).min(max_scroll);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.log_scroll = self.log_scroll.saturating_sub(1);
            }
            KeyCode::PageDown => {
                self.log_scroll = (self.log_scroll + 20).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.log_scroll = self.log_scroll.saturating_sub(20);
            }
            KeyCode::Home => {
                self.log_scroll = 0;
            }
            KeyCode::End => {
                self.log_scroll = max_scroll;
            }
            KeyCode::Char('1') => self.toggle_log_level(0),
            KeyCode::Char('2') => self.toggle_log_level(1),
            KeyCode::Char('3') => self.toggle_log_level(2),
            KeyCode::Char('4') => self.toggle_log_level(3),
            KeyCode::Char('5') => self.toggle_log_level(4),
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
