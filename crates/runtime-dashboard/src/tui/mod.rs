pub mod app;
mod event;
pub mod sse_client;
pub mod types;
mod ui;

use std::io;
use std::sync::Arc;

use crossterm::execute;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::widgets::TableState;

use swactor::runtime::Runtime;

use crate::collector::StatsCollector;
#[cfg(feature = "distribution")]
use crate::distribution_collector::DistributionStatsProvider;
use crate::layer::EventStore;
use self::app::App;
use self::event::{AppEvent, EventLoop};
use self::types::RuntimeEndpoint;

/// Configuration for the TUI dashboard.
#[derive(Debug, Clone)]
pub struct TuiConfig {
    /// How often to poll `Runtime::stats()`, in milliseconds.
    pub poll_interval_ms: u64,
}

impl Default for TuiConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 200,
        }
    }
}

/// Run the TUI dashboard. Blocks the calling thread until the user quits.
///
/// Takes ownership of the terminal (alternate screen + raw mode) and restores
/// it on exit, including on panic.
pub fn start_tui(
    runtime: Arc<Runtime>,
    collector: Arc<StatsCollector>,
    #[cfg(feature = "distribution")]
    distribution: Option<Arc<dyn DistributionStatsProvider>>,
    config: TuiConfig,
    event_store: Option<Arc<EventStore>>,
) -> io::Result<()> {
    // Set up terminal
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    // Run main loop
    let result = run_loop(
        &mut terminal,
        runtime,
        collector,
        #[cfg(feature = "distribution")]
        distribution,
        config,
        event_store,
    );

    // Restore terminal
    crossterm::terminal::disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    // Restore default panic hook
    let _ = std::panic::take_hook();

    result
}

fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    runtime: Arc<Runtime>,
    collector: Arc<StatsCollector>,
    #[cfg(feature = "distribution")]
    distribution: Option<Arc<dyn DistributionStatsProvider>>,
    config: TuiConfig,
    event_store: Option<Arc<EventStore>>,
) -> io::Result<()> {
    let mut app = App::new();
    if let Some(store) = event_store {
        app.set_event_store(store);
    }
    let mut table_state = TableState::default();
    let events = EventLoop::new(config.poll_interval_ms);

    // Initial stats poll
    let mut stats = runtime.stats();
    collector.enrich(&mut stats);
    app.update(stats);

    loop {
        terminal.draw(|f| ui::draw(f, &app, &mut table_state))?;

        match events.next() {
            Ok(AppEvent::Tick) => {
                let mut stats = runtime.stats();
                collector.enrich(&mut stats);
                app.update(stats);
                #[cfg(feature = "distribution")]
                if let Some(ref provider) = distribution {
                    if let Some(snapshot) = provider.snapshot() {
                        app.update_distribution(snapshot);
                    }
                }
            }
            Ok(AppEvent::Key(key)) => {
                app.handle_key(key);
            }
            Ok(AppEvent::StatsUpdate { stats, .. }) => {
                app.update(*stats);
            }
            #[cfg(feature = "distribution")]
            Ok(AppEvent::DistributionUpdate { snapshot }) => {
                app.update_distribution(*snapshot);
            }
            Err(_) => {
                // Channel closed, exit
                break;
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

/// Run the TUI dashboard connected to a remote runtime's SSE endpoint.
///
/// Blocks the calling thread until the user quits ('q' or Esc).
pub fn start_tui_remote(endpoint: RuntimeEndpoint, config: TuiConfig) -> io::Result<()> {
    // Set up terminal
    crossterm::terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    // Run main loop
    let result = run_loop_remote(&mut terminal, endpoint, config);

    // Restore terminal
    crossterm::terminal::disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    let _ = std::panic::take_hook();

    result
}

fn run_loop_remote(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    endpoint: RuntimeEndpoint,
    config: TuiConfig,
) -> io::Result<()> {
    let mut app = App::new();
    let mut table_state = TableState::default();

    let (events, tx) = EventLoop::new_with_sender(config.poll_interval_ms);

    // Spawn the SSE reader thread — pushes StatsUpdate events into the same channel
    let _sse_handle = sse_client::spawn_sse_reader(endpoint, tx)?;

    loop {
        terminal.draw(|f| ui::draw(f, &app, &mut table_state))?;

        match events.next() {
            Ok(AppEvent::StatsUpdate { stats, .. }) => {
                app.update(*stats);
            }
            Ok(AppEvent::Tick) => {
                // Just redraw — data comes from SSE, not local polling
            }
            Ok(AppEvent::Key(key)) => {
                app.handle_key(key);
            }
            #[cfg(feature = "distribution")]
            Ok(AppEvent::DistributionUpdate { snapshot }) => {
                app.update_distribution(*snapshot);
            }
            Err(_) => {
                break;
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}
