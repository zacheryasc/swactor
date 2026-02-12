use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};

use super::app::{App, SortColumn, ViewMode};

/// Render the full dashboard into the given frame.
pub fn draw(f: &mut Frame, app: &App, table_state: &mut TableState) {
    match app.view_mode {
        ViewMode::Overview => draw_overview(f, app, table_state),
        ViewMode::WorkerDetail => draw_worker_detail(f, app, table_state),
        #[cfg(feature = "distribution")]
        ViewMode::Distribution => draw_distribution(f, app, table_state),
    }
}

// ─── Overview ────────────────────────────────────────────────────────────────

fn draw_overview(f: &mut Frame, app: &App, table_state: &mut TableState) {
    let num_workers = app.workers.len().max(1);

    let chunks = Layout::vertical([
        Constraint::Length(num_workers as u16),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .split(f.area());

    draw_worker_bars(f, app, chunks[0]);
    draw_summary(f, app, chunks[1]);
    draw_actor_table(f, app, table_state, chunks[2]);
}

/// Render htop-style worker bars.
fn draw_worker_bars(f: &mut Frame, app: &App, area: Rect) {
    let lines: Vec<Line> = app
        .workers
        .iter()
        .map(|w| build_worker_line(w, area.width as usize))
        .collect();

    let paragraph = Paragraph::new(lines);
    f.render_widget(paragraph, area);
}

fn build_worker_line(w: &super::app::WorkerView, total_width: usize) -> Line<'static> {
    let id_str = format!("{:>3}", w.id);
    let suffix = format!(
        "  {} actors  {} m/s",
        w.num_actors,
        format_rate(w.msg_rate),
    );

    let pct_str = format!("{:>5.1}%", w.load_pct);
    let overhead = id_str.len() + 1 + 1 + pct_str.len() + suffix.len();
    let bar_width = if total_width > overhead {
        total_width - overhead
    } else {
        10
    };

    let filled = ((w.load_pct / 100.0) * bar_width as f64).round() as usize;
    let filled = filled.min(bar_width);

    let phase_colors = [Color::Green, Color::Blue, Color::Cyan, Color::Red];
    let mut phase_cells = [0usize; 4];
    if filled > 0 {
        let mut assigned = 0usize;
        for i in 0..4 {
            phase_cells[i] = (w.phase_fractions[i] * filled as f64).round() as usize;
            assigned += phase_cells[i];
        }
        if assigned != filled {
            let diff = filled as isize - assigned as isize;
            let max_idx = phase_cells
                .iter()
                .enumerate()
                .max_by_key(|(_, v)| *v)
                .map(|(i, _)| i)
                .unwrap_or(0);
            phase_cells[max_idx] = (phase_cells[max_idx] as isize + diff).max(0) as usize;
        }
    }

    let mut spans: Vec<Span> = Vec::new();

    spans.push(Span::raw(id_str));
    spans.push(Span::raw("["));

    for (i, &color) in phase_colors.iter().enumerate() {
        let count = phase_cells[i];
        if count > 0 {
            spans.push(Span::styled(
                "|".repeat(count),
                Style::default().fg(color),
            ));
        }
    }

    let filled_total: usize = phase_cells.iter().sum();
    let empty = bar_width.saturating_sub(filled_total);
    if empty >= pct_str.len() {
        let padding = empty - pct_str.len();
        spans.push(Span::raw(" ".repeat(padding)));
        spans.push(Span::styled(
            pct_str,
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ));
    } else {
        spans.push(Span::styled(
            pct_str,
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ));
    }

    spans.push(Span::raw("]"));
    spans.push(Span::styled(suffix, Style::default().fg(Color::DarkGray)));

    Line::from(spans)
}

/// Render the summary stats line.
fn draw_summary(f: &mut Frame, app: &App, area: Rect) {
    let panics_style = if app.total_panics > 0 {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let line = Line::from(vec![
        Span::styled(" Workers: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", app.num_workers),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Actors: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", app.total_actors),
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Msgs: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_num(app.total_messages),
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Mailbox: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", app.total_mailbox),
            Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Panics: ", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{}", app.total_panics), panics_style),
    ]);

    f.render_widget(Paragraph::new(line), area);
}

/// Render the actor table with scrolling and selection.
fn draw_actor_table(f: &mut Frame, app: &App, table_state: &mut TableState, area: Rect) {
    let sort_arrow = if app.sort_desc { " \u{25bc}" } else { " \u{25b2}" };

    let columns = [
        SortColumn::Address,
        SortColumn::Worker,
        SortColumn::Mailbox,
        SortColumn::MsgCount,
        SortColumn::LastMsg,
    ];
    let header_cells = columns.iter().map(|&col| {
        let mut label = col.label().to_string();
        if col == app.sort_column {
            label.push_str(sort_arrow);
        }
        Cell::from(label).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = app
        .actor_rows
        .iter()
        .map(|a| actor_row_cells(a))
        .collect();

    table_state.select(Some(app.selected));

    let help_text =
        " q: quit  \u{2191}\u{2193}: scroll  s: sort column  r: reverse  Tab: worker view  Enter: drill in";

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(24),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(
                " Actors (sorted by {}{}) ",
                app.sort_column.label(),
                sort_arrow
            ))
            .title_bottom(Line::from(help_text).centered()),
    )
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("> ");

    f.render_stateful_widget(table, area, table_state);
}

fn actor_row_cells(a: &super::app::ActorRow) -> Row<'static> {
    let addr = format!("{}", a.address);
    let addr_short = if a.poisoned {
        let s = if addr.len() > 16 {
            format!("!{}...", &addr[..14])
        } else {
            format!("!{}", addr)
        };
        s
    } else if addr.len() > 18 {
        format!("{}...", &addr[..16])
    } else {
        addr
    };
    let addr_style = if a.poisoned {
        Style::default().fg(Color::Red)
    } else {
        Style::default()
    };
    let msg_short = short_type_name(a.last_msg_type.as_deref());
    let msg_style = if a.poisoned {
        Style::default().fg(Color::Red)
    } else if a.last_msg_type.is_some() {
        Style::default().fg(Color::Green)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    Row::new(vec![
        Cell::from(addr_short).style(addr_style),
        Cell::from(format!("{}", a.worker_id)),
        Cell::from(format!("{}", a.mailbox_depth)),
        Cell::from(format!("{}", a.messages_processed)),
        Cell::from(msg_short).style(msg_style),
    ])
}

// ─── Worker Detail View ──────────────────────────────────────────────────────

fn draw_worker_detail(f: &mut Frame, app: &App, table_state: &mut TableState) {
    let num_workers = app.workers.len().max(1);

    let chunks = Layout::vertical([
        Constraint::Length(num_workers as u16),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .split(f.area());

    draw_worker_selector(f, app, chunks[0]);
    draw_worker_summary(f, app, chunks[1]);
    draw_focused_actor_table(f, app, table_state, chunks[2]);
}

/// Worker list with highlight on the focused worker.
fn draw_worker_selector(f: &mut Frame, app: &App, area: Rect) {
    let lines: Vec<Line> = app
        .workers
        .iter()
        .map(|w| {
            let mut line = build_worker_line(w, area.width as usize);
            if w.id == app.focused_worker {
                // Highlight the focused worker row
                line = line.style(
                    Style::default()
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                );
            }
            line
        })
        .collect();

    let paragraph = Paragraph::new(lines);
    f.render_widget(paragraph, area);
}

/// Summary line for the focused worker.
fn draw_worker_summary(f: &mut Frame, app: &App, area: Rect) {
    let focused = app.workers.iter().find(|w| w.id == app.focused_worker);
    let line = match focused {
        Some(w) => {
            let panics_style = if w.panics > 0 {
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let actor_count = app
                .actor_rows
                .iter()
                .filter(|r| r.worker_id == app.focused_worker)
                .count();
            Line::from(vec![
                Span::styled(" W", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", w.id),
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("  Actors: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", actor_count),
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("  Msgs: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format_num(w.messages_processed),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("  ({} m/s)", format_rate(w.msg_rate)),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled("  Mailbox: ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{}", w.mailbox_depth),
                    Style::default()
                        .fg(Color::Blue)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("  Panics: ", Style::default().fg(Color::DarkGray)),
                Span::styled(format!("{}", w.panics), panics_style),
            ])
        }
        None => Line::from(Span::styled(
            " No workers",
            Style::default().fg(Color::DarkGray),
        )),
    };

    f.render_widget(Paragraph::new(line), area);
}

/// Actor table filtered to the focused worker.
fn draw_focused_actor_table(f: &mut Frame, app: &App, table_state: &mut TableState, area: Rect) {
    let sort_arrow = if app.sort_desc { " \u{25bc}" } else { " \u{25b2}" };

    let columns = [SortColumn::Address, SortColumn::Mailbox, SortColumn::MsgCount, SortColumn::LastMsg];
    let header_cells = columns.iter().map(|&col| {
        let mut label = col.label().to_string();
        if col == app.sort_column {
            label.push_str(sort_arrow);
        }
        Cell::from(label).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1);

    let focused_rows: Vec<&super::app::ActorRow> = app.focused_actor_rows();

    let rows: Vec<Row> = focused_rows
        .iter()
        .map(|a| {
            let addr = format!("{}", a.address);
            let addr_short = if a.poisoned {
                let s = if addr.len() > 16 {
                    format!("!{}...", &addr[..14])
                } else {
                    format!("!{}", addr)
                };
                s
            } else if addr.len() > 18 {
                format!("{}...", &addr[..16])
            } else {
                addr
            };
            let addr_style = if a.poisoned {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };
            let msg_short = short_type_name(a.last_msg_type.as_deref());
            let msg_style = if a.poisoned {
                Style::default().fg(Color::Red)
            } else if a.last_msg_type.is_some() {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            Row::new(vec![
                Cell::from(addr_short).style(addr_style),
                Cell::from(format!("{}", a.mailbox_depth)),
                Cell::from(format!("{}", a.messages_processed)),
                Cell::from(msg_short).style(msg_style),
            ])
        })
        .collect();

    // Don't select any row in the focused table — it's read-only
    table_state.select(None);

    let help_text = " Esc/\u{2190}: overview  \u{2191}\u{2193}: select worker  s: sort  r: reverse  Tab: overview  q: quit";

    let table = Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(28),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!(" W{} Actors ", app.focused_worker))
            .title_bottom(Line::from(help_text).centered()),
    );

    f.render_stateful_widget(table, area, table_state);
}

// ─── Distribution View ──────────────────────────────────────────────────────

#[cfg(feature = "distribution")]
fn draw_distribution(f: &mut Frame, app: &App, table_state: &mut TableState) {
    let chunks = Layout::vertical([
        Constraint::Length(3),  // Summary bar
        Constraint::Fill(1),   // Members table
        Constraint::Length(10), // Bottom panels: cache + routing
        Constraint::Length(1),  // Help bar
    ])
    .split(f.area());

    draw_dist_summary(f, app, chunks[0]);
    draw_dist_members(f, app, table_state, chunks[1]);

    let bottom = Layout::horizontal([
        Constraint::Percentage(40),
        Constraint::Percentage(60),
    ])
    .split(chunks[2]);

    draw_dist_cache(f, app, bottom[0]);
    draw_dist_routing(f, app, bottom[1]);
    draw_dist_help(f, chunks[3]);
}

#[cfg(feature = "distribution")]
fn draw_dist_summary(f: &mut Frame, app: &App, area: Rect) {
    let (node_id, listen_addr, alive, suspect, dead, cache, dir, rt_size, rt_buckets, repair) =
        match &app.distribution {
            Some(d) => (
                &d.node_id[..d.node_id.len().min(16)],
                d.listen_addr.as_str(),
                d.alive_count,
                d.suspect_count,
                d.dead_count,
                d.cache_size,
                d.directory_entry_count,
                d.routing_table_size,
                d.routing_buckets.len(),
                d.repair_queue_size,
            ),
            None => ("—", "—", 0, 0, 0, 0, 0, 0, 0, 0),
        };

    let lines = vec![
        Line::from(vec![
            Span::styled("  Node: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                node_id.to_string(),
                Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
            ),
            Span::styled("  Addr: ", Style::default().fg(Color::DarkGray)),
            Span::styled(listen_addr.to_string(), Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::styled("  Members: ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{alive}"), Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::styled(" alive, ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{suspect}"),
                if suspect > 0 { Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { Style::default().fg(Color::DarkGray) },
            ),
            Span::styled(" suspect, ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{dead}"),
                if dead > 0 { Style::default().fg(Color::Red).add_modifier(Modifier::BOLD) } else { Style::default().fg(Color::DarkGray) },
            ),
            Span::styled(" dead", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("   Cache: {cache}"), Style::default().fg(Color::DarkGray)),
            Span::styled(format!("  Directory: {dir}"), Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::styled(format!("  Routing: {rt_size} nodes, {rt_buckets} buckets"), Style::default().fg(Color::DarkGray)),
            Span::styled(format!("   Repair queue: {repair}"), Style::default().fg(Color::DarkGray)),
        ]),
    ];

    let block = Block::default().borders(Borders::ALL).title(" Distribution ");
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

#[cfg(feature = "distribution")]
fn draw_dist_members(f: &mut Frame, app: &App, table_state: &mut TableState, area: Rect) {
    let header_cells = ["STATE", "NODE ID", "ADDRESS", "INCARNATION"].iter().map(|&h| {
        Cell::from(h).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1);

    let rows: Vec<Row> = match &app.distribution {
        Some(d) => d
            .members
            .iter()
            .map(|m| {
                let state_style = match m.state.as_str() {
                    "alive" => Style::default().fg(Color::Green),
                    "suspect" => Style::default().fg(Color::Yellow),
                    "dead" => Style::default().fg(Color::Red),
                    _ => Style::default(),
                };
                let id_short = if m.node_id.len() > 16 {
                    format!("{}...", &m.node_id[..14])
                } else {
                    m.node_id.clone()
                };
                Row::new(vec![
                    Cell::from(m.state.clone()).style(state_style),
                    Cell::from(id_short),
                    Cell::from(m.addr.clone()),
                    Cell::from(format!("{}", m.incarnation)),
                ])
            })
            .collect(),
        None => vec![],
    };

    table_state.select(Some(app.dist_member_selected));

    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Min(18),
            Constraint::Length(22),
            Constraint::Length(12),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Members "),
    )
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("> ");

    f.render_stateful_widget(table, area, table_state);
}

#[cfg(feature = "distribution")]
fn draw_dist_cache(f: &mut Frame, app: &App, area: Rect) {
    let rows: Vec<Row> = match &app.distribution {
        Some(d) => d
            .cache_entries
            .iter()
            .take(area.height.saturating_sub(2) as usize)
            .map(|e| {
                let actor_short = if e.actor_addr.len() > 16 {
                    format!("{}...", &e.actor_addr[..14])
                } else {
                    e.actor_addr.clone()
                };
                let node_short = if e.node_id.len() > 12 {
                    format!("{}...", &e.node_id[..10])
                } else {
                    e.node_id.clone()
                };
                Row::new(vec![
                    Cell::from(actor_short),
                    Cell::from(node_short),
                ])
            })
            .collect(),
        None => vec![],
    };

    let header = Row::new(vec![
        Cell::from("ACTOR").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Cell::from("NODE").style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
    ])
    .height(1);

    let table = Table::new(
        rows,
        [Constraint::Percentage(55), Constraint::Percentage(45)],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(" Cache "));

    f.render_widget(table, area);
}

#[cfg(feature = "distribution")]
fn draw_dist_routing(f: &mut Frame, app: &App, area: Rect) {
    let buckets: Vec<(usize, usize)> = match &app.distribution {
        Some(d) => d.routing_buckets.clone(),
        None => vec![],
    };

    let max_count = buckets.iter().map(|(_, c)| *c).max().unwrap_or(1).max(1);
    let bar_max_width = area.width.saturating_sub(16) as usize; // space for "[NNN] " + " N"

    let lines: Vec<Line> = buckets
        .iter()
        .take(area.height.saturating_sub(2) as usize)
        .map(|(idx, count)| {
            let bar_len = (*count as f64 / max_count as f64 * bar_max_width as f64).round() as usize;
            let bar_len = bar_len.max(1);
            Line::from(vec![
                Span::styled(
                    format!(" [{:>3}] ", idx),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    "\u{2588}".repeat(bar_len),
                    Style::default().fg(Color::Cyan),
                ),
                Span::styled(
                    format!(" {}", count),
                    Style::default().fg(Color::White),
                ),
            ])
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Routing Buckets ");
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

#[cfg(feature = "distribution")]
fn draw_dist_help(f: &mut Frame, area: Rect) {
    let help = Line::from(vec![
        Span::styled(
            " Tab: views  \u{2191}\u{2193}: scroll  Esc: overview  q: quit",
            Style::default().fg(Color::DarkGray),
        ),
    ]);
    f.render_widget(Paragraph::new(help), area);
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn short_type_name(full: Option<&str>) -> String {
    match full {
        Some(s) => s.rsplit("::").next().unwrap_or(s).to_string(),
        None => "\u{2014}".to_string(), // em dash
    }
}

fn format_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_rate(rate: f64) -> String {
    if rate >= 1_000_000.0 {
        format!("{:.1}M", rate / 1_000_000.0)
    } else if rate >= 1_000.0 {
        format!("{:.1}k", rate / 1_000.0)
    } else {
        format!("{:.0}", rate)
    }
}
