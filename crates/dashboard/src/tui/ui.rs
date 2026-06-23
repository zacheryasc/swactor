use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Sparkline, Table, TableState};

use super::app::{App, SortColumn, ViewMode};

/// Render the full dashboard into the given frame.
pub fn draw(f: &mut Frame, app: &App, table_state: &mut TableState) {
    match app.view_mode {
        ViewMode::Overview => draw_overview(f, app, table_state),
        ViewMode::WorkerDetail => draw_worker_detail(f, app, table_state),
        ViewMode::ActorDetail => draw_actor_detail(f, app),
    }
}

// ─── Overview ────────────────────────────────────────────────────────────────

fn draw_overview(f: &mut Frame, app: &App, table_state: &mut TableState) {
    let num_workers = app.workers.len().max(1);
    let has_sparkline_data = app.workers.iter().any(|w| w.sparkline_rates.len() > 1);
    let sparkline_height = if has_sparkline_data { 4u16 } else { 0u16 };

    let chunks = Layout::vertical([
        Constraint::Length(num_workers as u16),
        Constraint::Length(sparkline_height),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .split(f.area());

    draw_worker_bars(f, app, chunks[0]);
    if has_sparkline_data {
        draw_worker_sparklines(f, app, chunks[1]);
    }
    draw_summary(f, app, chunks[2]);
    draw_actor_table(f, app, table_state, chunks[3]);
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

/// Render per-worker sparklines showing message rate trends.
fn draw_worker_sparklines(f: &mut Frame, app: &App, area: Rect) {
    if app.workers.is_empty() {
        return;
    }
    // Split area horizontally: one sparkline per worker
    let constraints: Vec<Constraint> = app
        .workers
        .iter()
        .map(|_| Constraint::Ratio(1, app.workers.len() as u32))
        .collect();
    let cols = Layout::horizontal(constraints).split(area);

    for (i, w) in app.workers.iter().enumerate() {
        if let Some(&col_area) = cols.get(i) {
            let block = Block::default().borders(Borders::NONE).title(Span::styled(
                format!(" W{} ", w.id),
                Style::default().fg(Color::DarkGray),
            ));
            let sparkline = Sparkline::default()
                .block(block)
                .data(&w.sparkline_rates)
                .style(Style::default().fg(Color::Green));
            f.render_widget(sparkline, col_area);
        }
    }
}

fn build_worker_line(w: &super::app::WorkerView, total_width: usize) -> Line<'static> {
    let id_str = format!("{:>3}", w.id);
    let suffix = format!("  {} actors  {} m/s", w.num_actors, format_rate(w.msg_rate),);

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
            spans.push(Span::styled("|".repeat(count), Style::default().fg(color)));
        }
    }

    let filled_total: usize = phase_cells.iter().sum();
    let empty = bar_width.saturating_sub(filled_total);
    if empty >= pct_str.len() {
        let padding = empty - pct_str.len();
        spans.push(Span::raw(" ".repeat(padding)));
        spans.push(Span::styled(
            pct_str,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ));
    } else {
        spans.push(Span::styled(
            pct_str,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
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

    let mut spans = vec![
        Span::styled(" Workers: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", app.num_workers),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Actors: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", app.total_actors),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Msgs: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_num(app.total_messages),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Mailbox: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", app.total_mailbox),
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  Panics: ", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{}", app.total_panics), panics_style),
    ];

    if !app.warnings.is_empty() {
        spans.push(Span::styled(
            "  Warnings: ",
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(
            format!("{}", app.warnings.len()),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    }

    let line = Line::from(spans);
    f.render_widget(Paragraph::new(line), area);
}

/// Render the actor table with scrolling, selection, and search.
fn draw_actor_table(f: &mut Frame, app: &App, table_state: &mut TableState, area: Rect) {
    // Split area: optional search bar + table
    let has_search = app.search_active || !app.search_query.is_empty();
    let search_height = if has_search { 1u16 } else { 0 };
    let chunks =
        Layout::vertical([Constraint::Length(search_height), Constraint::Fill(1)]).split(area);

    // Draw search bar
    if has_search {
        let search_style = if app.search_active {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let cursor = if app.search_active { "\u{2588}" } else { "" };
        let prefix = if app.search_locked {
            " [locked] /"
        } else {
            " /"
        };
        let line = Line::from(vec![
            Span::styled(prefix, Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{}{}", app.search_query, cursor), search_style),
        ]);
        f.render_widget(Paragraph::new(line), chunks[0]);
    }

    let sort_arrow = if app.sort_desc {
        " \u{25bc}"
    } else {
        " \u{25b2}"
    };

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

    let visible = app.visible_actor_rows();
    let rows: Vec<Row> = visible.iter().map(|a| actor_row_cells(a)).collect();

    table_state.select(Some(app.selected));

    let help_text = " q: quit  /: search  \u{2191}\u{2193}: scroll  s: sort  r: reverse  Tab: worker view  Enter: detail";

    let title = if !app.search_query.is_empty() {
        format!(
            " Actors ({} of {} matching \"{}\") ",
            visible.len(),
            app.actor_rows.len(),
            app.search_query,
        )
    } else {
        format!(
            " Actors (sorted by {}{}) ",
            app.sort_column.label(),
            sort_arrow
        )
    };

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
            .title(title)
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
    let addr_short = if let Some(ref name) = a.name {
        if a.poisoned {
            format!("!{}", name)
        } else {
            name.clone()
        }
    } else {
        let addr = format!("{}", a.address);
        if a.poisoned {
            if addr.len() > 16 {
                format!("!{}...", &addr[..14])
            } else {
                format!("!{}", addr)
            }
        } else if addr.len() > 18 {
            format!("{}...", &addr[..16])
        } else {
            addr
        }
    };
    let addr_style = if a.poisoned {
        Style::default().fg(Color::Red)
    } else if a.name.is_some() {
        Style::default().fg(Color::Cyan)
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
    let sort_arrow = if app.sort_desc {
        " \u{25bc}"
    } else {
        " \u{25b2}"
    };

    let columns = [
        SortColumn::Address,
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

    let focused_rows: Vec<&super::app::ActorRow> = app.focused_actor_rows();

    let rows: Vec<Row> = focused_rows
        .iter()
        .map(|a| {
            let addr_short = if let Some(ref name) = a.name {
                if a.poisoned {
                    format!("!{}", name)
                } else {
                    name.clone()
                }
            } else {
                let addr = format!("{}", a.address);
                if a.poisoned {
                    if addr.len() > 16 {
                        format!("!{}...", &addr[..14])
                    } else {
                        format!("!{}", addr)
                    }
                } else if addr.len() > 18 {
                    format!("{}...", &addr[..16])
                } else {
                    addr
                }
            };
            let addr_style = if a.poisoned {
                Style::default().fg(Color::Red)
            } else if a.name.is_some() {
                Style::default().fg(Color::Cyan)
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

// ─── Actor Detail View ──────────────────────────────────────────────────────

fn draw_actor_detail(f: &mut Frame, app: &App) {
    let actor = match app.focused_actor_row() {
        Some(a) => a,
        None => {
            let msg = Paragraph::new(" No actor selected. Press Esc to go back.")
                .style(Style::default().fg(Color::DarkGray));
            f.render_widget(msg, f.area());
            return;
        }
    };

    let has_types = !actor.message_type_counts.is_empty();
    let type_height = if has_types {
        (actor.message_type_counts.len() as u16 + 2).min(10)
    } else {
        0
    };

    let chunks = Layout::vertical([
        Constraint::Length(5),           // Info card
        Constraint::Length(5),           // Rate sparkline
        Constraint::Length(5),           // Mailbox sparkline
        Constraint::Length(type_height), // Type breakdown
        Constraint::Length(1),           // Help bar
        Constraint::Fill(1),             // Logs panel
    ])
    .split(f.area());

    // Info card
    let addr_str = format!("{}", actor.address);
    let status = if actor.poisoned {
        "POISONED"
    } else {
        "Healthy"
    };
    let status_color = if actor.poisoned {
        Color::Red
    } else {
        Color::Green
    };
    let msg_type = actor
        .last_msg_type
        .as_deref()
        .map(|s| short_type_name(Some(s)))
        .unwrap_or_else(|| "\u{2014}".to_string());

    let mut info_lines = Vec::new();
    if let Some(ref name) = actor.name {
        info_lines.push(Line::from(vec![
            Span::styled("  Name: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                name.clone(),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("   Address: ", Style::default().fg(Color::DarkGray)),
            Span::styled(addr_str, Style::default().fg(Color::White)),
        ]));
    } else {
        info_lines.push(Line::from(vec![
            Span::styled("  Address: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                addr_str,
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
    }
    info_lines.push(Line::from(vec![
        Span::styled("  Worker: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("W{}", actor.worker_id),
            Style::default().fg(Color::Cyan),
        ),
        Span::styled("   Status: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            status,
            Style::default()
                .fg(status_color)
                .add_modifier(Modifier::BOLD),
        ),
    ]));
    info_lines.push(Line::from(vec![
        Span::styled("  Messages: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_num(actor.messages_processed),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("   Mailbox: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{}", actor.mailbox_depth),
            Style::default()
                .fg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("   Last Msg: ", Style::default().fg(Color::DarkGray)),
        Span::styled(msg_type, Style::default().fg(Color::Green)),
    ]));

    let info_block = Block::default()
        .borders(Borders::ALL)
        .title(" Actor Detail ");
    let info = Paragraph::new(info_lines).block(info_block);
    f.render_widget(info, chunks[0]);

    // Rate sparkline
    let rate_block = Block::default().borders(Borders::ALL).title(Span::styled(
        " Msg Rate ",
        Style::default().fg(Color::Green),
    ));
    let rate_sparkline = Sparkline::default()
        .block(rate_block)
        .data(&actor.sparkline_rates)
        .style(Style::default().fg(Color::Green));
    f.render_widget(rate_sparkline, chunks[1]);

    // Mailbox sparkline
    let mbox_block = Block::default().borders(Borders::ALL).title(Span::styled(
        " Mailbox Depth ",
        Style::default().fg(Color::Blue),
    ));
    let mbox_sparkline = Sparkline::default()
        .block(mbox_block)
        .data(&actor.sparkline_mailbox)
        .style(Style::default().fg(Color::Blue));
    f.render_widget(mbox_sparkline, chunks[2]);

    // Message type breakdown
    if has_types {
        draw_type_breakdown(f, &actor.message_type_counts, chunks[3]);
    }

    // Help bar
    let level_names = ["ERR", "WARN", "INFO", "DBG", "TRC"];
    let level_colors = [
        Color::Red,
        Color::Yellow,
        Color::Blue,
        Color::DarkGray,
        Color::DarkGray,
    ];
    let mut help_spans: Vec<Span> = vec![Span::styled(
        " Esc: back  \u{2191}\u{2193}: scroll logs  ",
        Style::default().fg(Color::DarkGray),
    )];
    for (i, &name) in level_names.iter().enumerate() {
        let active = app.log_levels[i];
        let color = if active {
            level_colors[i]
        } else {
            Color::DarkGray
        };
        let style = if active {
            Style::default().fg(color).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(color)
        };
        help_spans.push(Span::styled(format!("{}:{} ", i + 1, name), style));
    }
    f.render_widget(Paragraph::new(Line::from(help_spans)), chunks[4]);

    // Logs panel
    draw_actor_logs(f, app, chunks[5]);
}

fn draw_type_breakdown(f: &mut Frame, types: &[(String, u64)], area: Rect) {
    let total: u64 = types.iter().map(|(_, c)| *c).sum();
    let max_count = types.first().map(|(_, c)| *c).unwrap_or(1).max(1);
    let inner_height = area.height.saturating_sub(2) as usize;

    let bar_colors = [
        Color::Green,
        Color::Blue,
        Color::Yellow,
        Color::Magenta,
        Color::Cyan,
        Color::Red,
    ];

    let lines: Vec<Line> = types
        .iter()
        .take(inner_height)
        .enumerate()
        .map(|(i, (name, count))| {
            let short = name.rsplit("::").next().unwrap_or(name);
            let pct = if total > 0 {
                *count as f64 / total as f64 * 100.0
            } else {
                0.0
            };
            let bar_width = 20usize;
            let filled = ((*count as f64 / max_count as f64) * bar_width as f64).round() as usize;
            let color = bar_colors[i % bar_colors.len()];

            Line::from(vec![
                Span::styled(
                    format!(" {:>16} ", short),
                    Style::default().fg(Color::White),
                ),
                Span::styled("\u{2588}".repeat(filled), Style::default().fg(color)),
                Span::styled(
                    " ".repeat(bar_width.saturating_sub(filled)),
                    Style::default(),
                ),
                Span::styled(
                    format!(" {:>8} ", count),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("{:>5.1}%", pct),
                    Style::default().fg(Color::DarkGray),
                ),
            ])
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Message Types ({}) ", types.len()));
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn draw_actor_logs(f: &mut Frame, app: &App, area: Rect) {
    let visible = app.visible_logs();
    let log_count = visible.len();
    let inner_height = area.height.saturating_sub(2) as usize; // borders

    // Compute scroll window
    let scroll = app.log_scroll.min(log_count.saturating_sub(inner_height));

    let lines: Vec<Line> = visible
        .iter()
        .skip(scroll)
        .take(inner_height)
        .map(|e| {
            let level_color = match e.level.as_str() {
                "ERROR" => Color::Red,
                "WARN" => Color::Yellow,
                "INFO" => Color::Blue,
                "DEBUG" => Color::DarkGray,
                "TRACE" => Color::DarkGray,
                _ => Color::White,
            };
            let ts = {
                let secs = e.timestamp_ms / 1000;
                let ms = e.timestamp_ms % 1000;
                let h = (secs / 3600) % 24;
                let m = (secs / 60) % 60;
                let s = secs % 60;
                format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
            };
            Line::from(vec![
                Span::styled(format!(" {} ", ts), Style::default().fg(Color::DarkGray)),
                Span::styled(
                    format!("{:<5} ", e.level),
                    Style::default()
                        .fg(level_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(e.message.clone(), Style::default().fg(Color::White)),
            ])
        })
        .collect();

    let title = format!(" Logs ({}) ", log_count);
    let block = Block::default().borders(Borders::ALL).title(title);
    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
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
