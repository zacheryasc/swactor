use std::collections::HashMap;
use std::f64::consts::PI;

use crate::trace::{GossipEventKind, SimulationTrace};

/// Generate a self-contained HTML report from a simulation trace.
pub fn generate_html_report(trace: &SimulationTrace) -> String {
    let mut html = String::with_capacity(32_000);

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    html.push_str(&format!(
        "<title>Gossip Simulation: {}</title>\n",
        escape_html(&trace.name)
    ));
    html.push_str("<style>\n");
    html.push_str(CSS);
    html.push_str("</style>\n</head>\n<body>\n");

    html.push_str(&format!(
        "<h1>Gossip Simulation: {}</h1>\n",
        escape_html(&trace.name)
    ));

    // Summary metrics
    render_summary(&mut html, trace);

    // Network topology
    render_topology_svg(&mut html, trace);

    // Propagation heatmap
    render_heatmap_svg(&mut html, trace);

    // Convergence curve
    render_convergence_svg(&mut html, trace);

    // Message flow timeline
    render_message_flow_svg(&mut html, trace);

    // Event log table
    render_event_table(&mut html, trace);

    html.push_str("</body>\n</html>\n");
    html
}

// ── CSS ──────────────────────────────────────────────────────────────────

const CSS: &str = r#"
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    max-width: 1200px; margin: 0 auto; padding: 20px;
    background: #fafafa; color: #222;
}
h1 { border-bottom: 3px solid #333; padding-bottom: 8px; }
h2 { margin-top: 32px; color: #444; }
.metrics { display: flex; flex-wrap: wrap; gap: 16px; margin: 16px 0; }
.metric {
    background: #fff; border: 1px solid #ddd; border-radius: 8px;
    padding: 12px 20px; min-width: 140px;
}
.metric .label { font-size: 0.85em; color: #666; }
.metric .value { font-size: 1.5em; font-weight: bold; }
svg { display: block; margin: 12px 0; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; }
th, td { border: 1px solid #ddd; padding: 6px 10px; text-align: left; font-size: 0.85em; }
th { background: #f0f0f0; }
tr:nth-child(even) { background: #fafafa; }
.capped { color: #999; font-style: italic; margin: 4px 0; }
"#;

// ── Summary metrics ──────────────────────────────────────────────────────

fn render_summary(html: &mut String, trace: &SimulationTrace) {
    let num_nodes = trace.node_names.len();
    let total_pushes = trace
        .events
        .iter()
        .filter(|e| matches!(e.kind, GossipEventKind::GossipRoundStarted { .. }))
        .count();
    let redundant_pushes = trace
        .events
        .iter()
        .filter(|e| matches!(e.kind, GossipEventKind::PushReceived { keys_updated: 0, .. }))
        .count();
    let convergence_round = find_convergence_round(trace);

    html.push_str("<h2>Summary</h2>\n<div class=\"metrics\">\n");
    metric(html, "Nodes", &num_nodes.to_string());
    metric(html, "Keys", &trace.total_keys.to_string());
    metric(html, "Rounds", &trace.num_rounds.to_string());
    metric(html, "Pushes", &total_pushes.to_string());
    metric(html, "Redundant", &redundant_pushes.to_string());
    metric(
        html,
        "Converged at",
        &convergence_round
            .map(|r| format!("round {r}"))
            .unwrap_or_else(|| "never".into()),
    );
    if total_pushes > 0 {
        let efficiency = 100.0 * (1.0 - redundant_pushes as f64 / total_pushes as f64);
        metric(html, "Efficiency", &format!("{efficiency:.0}%"));
    }
    html.push_str("</div>\n");
}

fn metric(html: &mut String, label: &str, value: &str) {
    html.push_str(&format!(
        "<div class=\"metric\"><div class=\"label\">{label}</div><div class=\"value\">{value}</div></div>\n"
    ));
}

fn find_convergence_round(trace: &SimulationTrace) -> Option<usize> {
    if trace.total_keys == 0 {
        return Some(0);
    }
    for (round_idx, snapshots) in trace.snapshots_per_round.iter().enumerate() {
        let all_converged = snapshots
            .iter()
            .all(|(_, snap)| snap.entries.len() >= trace.total_keys);
        if all_converged {
            return Some(round_idx + 1);
        }
    }
    None
}

// ── Topology SVG ─────────────────────────────────────────────────────────

fn render_topology_svg(html: &mut String, trace: &SimulationTrace) {
    html.push_str("<h2>Network Topology</h2>\n");

    let n = trace.node_names.len();
    let size = 400.0_f64;
    let cx = size / 2.0;
    let cy = size / 2.0;
    let radius = size / 2.0 - 50.0;

    // Compute node positions in circular layout.
    let positions: Vec<(f64, f64)> = (0..n)
        .map(|i| {
            let angle = 2.0 * PI * (i as f64) / (n as f64) - PI / 2.0;
            (cx + radius * angle.cos(), cy + radius * angle.sin())
        })
        .collect();

    let name_to_idx: HashMap<&str, usize> = trace
        .node_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    html.push_str(&format!(
        "<svg width=\"{size}\" height=\"{size}\" viewBox=\"0 0 {size} {size}\">\n"
    ));
    html.push_str("<defs><marker id=\"arrow\" markerWidth=\"8\" markerHeight=\"6\" refX=\"8\" refY=\"3\" orient=\"auto\"><path d=\"M0,0 L8,3 L0,6\" fill=\"#888\"/></marker></defs>\n");

    // Draw edges.
    for (from_name, to_name) in &trace.topology_edges {
        if let (Some(&fi), Some(&ti)) = (name_to_idx.get(from_name.as_str()), name_to_idx.get(to_name.as_str())) {
            let (x1, y1) = positions[fi];
            let (x2, y2) = positions[ti];
            // Shorten line to not overlap circle.
            let dx = x2 - x1;
            let dy = y2 - y1;
            let len = (dx * dx + dy * dy).sqrt();
            if len > 0.0 {
                let nx = dx / len;
                let ny = dy / len;
                let sx = x1 + nx * 18.0;
                let sy = y1 + ny * 18.0;
                let ex = x2 - nx * 18.0;
                let ey = y2 - ny * 18.0;
                html.push_str(&format!(
                    "<line x1=\"{sx:.1}\" y1=\"{sy:.1}\" x2=\"{ex:.1}\" y2=\"{ey:.1}\" stroke=\"#aaa\" stroke-width=\"1\" marker-end=\"url(#arrow)\"/>\n"
                ));
            }
        }
    }

    // Draw nodes.
    for (i, name) in trace.node_names.iter().enumerate() {
        let (x, y) = positions[i];
        html.push_str(&format!(
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"16\" fill=\"#4a90d9\" stroke=\"#2a5a9d\" stroke-width=\"2\"/>\n"
        ));
        html.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" fill=\"#fff\" font-size=\"10\" font-weight=\"bold\">{name}</text>\n",
            ty = y + 4.0,
        ));
    }

    html.push_str("</svg>\n");
}

// ── Propagation heatmap ──────────────────────────────────────────────────

fn render_heatmap_svg(html: &mut String, trace: &SimulationTrace) {
    html.push_str("<h2>Propagation Heatmap</h2>\n");
    html.push_str("<p>Rows = nodes, columns = rounds. Color intensity = fraction of total keys held.</p>\n");

    let n = trace.node_names.len();
    let rounds = trace.snapshots_per_round.len();
    if rounds == 0 || n == 0 {
        html.push_str("<p>No data.</p>\n");
        return;
    }

    let cell_w = 36.0_f64;
    let cell_h = 28.0_f64;
    let label_w = 80.0_f64;
    let header_h = 28.0_f64;
    let w = label_w + cell_w * rounds as f64 + 10.0;
    let h = header_h + cell_h * n as f64 + 10.0;

    html.push_str(&format!(
        "<svg width=\"{w:.0}\" height=\"{h:.0}\" viewBox=\"0 0 {w:.0} {h:.0}\">\n"
    ));

    // Column headers.
    for r in 0..rounds {
        let x = label_w + r as f64 * cell_w + cell_w / 2.0;
        let ty = header_h - 6.0;
        let label = r + 1;
        html.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{ty}\" text-anchor=\"middle\" font-size=\"10\" fill=\"#666\">R{label}</text>\n"
        ));
    }

    // Build a name→row index for stable ordering.
    let name_to_row: HashMap<&str, usize> = trace
        .node_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    for (r, round_snaps) in trace.snapshots_per_round.iter().enumerate() {
        for (name, snap) in round_snaps {
            if let Some(&row) = name_to_row.get(name.as_str()) {
                let frac = if trace.total_keys > 0 {
                    snap.entries.len() as f64 / trace.total_keys as f64
                } else {
                    0.0
                };
                let x = label_w + r as f64 * cell_w;
                let y = header_h + row as f64 * cell_h;
                let color = heatmap_color(frac);
                html.push_str(&format!(
                    "<rect x=\"{x:.1}\" y=\"{y:.1}\" width=\"{cell_w}\" height=\"{cell_h}\" fill=\"{color}\" stroke=\"#fff\" stroke-width=\"1\"/>\n"
                ));
                // Show count inside cell.
                let text_color = if frac > 0.5 { "#fff" } else { "#333" };
                let tx = x + cell_w / 2.0;
                let ty = y + cell_h / 2.0 + 3.0;
                let count = snap.entries.len();
                html.push_str(&format!(
                    "<text x=\"{tx:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" font-size=\"10\" fill=\"{text_color}\">{count}</text>\n"
                ));
            }
        }
    }

    // Row labels.
    for (i, name) in trace.node_names.iter().enumerate() {
        let y = header_h + i as f64 * cell_h + cell_h / 2.0 + 4.0;
        html.push_str(&format!(
            "<text x=\"{x}\" y=\"{y:.1}\" font-size=\"11\" fill=\"#333\">{name}</text>\n",
            x = 4.0,
        ));
    }

    html.push_str("</svg>\n");
}

fn heatmap_color(frac: f64) -> String {
    // Interpolate from light (#e8f4e8) to deep green (#1a7a1a).
    let f = frac.clamp(0.0, 1.0);
    let r = (232.0 + f * (26.0 - 232.0)) as u8;
    let g = (244.0 + f * (122.0 - 244.0)) as u8;
    let b = (232.0 + f * (26.0 - 232.0)) as u8;
    format!("#{r:02x}{g:02x}{b:02x}")
}

// ── Convergence curve ────────────────────────────────────────────────────

fn render_convergence_svg(html: &mut String, trace: &SimulationTrace) {
    html.push_str("<h2>Convergence Curve</h2>\n");
    html.push_str("<p>Percentage of nodes that hold all keys vs. round number.</p>\n");

    let rounds = trace.snapshots_per_round.len();
    if rounds == 0 {
        html.push_str("<p>No data.</p>\n");
        return;
    }

    let chart_w = 600.0_f64;
    let chart_h = 300.0_f64;
    let margin_l = 50.0_f64;
    let margin_b = 40.0_f64;
    let margin_t = 20.0_f64;
    let margin_r = 20.0_f64;
    let w = chart_w + margin_l + margin_r;
    let h = chart_h + margin_t + margin_b;

    html.push_str(&format!(
        "<svg width=\"{w:.0}\" height=\"{h:.0}\" viewBox=\"0 0 {w:.0} {h:.0}\">\n"
    ));

    // Axes.
    html.push_str(&format!(
        "<line x1=\"{margin_l}\" y1=\"{margin_t}\" x2=\"{margin_l}\" y2=\"{}\" stroke=\"#333\" stroke-width=\"1\"/>\n",
        margin_t + chart_h,
    ));
    html.push_str(&format!(
        "<line x1=\"{margin_l}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" stroke=\"#333\" stroke-width=\"1\"/>\n",
        margin_t + chart_h,
        margin_l + chart_w,
        margin_t + chart_h,
    ));

    // Y-axis labels.
    for pct in [0, 25, 50, 75, 100] {
        let y = margin_t + chart_h - (pct as f64 / 100.0) * chart_h;
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"10\" fill=\"#666\">{pct}%</text>\n",
            margin_l - 6.0, y + 3.0,
        ));
        html.push_str(&format!(
            "<line x1=\"{margin_l}\" y1=\"{y:.1}\" x2=\"{}\" y2=\"{y:.1}\" stroke=\"#eee\" stroke-width=\"1\"/>\n",
            margin_l + chart_w,
        ));
    }

    // X-axis labels.
    let step = (rounds / 10).max(1);
    for r in (0..rounds).step_by(step) {
        let x = margin_l + (r as f64 + 0.5) / rounds as f64 * chart_w;
        html.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{}\" text-anchor=\"middle\" font-size=\"10\" fill=\"#666\">{}</text>\n",
            margin_t + chart_h + 16.0, r + 1,
        ));
    }
    // X-axis title.
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\" font-size=\"11\" fill=\"#444\">Round</text>\n",
        margin_l + chart_w / 2.0,
        margin_t + chart_h + 34.0,
    ));

    // Compute data points.
    let n = trace.node_names.len();
    let mut points = Vec::with_capacity(rounds);
    for round_snaps in &trace.snapshots_per_round {
        let converged = round_snaps
            .iter()
            .filter(|(_, snap)| snap.entries.len() >= trace.total_keys && trace.total_keys > 0)
            .count();
        let pct = if n > 0 {
            converged as f64 / n as f64 * 100.0
        } else {
            0.0
        };
        points.push(pct);
    }

    // Draw line.
    let mut path = String::new();
    for (i, &pct) in points.iter().enumerate() {
        let x = margin_l + (i as f64 + 0.5) / rounds as f64 * chart_w;
        let y = margin_t + chart_h - (pct / 100.0) * chart_h;
        if i == 0 {
            path.push_str(&format!("M{x:.1},{y:.1}"));
        } else {
            path.push_str(&format!(" L{x:.1},{y:.1}"));
        }
    }
    html.push_str(&format!(
        "<path d=\"{path}\" fill=\"none\" stroke=\"#4a90d9\" stroke-width=\"2\"/>\n"
    ));

    // Draw dots.
    for (i, &pct) in points.iter().enumerate() {
        let x = margin_l + (i as f64 + 0.5) / rounds as f64 * chart_w;
        let y = margin_t + chart_h - (pct / 100.0) * chart_h;
        html.push_str(&format!(
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"3\" fill=\"#4a90d9\"/>\n"
        ));
    }

    html.push_str("</svg>\n");
}

// ── Message flow timeline ────────────────────────────────────────────────

fn render_message_flow_svg(html: &mut String, trace: &SimulationTrace) {
    html.push_str("<h2>Message Flow Timeline</h2>\n");
    html.push_str("<p>Arrows show Push messages from sender to receiver, grouped by round.</p>\n");

    let n = trace.node_names.len();
    let rounds = trace.num_rounds;
    if n == 0 || rounds == 0 {
        html.push_str("<p>No data.</p>\n");
        return;
    }

    let name_to_col: HashMap<&str, usize> = trace
        .node_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();

    // Collect message arrows grouped by round.
    let mut arrows_per_round: Vec<Vec<(usize, usize)>> = vec![Vec::new(); rounds];
    for event in &trace.events {
        if let GossipEventKind::PushReceived { ref from_name, .. } = event.kind {
            let round_idx = event.tick.saturating_sub(1) as usize;
            if round_idx < rounds {
                if let (Some(&from_col), Some(&to_col)) = (
                    name_to_col.get(from_name.as_str()),
                    name_to_col.get(event.node_name.as_str()),
                ) {
                    arrows_per_round[round_idx].push((from_col, to_col));
                }
            }
        }
    }

    let col_w = 80.0_f64;
    let row_h = 40.0_f64;
    let header_h = 30.0_f64;
    let label_h = 24.0_f64;
    let svg_w = col_w * n as f64 + 40.0;
    let svg_h = header_h + label_h + row_h * rounds as f64 + 20.0;

    html.push_str(&format!(
        "<svg width=\"{svg_w:.0}\" height=\"{svg_h:.0}\" viewBox=\"0 0 {svg_w:.0} {svg_h:.0}\">\n"
    ));
    html.push_str("<defs><marker id=\"flow-arrow\" markerWidth=\"8\" markerHeight=\"6\" refX=\"8\" refY=\"3\" orient=\"auto\"><path d=\"M0,0 L8,3 L0,6\" fill=\"#d94a4a\"/></marker></defs>\n");

    // Column headers (node names).
    for (i, name) in trace.node_names.iter().enumerate() {
        let x = 20.0 + i as f64 * col_w + col_w / 2.0;
        html.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{label_h:.0}\" text-anchor=\"middle\" font-size=\"11\" font-weight=\"bold\" fill=\"#333\">{name}</text>\n"
        ));
        // Vertical lifeline.
        let y_start = header_h + label_h;
        let y_end = header_h + label_h + row_h * rounds as f64;
        html.push_str(&format!(
            "<line x1=\"{x:.1}\" y1=\"{y_start:.0}\" x2=\"{x:.1}\" y2=\"{y_end:.0}\" stroke=\"#ddd\" stroke-width=\"1\" stroke-dasharray=\"4,3\"/>\n"
        ));
    }

    // Round labels and arrows.
    for (r, arrows) in arrows_per_round.iter().enumerate() {
        let y = header_h + label_h + r as f64 * row_h + row_h / 2.0;
        // Round label on left.
        html.push_str(&format!(
            "<text x=\"4\" y=\"{y:.1}\" font-size=\"9\" fill=\"#999\">R{}</text>\n",
            r + 1,
        ));

        for &(from_col, to_col) in arrows {
            let x1 = 20.0 + from_col as f64 * col_w + col_w / 2.0;
            let x2 = 20.0 + to_col as f64 * col_w + col_w / 2.0;
            // Offset slightly so overlapping arrows are visible.
            let offset = if from_col < to_col { -3.0 } else { 3.0 };
            html.push_str(&format!(
                "<line x1=\"{x1:.1}\" y1=\"{y1:.1}\" x2=\"{x2:.1}\" y2=\"{y2:.1}\" stroke=\"#d94a4a\" stroke-width=\"1.5\" marker-end=\"url(#flow-arrow)\"/>\n",
                y1 = y + offset,
                y2 = y + offset,
            ));
        }
    }

    html.push_str("</svg>\n");
}

// ── Event log table ──────────────────────────────────────────────────────

fn render_event_table(html: &mut String, trace: &SimulationTrace) {
    html.push_str("<h2>Event Log</h2>\n");

    let max_rows = 500;
    let events: Vec<_> = trace
        .events
        .iter()
        .filter(|e| !matches!(e.kind, GossipEventKind::StateSnapshot { .. }))
        .collect();

    let total = events.len();
    let display = events.iter().take(max_rows);

    html.push_str("<table>\n<tr><th>Round</th><th>Node</th><th>Event</th><th>Details</th></tr>\n");
    for event in display {
        let (kind_str, detail) = format_event_kind(&event.kind);
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{kind_str}</td><td>{detail}</td></tr>\n",
            event.tick,
            escape_html(&event.node_name),
        ));
    }
    html.push_str("</table>\n");

    if total > max_rows {
        html.push_str(&format!(
            "<p class=\"capped\">Showing {max_rows} of {total} events.</p>\n"
        ));
    }
}

fn format_event_kind(kind: &GossipEventKind) -> (&'static str, String) {
    match kind {
        GossipEventKind::LocalSet { key } => ("LocalSet", format!("key={}", escape_html(key))),
        GossipEventKind::GossipRoundStarted { target_name } => {
            ("GossipRound", format!("→ {}", escape_html(target_name)))
        }
        GossipEventKind::GossipRoundNoPeers => ("GossipRound", "no peers".into()),
        GossipEventKind::PushReceived {
            from_name,
            keys_updated,
        } => (
            "PushReceived",
            format!(
                "from {} ({keys_updated} updated)",
                escape_html(from_name)
            ),
        ),
        GossipEventKind::QueryReceived { key } => {
            ("Query", format!("key={}", escape_html(key)))
        }
        GossipEventKind::PeerAdded { peer_name } => {
            ("PeerAdded", escape_html(peer_name))
        }
        GossipEventKind::PeerRemoved { peer_name } => {
            ("PeerRemoved", escape_html(peer_name))
        }
        GossipEventKind::StateSnapshot { .. } => ("Snapshot", String::new()),
    }
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
