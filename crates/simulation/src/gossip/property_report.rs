use crate::properties::PropertyResult;

use super::properties::GossipMetrics;

// ── Public API ──────────────────────────────────────────────────────────────

/// A named scenario with its metrics and property results.
pub struct ScenarioReport {
    pub name: String,
    pub description: String,
    pub metrics: GossipMetrics,
    pub results: Vec<PropertyResult>,
}

/// A section groups related scenarios under a category heading.
pub struct ReportSection {
    pub title: String,
    pub explanation: String,
    pub scenarios: Vec<ScenarioReport>,
}

/// Data for the scalability scatter plot.
pub struct ScalingPoint {
    pub n: usize,
    pub convergence_round: Option<usize>,
    pub total_pushes: usize,
    pub label: String,
}

/// Full report data.
pub struct PropertyReportData {
    pub sections: Vec<ReportSection>,
    pub scaling_points_st: Vec<ScalingPoint>,
    pub scaling_points_mt: Vec<ScalingPoint>,
    pub thread_comparison: Vec<ThreadComparison>,
    /// All convergence curves keyed by scenario name, for the multi-line overlay.
    pub convergence_overlays: Vec<(String, Vec<f64>)>,
}

pub struct ThreadComparison {
    pub label: String,
    pub num_threads: usize,
    pub convergence_round: Option<usize>,
    pub total_pushes: usize,
}

/// Generate a self-contained HTML report from the collected data.
pub fn generate_property_report(data: &PropertyReportData) -> String {
    let mut html = String::with_capacity(128_000);

    html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    html.push_str("<title>Gossip Protocol Property Verification Report</title>\n");
    html.push_str("<style>\n");
    html.push_str(CSS);
    html.push_str("</style>\n</head>\n<body>\n");

    html.push_str("<h1>Gossip Protocol Property Verification Report</h1>\n");

    // Executive summary.
    render_executive_summary(&mut html, data);

    // Per-section content.
    for section in &data.sections {
        render_section(&mut html, section);
    }

    // Convergence overlay chart.
    if !data.convergence_overlays.is_empty() {
        render_convergence_overlay(&mut html, &data.convergence_overlays);
    }

    // Scalability charts.
    if !data.scaling_points_st.is_empty() {
        render_scalability_section(&mut html, data);
    }

    // Thread comparison.
    if !data.thread_comparison.is_empty() {
        render_thread_comparison(&mut html, &data.thread_comparison);
    }

    html.push_str("</body>\n</html>\n");
    html
}

// ── CSS ─────────────────────────────────────────────────────────────────────

const CSS: &str = r#"
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    max-width: 1400px; margin: 0 auto; padding: 20px;
    background: #fafafa; color: #222;
}
h1 { border-bottom: 3px solid #333; padding-bottom: 8px; }
h2 { margin-top: 40px; color: #333; border-bottom: 2px solid #ddd; padding-bottom: 4px; }
h3 { color: #555; margin-top: 24px; }
.summary-grid { display: flex; flex-wrap: wrap; gap: 16px; margin: 16px 0; }
.summary-card {
    background: #fff; border: 1px solid #ddd; border-radius: 8px;
    padding: 16px 24px; min-width: 160px;
}
.summary-card .label { font-size: 0.85em; color: #666; }
.summary-card .value { font-size: 1.8em; font-weight: bold; }
.badge-pass { display: inline-block; padding: 4px 12px; border-radius: 12px; background: #28a745; color: #fff; font-weight: bold; font-size: 0.9em; }
.badge-fail { display: inline-block; padding: 4px 12px; border-radius: 12px; background: #dc3545; color: #fff; font-weight: bold; font-size: 0.9em; }
.badge-partial { display: inline-block; padding: 4px 12px; border-radius: 12px; background: #ffc107; color: #333; font-weight: bold; font-size: 0.9em; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; }
th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; font-size: 0.9em; }
th { background: #f0f0f0; }
tr:nth-child(even) { background: #fafafa; }
.pass { color: #28a745; font-weight: bold; }
.fail { color: #dc3545; font-weight: bold; }
svg { display: block; margin: 12px 0; }
.explanation { color: #555; margin: 8px 0 16px 0; line-height: 1.5; }
.scenario-desc { color: #777; font-style: italic; margin: 4px 0 8px 0; }
"#;

// ── Executive summary ───────────────────────────────────────────────────────

fn render_executive_summary(html: &mut String, data: &PropertyReportData) {
    html.push_str("<h2>Executive Summary</h2>\n");

    let mut total_pass = 0usize;
    let mut total_fail = 0usize;
    for section in &data.sections {
        for scenario in &section.scenarios {
            for r in &scenario.results {
                if r.passed {
                    total_pass += 1;
                } else {
                    total_fail += 1;
                }
            }
        }
    }
    let total = total_pass + total_fail;

    let badge = if total_fail == 0 {
        "<span class=\"badge-pass\">ALL PASSED</span>"
    } else if total_pass == 0 {
        "<span class=\"badge-fail\">ALL FAILED</span>"
    } else {
        "<span class=\"badge-partial\">PARTIAL</span>"
    };

    html.push_str("<div class=\"summary-grid\">\n");
    summary_card(html, "Total Checks", &total.to_string());
    summary_card(html, "Passed", &total_pass.to_string());
    summary_card(html, "Failed", &total_fail.to_string());
    html.push_str(&format!(
        "<div class=\"summary-card\"><div class=\"label\">Verdict</div><div class=\"value\">{badge}</div></div>\n"
    ));
    html.push_str("</div>\n");
}

fn summary_card(html: &mut String, label: &str, value: &str) {
    html.push_str(&format!(
        "<div class=\"summary-card\"><div class=\"label\">{label}</div><div class=\"value\">{value}</div></div>\n"
    ));
}

// ── Section rendering ───────────────────────────────────────────────────────

fn render_section(html: &mut String, section: &ReportSection) {
    html.push_str(&format!("<h2>{}</h2>\n", esc(&section.title)));
    html.push_str(&format!(
        "<p class=\"explanation\">{}</p>\n",
        esc(&section.explanation)
    ));

    for scenario in &section.scenarios {
        html.push_str(&format!("<h3>{}</h3>\n", esc(&scenario.name)));
        html.push_str(&format!(
            "<p class=\"scenario-desc\">{}</p>\n",
            esc(&scenario.description)
        ));

        // Results table.
        html.push_str("<table>\n<tr><th>Property</th><th>Status</th><th>Expected</th><th>Actual</th><th>Description</th></tr>\n");
        for r in &scenario.results {
            let status = if r.passed {
                "<span class=\"pass\">PASS</span>"
            } else {
                "<span class=\"fail\">FAIL</span>"
            };
            html.push_str(&format!(
                "<tr><td>{}</td><td>{status}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                esc(&r.name),
                esc(&r.expected),
                esc(&r.actual),
                esc(&r.description),
            ));
        }
        html.push_str("</table>\n");

        // Inline SVG graph for this scenario based on category.
        render_scenario_graph(html, section, scenario);
    }
}

// ── Per-scenario graphs ─────────────────────────────────────────────────────

fn render_scenario_graph(html: &mut String, section: &ReportSection, scenario: &ScenarioReport) {
    let m = &scenario.metrics;
    match section.title.as_str() {
        "Convergence" | "Fault Tolerance" => {
            render_convergence_curve_svg(html, &scenario.name, &m.convergence_curve);
        }
        "Consistency" => {
            render_entropy_chart(html, &m.entropy_per_round);
        }
        "Practical" => {
            render_state_size_chart(html, &m.avg_state_size_per_round);
        }
        "Bandwidth/Load" => {
            render_load_bar_chart(html, m);
        }
        "Message Complexity" => {
            render_message_stacked_bar(html, m);
        }
        "Peer Selection" => {
            render_peer_histogram(html, m);
        }
        _ => {}
    }
}

// ── SVG chart helpers ───────────────────────────────────────────────────────

const CHART_W: f64 = 700.0;
const CHART_H: f64 = 280.0;
const ML: f64 = 60.0; // margin left
const MR: f64 = 20.0;
const MT: f64 = 20.0;
const MB: f64 = 50.0;

fn svg_open(html: &mut String, w: f64, h: f64) {
    html.push_str(&format!(
        "<svg width=\"{w:.0}\" height=\"{h:.0}\" viewBox=\"0 0 {w:.0} {h:.0}\">\n"
    ));
}

fn svg_close(html: &mut String) {
    html.push_str("</svg>\n");
}

fn draw_axes(html: &mut String) {
    let bx = ML;
    let by = MT + CHART_H;
    let rx = ML + CHART_W - ML;
    html.push_str(&format!(
        "<line x1=\"{bx}\" y1=\"{MT}\" x2=\"{bx}\" y2=\"{by}\" stroke=\"#333\" stroke-width=\"1\"/>\n"
    ));
    html.push_str(&format!(
        "<line x1=\"{bx}\" y1=\"{by}\" x2=\"{rx}\" y2=\"{by}\" stroke=\"#333\" stroke-width=\"1\"/>\n"
    ));
}

fn y_for(val: f64, max_val: f64) -> f64 {
    if max_val < 1e-9 {
        return MT + CHART_H;
    }
    MT + CHART_H - (val / max_val) * CHART_H
}

fn x_for(idx: usize, total: usize) -> f64 {
    if total == 0 {
        return ML;
    }
    ML + (idx as f64 + 0.5) / total as f64 * (CHART_W - ML - MR)
}

// ── Convergence curve SVG ───────────────────────────────────────────────────

fn render_convergence_curve_svg(html: &mut String, _name: &str, curve: &[f64]) {
    if curve.is_empty() {
        return;
    }
    let total_w = CHART_W + MR;
    let total_h = CHART_H + MT + MB;
    svg_open(html, total_w, total_h);
    draw_axes(html);

    // Y-axis labels (0% to 100%).
    for pct in [0, 25, 50, 75, 100] {
        let y = y_for(pct as f64 / 100.0, 1.0);
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"10\" fill=\"#666\">{pct}%</text>\n",
            ML - 6.0, y + 3.0,
        ));
        html.push_str(&format!(
            "<line x1=\"{ML}\" y1=\"{y:.1}\" x2=\"{}\" y2=\"{y:.1}\" stroke=\"#eee\" stroke-width=\"1\"/>\n",
            ML + CHART_W - ML - MR,
        ));
    }

    // X-axis labels.
    let step = (curve.len() / 10).max(1);
    for r in (0..curve.len()).step_by(step) {
        let x = x_for(r, curve.len());
        html.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{}\" text-anchor=\"middle\" font-size=\"10\" fill=\"#666\">{}</text>\n",
            MT + CHART_H + 16.0, r + 1,
        ));
    }

    // Line.
    let mut path = String::new();
    for (i, &v) in curve.iter().enumerate() {
        let x = x_for(i, curve.len());
        let y = y_for(v, 1.0);
        if i == 0 {
            path.push_str(&format!("M{x:.1},{y:.1}"));
        } else {
            path.push_str(&format!(" L{x:.1},{y:.1}"));
        }
    }
    html.push_str(&format!(
        "<path d=\"{path}\" fill=\"none\" stroke=\"#4a90d9\" stroke-width=\"2\"/>\n"
    ));

    // Dots.
    let dot_step = (curve.len() / 30).max(1);
    for (i, &v) in curve.iter().enumerate() {
        if i % dot_step == 0 {
            let x = x_for(i, curve.len());
            let y = y_for(v, 1.0);
            html.push_str(&format!(
                "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"2.5\" fill=\"#4a90d9\"/>\n"
            ));
        }
    }

    svg_close(html);
}

// ── Multi-line convergence overlay ──────────────────────────────────────────

fn render_convergence_overlay(html: &mut String, curves: &[(String, Vec<f64>)]) {
    html.push_str("<h2>Convergence Comparison (All Topologies)</h2>\n");
    html.push_str("<p class=\"explanation\">Overlay of convergence curves across different topologies at scale.</p>\n");

    let max_len = curves.iter().map(|(_, c)| c.len()).max().unwrap_or(0);
    if max_len == 0 {
        return;
    }

    let total_w = CHART_W + MR;
    let total_h = CHART_H + MT + MB + 40.0; // extra for legend
    svg_open(html, total_w, total_h);
    draw_axes(html);

    let colors = ["#4a90d9", "#d94a4a", "#4ad94a", "#d9a64a", "#9a4ad9", "#4ad9d9"];

    for pct in [0, 25, 50, 75, 100] {
        let y = y_for(pct as f64 / 100.0, 1.0);
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"10\" fill=\"#666\">{pct}%</text>\n",
            ML - 6.0, y + 3.0,
        ));
    }

    for (ci, (name, curve)) in curves.iter().enumerate() {
        let color = colors[ci % colors.len()];
        let mut path = String::new();
        for (i, &v) in curve.iter().enumerate() {
            let x = x_for(i, max_len);
            let y = y_for(v, 1.0);
            if i == 0 {
                path.push_str(&format!("M{x:.1},{y:.1}"));
            } else {
                path.push_str(&format!(" L{x:.1},{y:.1}"));
            }
        }
        html.push_str(&format!(
            "<path d=\"{path}\" fill=\"none\" stroke=\"{color}\" stroke-width=\"2\"/>\n"
        ));

        // Legend entry.
        let lx = ML + ci as f64 * 140.0;
        let ly = MT + CHART_H + 36.0;
        html.push_str(&format!(
            "<rect x=\"{lx:.0}\" y=\"{ly:.0}\" width=\"14\" height=\"10\" fill=\"{color}\"/>\n"
        ));
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#333\">{name}</text>\n",
            lx + 18.0, ly + 9.0,
        ));
    }

    svg_close(html);
}

// ── Entropy chart ───────────────────────────────────────────────────────────

fn render_entropy_chart(html: &mut String, entropy: &[usize]) {
    if entropy.is_empty() {
        return;
    }
    let max_e = *entropy.iter().max().unwrap_or(&1) as f64;
    let total_w = CHART_W + MR;
    let total_h = CHART_H + MT + MB;
    svg_open(html, total_w, total_h);
    draw_axes(html);

    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"10\" fill=\"#666\">{}</text>\n",
        ML - 6.0, MT + 3.0, max_e as usize,
    ));
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"10\" fill=\"#666\">0</text>\n",
        ML - 6.0, MT + CHART_H + 3.0,
    ));

    let mut path = String::new();
    for (i, &e) in entropy.iter().enumerate() {
        let x = x_for(i, entropy.len());
        let y = y_for(e as f64, max_e);
        if i == 0 {
            path.push_str(&format!("M{x:.1},{y:.1}"));
        } else {
            path.push_str(&format!(" L{x:.1},{y:.1}"));
        }
    }
    html.push_str(&format!(
        "<path d=\"{path}\" fill=\"none\" stroke=\"#d94a4a\" stroke-width=\"2\"/>\n"
    ));

    svg_close(html);
}

// ── State size chart ────────────────────────────────────────────────────────

fn render_state_size_chart(html: &mut String, sizes: &[f64]) {
    if sizes.is_empty() {
        return;
    }
    let max_s = sizes.iter().cloned().fold(0.0f64, f64::max).max(1.0);
    let total_w = CHART_W + MR;
    let total_h = CHART_H + MT + MB;
    svg_open(html, total_w, total_h);
    draw_axes(html);

    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"10\" fill=\"#666\">{:.1}</text>\n",
        ML - 6.0, MT + 3.0, max_s,
    ));

    let mut path = String::new();
    for (i, &s) in sizes.iter().enumerate() {
        let x = x_for(i, sizes.len());
        let y = y_for(s, max_s);
        if i == 0 {
            path.push_str(&format!("M{x:.1},{y:.1}"));
        } else {
            path.push_str(&format!(" L{x:.1},{y:.1}"));
        }
    }
    html.push_str(&format!(
        "<path d=\"{path}\" fill=\"none\" stroke=\"#4a90d9\" stroke-width=\"2\"/>\n"
    ));

    svg_close(html);
}

// ── Load bar chart ──────────────────────────────────────────────────────────

fn render_load_bar_chart(html: &mut String, metrics: &GossipMetrics) {
    let mut nodes: Vec<(&String, usize)> = metrics
        .pushes_received_per_node
        .iter()
        .map(|(n, &c)| (n, c))
        .collect();
    nodes.sort_by(|a, b| b.1.cmp(&a.1));
    // Show top 20 nodes.
    nodes.truncate(20);

    if nodes.is_empty() {
        return;
    }
    let max_v = nodes[0].1 as f64;
    let bar_h = 18.0;
    let gap = 4.0;
    let total_h = MT + (bar_h + gap) * nodes.len() as f64 + MB;
    let total_w = CHART_W + MR;
    svg_open(html, total_w, total_h);

    for (i, (name, count)) in nodes.iter().enumerate() {
        let y = MT + i as f64 * (bar_h + gap);
        let w = if max_v > 0.0 {
            (*count as f64 / max_v) * (CHART_W - ML - MR - 40.0)
        } else {
            0.0
        };
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" text-anchor=\"end\" font-size=\"10\" fill=\"#333\">{name}</text>\n",
            ML - 4.0, y + bar_h - 4.0,
        ));
        html.push_str(&format!(
            "<rect x=\"{ML}\" y=\"{y:.1}\" width=\"{w:.1}\" height=\"{bar_h}\" fill=\"#4a90d9\" rx=\"3\"/>\n"
        ));
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#333\">{count}</text>\n",
            ML + w + 4.0, y + bar_h - 4.0,
        ));
    }

    svg_close(html);
}

// ── Message stacked bar ─────────────────────────────────────────────────────

fn render_message_stacked_bar(html: &mut String, metrics: &GossipMetrics) {
    let useful = metrics.total_pushes - metrics.redundant_pushes;
    let redundant = metrics.redundant_pushes;
    let total = metrics.total_pushes.max(1) as f64;

    let total_w = 400.0;
    let total_h = 80.0;
    svg_open(html, total_w, total_h);

    let bar_w = 300.0;
    let bar_h = 30.0;
    let y = 20.0;
    let x = 60.0;

    let useful_w = (useful as f64 / total) * bar_w;
    let redundant_w = (redundant as f64 / total) * bar_w;

    html.push_str(&format!(
        "<rect x=\"{x}\" y=\"{y}\" width=\"{useful_w:.1}\" height=\"{bar_h}\" fill=\"#28a745\" rx=\"3\"/>\n"
    ));
    html.push_str(&format!(
        "<rect x=\"{}\" y=\"{y}\" width=\"{redundant_w:.1}\" height=\"{bar_h}\" fill=\"#dc3545\" rx=\"3\"/>\n",
        x + useful_w,
    ));

    // Legend.
    let ly = y + bar_h + 16.0;
    html.push_str(&format!(
        "<rect x=\"{x}\" y=\"{ly}\" width=\"12\" height=\"10\" fill=\"#28a745\"/>\n"
    ));
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#333\">Useful ({useful})</text>\n",
        x + 16.0, ly + 9.0,
    ));
    html.push_str(&format!(
        "<rect x=\"{}\" y=\"{ly}\" width=\"12\" height=\"10\" fill=\"#dc3545\"/>\n",
        x + 140.0,
    ));
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#333\">Redundant ({redundant})</text>\n",
        x + 156.0, ly + 9.0,
    ));

    svg_close(html);
}

// ── Peer selection histogram ────────────────────────────────────────────────

fn render_peer_histogram(html: &mut String, metrics: &GossipMetrics) {
    // Aggregate: for each target, total selection count across all nodes.
    let mut target_totals: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for targets in metrics.peer_selection_distribution.values() {
        for (target, &count) in targets {
            *target_totals.entry(target.clone()).or_default() += count;
        }
    }
    let mut sorted: Vec<(String, usize)> = target_totals.into_iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));

    if sorted.is_empty() {
        return;
    }

    let max_v = sorted.iter().map(|(_, c)| *c).max().unwrap_or(1) as f64;
    let bar_w = 30.0;
    let gap = 4.0;
    let total_w = ML + (bar_w + gap) * sorted.len() as f64 + MR;
    let total_h = CHART_H + MT + MB;
    svg_open(html, total_w, total_h);

    // Axes.
    let base_y = MT + CHART_H;
    html.push_str(&format!(
        "<line x1=\"{ML}\" y1=\"{MT}\" x2=\"{ML}\" y2=\"{base_y}\" stroke=\"#333\" stroke-width=\"1\"/>\n"
    ));
    html.push_str(&format!(
        "<line x1=\"{ML}\" y1=\"{base_y}\" x2=\"{}\" y2=\"{base_y}\" stroke=\"#333\" stroke-width=\"1\"/>\n",
        total_w - MR,
    ));

    for (i, (name, count)) in sorted.iter().enumerate() {
        let x = ML + i as f64 * (bar_w + gap);
        let h = (*count as f64 / max_v) * CHART_H;
        let y = base_y - h;
        html.push_str(&format!(
            "<rect x=\"{x:.1}\" y=\"{y:.1}\" width=\"{bar_w}\" height=\"{h:.1}\" fill=\"#4a90d9\" rx=\"2\"/>\n"
        ));
        // Label.
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\" font-size=\"9\" fill=\"#666\" transform=\"rotate(-45 {} {})\">{name}</text>\n",
            x + bar_w / 2.0, base_y + 14.0, x + bar_w / 2.0, base_y + 14.0,
        ));
        // Count on top.
        html.push_str(&format!(
            "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\" font-size=\"9\" fill=\"#333\">{count}</text>\n",
            x + bar_w / 2.0, y - 3.0,
        ));
    }

    svg_close(html);
}

// ── Scalability section ─────────────────────────────────────────────────────

fn render_scalability_section(html: &mut String, data: &PropertyReportData) {
    html.push_str("<h2>Scalability</h2>\n");
    html.push_str("<p class=\"explanation\">How convergence time and message count scale with network size.</p>\n");

    // Convergence round vs N (with O(log N) reference).
    html.push_str("<h3>Convergence Time vs Network Size</h3>\n");
    render_scaling_scatter(
        html,
        &data.scaling_points_st,
        &data.scaling_points_mt,
        true,
    );

    // Total messages vs N (with O(N) reference).
    html.push_str("<h3>Total Messages vs Network Size</h3>\n");
    render_scaling_scatter(
        html,
        &data.scaling_points_st,
        &data.scaling_points_mt,
        false,
    );
}

fn render_scaling_scatter(
    html: &mut String,
    st_points: &[ScalingPoint],
    mt_points: &[ScalingPoint],
    is_convergence: bool,
) {
    let all_n: Vec<usize> = st_points
        .iter()
        .chain(mt_points.iter())
        .map(|p| p.n)
        .collect();
    let all_y: Vec<f64> = st_points
        .iter()
        .chain(mt_points.iter())
        .map(|p| {
            if is_convergence {
                p.convergence_round.unwrap_or(0) as f64
            } else {
                p.total_pushes as f64
            }
        })
        .collect();

    if all_n.is_empty() {
        return;
    }

    let max_n = *all_n.iter().max().unwrap() as f64;
    let max_y = all_y.iter().cloned().fold(0.0f64, f64::max).max(1.0);

    let total_w = CHART_W + MR;
    let total_h = CHART_H + MT + MB + 30.0;
    svg_open(html, total_w, total_h);
    draw_axes(html);

    // Reference line.
    let ref_color = "#ccc";
    let ref_points = 50;
    let mut ref_path = String::new();
    for i in 0..=ref_points {
        let n = (i as f64 / ref_points as f64) * max_n;
        let ref_y_val = if is_convergence {
            // O(log N) reference scaled to fit.
            if n > 1.0 {
                (n.ln() / max_n.ln()) * max_y
            } else {
                0.0
            }
        } else {
            // O(N) reference.
            (n / max_n) * max_y
        };
        let x = ML + (n / max_n) * (CHART_W - ML - MR);
        let y = y_for(ref_y_val, max_y);
        if i == 0 {
            ref_path.push_str(&format!("M{x:.1},{y:.1}"));
        } else {
            ref_path.push_str(&format!(" L{x:.1},{y:.1}"));
        }
    }
    html.push_str(&format!(
        "<path d=\"{ref_path}\" fill=\"none\" stroke=\"{ref_color}\" stroke-width=\"1.5\" stroke-dasharray=\"6,4\"/>\n"
    ));
    let ref_label = if is_convergence { "O(log N)" } else { "O(N)" };
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#aaa\">{ref_label}</text>\n",
        ML + CHART_W - ML - MR - 50.0, MT + 14.0,
    ));

    // Single-threaded points.
    for p in st_points {
        let x = ML + (p.n as f64 / max_n) * (CHART_W - ML - MR);
        let yv = if is_convergence {
            p.convergence_round.unwrap_or(0) as f64
        } else {
            p.total_pushes as f64
        };
        let y = y_for(yv, max_y);
        html.push_str(&format!(
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"5\" fill=\"#4a90d9\" stroke=\"#2a5a9d\" stroke-width=\"1\"/>\n"
        ));
    }

    // Multi-threaded points.
    for p in mt_points {
        let x = ML + (p.n as f64 / max_n) * (CHART_W - ML - MR);
        let yv = if is_convergence {
            p.convergence_round.unwrap_or(0) as f64
        } else {
            p.total_pushes as f64
        };
        let y = y_for(yv, max_y);
        html.push_str(&format!(
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"5\" fill=\"#d94a4a\" stroke=\"#9d2a2a\" stroke-width=\"1\"/>\n"
        ));
    }

    // Legend.
    let ly = MT + CHART_H + 30.0;
    html.push_str(&format!(
        "<circle cx=\"{ML}\" cy=\"{ly}\" r=\"5\" fill=\"#4a90d9\"/>\n"
    ));
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#333\">Single-threaded</text>\n",
        ML + 10.0, ly + 4.0,
    ));
    html.push_str(&format!(
        "<circle cx=\"{}\" cy=\"{ly}\" r=\"5\" fill=\"#d94a4a\"/>\n",
        ML + 130.0,
    ));
    html.push_str(&format!(
        "<text x=\"{}\" y=\"{}\" font-size=\"10\" fill=\"#333\">Multi-threaded</text>\n",
        ML + 140.0, ly + 4.0,
    ));

    svg_close(html);
}

// ── Thread comparison ───────────────────────────────────────────────────────

fn render_thread_comparison(html: &mut String, comparisons: &[ThreadComparison]) {
    html.push_str("<h2>Thread-Mode Comparison</h2>\n");
    html.push_str("<p class=\"explanation\">Comparing convergence time and message counts across different thread configurations.</p>\n");

    html.push_str("<table>\n<tr><th>Configuration</th><th>Threads</th><th>Convergence Round</th><th>Total Pushes</th></tr>\n");
    for tc in comparisons {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            esc(&tc.label),
            tc.num_threads,
            tc.convergence_round
                .map(|r| r.to_string())
                .unwrap_or("never".into()),
            tc.total_pushes,
        ));
    }
    html.push_str("</table>\n");
}

// ── HTML escape ─────────────────────────────────────────────────────────────

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
