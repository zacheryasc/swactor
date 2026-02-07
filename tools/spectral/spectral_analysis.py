#!/usr/bin/env python3
"""Spectral analysis tool for dependency DAGs.

Reads a GraphViz DOT file (produced by the depgraph tool) and applies spectral
graph theory (Laplacian eigenvalues, Fiedler vectors) to derive quantitative
complexity metrics and visual analysis of codebase structural coupling.

Usage:
    python spectral_analysis.py deps.dot [-o OUTPUT_DIR] [--no-plots] [--json]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any

import numpy as np
from scipy import sparse


# ─── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class Node:
    name: str
    module: str


@dataclass
class Edge:
    source: str
    target: str
    label: str
    edge_type: str  # "field" or "trait_impl"
    cross_module: bool


@dataclass
class DependencyGraph:
    nodes: list[Node] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)
    modules: list[str] = field(default_factory=list)  # ordered module names
    node_to_module: dict[str, str] = field(default_factory=dict)


@dataclass
class SpectralResults:
    eigenvalues: np.ndarray
    eigenvectors: np.ndarray
    fiedler_value: float
    fiedler_vector: np.ndarray
    adjacency: np.ndarray
    adjacency_sym: np.ndarray
    laplacian: np.ndarray
    node_names: list[str]
    node_modules: list[str]


@dataclass
class ModuleCouplingResult:
    module_names: list[str]
    coupling_matrix: np.ndarray  # directed
    cross_module_edges: int
    total_edges: int


@dataclass
class ComplexityMetrics:
    algebraic_connectivity: float
    normalized_algebraic_connectivity: float
    spectral_entropy: float
    normalized_spectral_entropy: float
    edge_density: float
    cross_module_ratio: float
    spectral_radius: float
    normalized_spectral_radius: float
    cci: float
    n_nodes: int
    n_edges: int
    n_modules: int
    connected_components: int


# ─── DOT Parser ───────────────────────────────────────────────────────────────

def parse_dot(text: str) -> DependencyGraph:
    """Parse a depgraph-generated DOT file into a DependencyGraph.

    Uses a line-by-line state machine to extract:
    - subgraph cluster_<module> blocks -> nodes with module membership
    - A -> B [label="...", style=..., ...] -> edges with classification
    """
    graph = DependencyGraph()
    current_module: str | None = None
    module_order: list[str] = []
    seen_nodes: set[str] = set()

    for line in text.splitlines():
        stripped = line.strip()

        # Entering a subgraph cluster
        m = re.match(r'subgraph\s+cluster_(\w+)\s*\{', stripped)
        if m:
            current_module = m.group(1)
            if current_module not in module_order:
                module_order.append(current_module)
            continue

        # Closing brace - exit current subgraph if we're in one
        if stripped == '}' and current_module is not None:
            current_module = None
            continue

        # Node definition inside a subgraph: NodeName [label="...", ...]
        if current_module is not None:
            node_match = re.match(r'(\w+)\s*\[', stripped)
            if node_match:
                node_name = node_match.group(1)
                # Skip DOT keywords
                if node_name in ('label', 'style', 'node', 'edge', 'graph',
                                 'subgraph', 'digraph', 'rankdir', 'fontname',
                                 'fontsize', 'labelloc', 'compound', 'newrank',
                                 'splines', 'fillcolor', 'color'):
                    continue
                if node_name not in seen_nodes:
                    seen_nodes.add(node_name)
                    graph.nodes.append(Node(name=node_name, module=current_module))
                    graph.node_to_module[node_name] = current_module
                continue

        # Edge definition: A -> B [label="...", style=..., ...]
        edge_match = re.match(
            r'(\w+)\s*->\s*(\w+)\s*\[(.+)\];', stripped
        )
        if edge_match:
            src = edge_match.group(1)
            tgt = edge_match.group(2)
            attrs_str = edge_match.group(3)

            # Extract label
            label_match = re.search(r'label="([^"]*)"', attrs_str)
            label = label_match.group(1) if label_match else ""

            # Classify edge type
            style_match = re.search(r'style=(\w+)', attrs_str)
            style = style_match.group(1) if style_match else "solid"
            edge_type = "trait_impl" if style == "dotted" else "field"

            # Determine cross-module status
            src_mod = graph.node_to_module.get(src)
            tgt_mod = graph.node_to_module.get(tgt)
            cross = src_mod is not None and tgt_mod is not None and src_mod != tgt_mod

            graph.edges.append(Edge(
                source=src, target=tgt, label=label,
                edge_type=edge_type, cross_module=cross,
            ))
            continue

    graph.modules = module_order
    return graph


# ─── Matrix Construction ──────────────────────────────────────────────────────

def get_node_ordering(graph: DependencyGraph) -> list[str]:
    """Order nodes by module order, then alphabetical within module."""
    module_index = {m: i for i, m in enumerate(graph.modules)}
    return sorted(
        [n.name for n in graph.nodes],
        key=lambda name: (
            module_index.get(graph.node_to_module.get(name, ""), 999),
            name,
        ),
    )


def build_adjacency(graph: DependencyGraph, node_order: list[str]) -> np.ndarray:
    """Build directed binary adjacency matrix."""
    n = len(node_order)
    idx = {name: i for i, name in enumerate(node_order)}
    A = np.zeros((n, n), dtype=float)
    for edge in graph.edges:
        i = idx.get(edge.source)
        j = idx.get(edge.target)
        if i is not None and j is not None:
            A[i, j] = 1.0
    return A


def symmetrize(A: np.ndarray) -> np.ndarray:
    """OR-symmetrize: A_sym[i,j] = 1 if A[i,j] or A[j,i]."""
    return np.clip(A + A.T, 0, 1)


def build_laplacian(A_sym: np.ndarray) -> np.ndarray:
    """Build graph Laplacian L = D - A_sym."""
    D = np.diag(A_sym.sum(axis=1))
    return D - A_sym


# ─── Spectral Analysis ────────────────────────────────────────────────────────

def compute_spectral(graph: DependencyGraph) -> SpectralResults:
    """Compute full spectral analysis of the dependency graph."""
    node_order = get_node_ordering(graph)
    n = len(node_order)

    A = build_adjacency(graph, node_order)
    A_sym = symmetrize(A)
    L = build_laplacian(A_sym)

    if n == 0:
        return SpectralResults(
            eigenvalues=np.array([]),
            eigenvectors=np.array([[]]),
            fiedler_value=0.0,
            fiedler_vector=np.array([]),
            adjacency=A, adjacency_sym=A_sym, laplacian=L,
            node_names=node_order,
            node_modules=[graph.node_to_module.get(name, "") for name in node_order],
        )

    eigenvalues, eigenvectors = np.linalg.eigh(L)

    # Clean up near-zero eigenvalues
    eigenvalues = np.where(np.abs(eigenvalues) < 1e-10, 0.0, eigenvalues)

    if n == 1:
        fiedler_value = 0.0
        fiedler_vector = np.array([0.0])
    elif n >= 2:
        fiedler_value = float(eigenvalues[1])
        fiedler_vector = eigenvectors[:, 1]
    else:
        fiedler_value = 0.0
        fiedler_vector = np.array([])

    return SpectralResults(
        eigenvalues=eigenvalues,
        eigenvectors=eigenvectors,
        fiedler_value=fiedler_value,
        fiedler_vector=fiedler_vector,
        adjacency=A,
        adjacency_sym=A_sym,
        laplacian=L,
        node_names=node_order,
        node_modules=[graph.node_to_module.get(name, "") for name in node_order],
    )


# ─── Module Coupling ──────────────────────────────────────────────────────────

def compute_module_coupling(graph: DependencyGraph) -> ModuleCouplingResult:
    """Compute directed module-level coupling matrix."""
    modules = graph.modules
    n = len(modules)
    mod_idx = {m: i for i, m in enumerate(modules)}
    M = np.zeros((n, n), dtype=float)

    cross = 0
    total = len(graph.edges)

    for edge in graph.edges:
        src_mod = graph.node_to_module.get(edge.source)
        tgt_mod = graph.node_to_module.get(edge.target)
        if src_mod is not None and tgt_mod is not None:
            i = mod_idx.get(src_mod)
            j = mod_idx.get(tgt_mod)
            if i is not None and j is not None:
                M[i, j] += 1.0
                if src_mod != tgt_mod:
                    cross += 1

    return ModuleCouplingResult(
        module_names=modules,
        coupling_matrix=M,
        cross_module_edges=cross,
        total_edges=total,
    )


# ─── Complexity Metrics ───────────────────────────────────────────────────────

def count_connected_components(A_sym: np.ndarray) -> int:
    """Count connected components using BFS on the symmetrized adjacency."""
    n = A_sym.shape[0]
    if n == 0:
        return 0
    visited = set()
    components = 0
    for start in range(n):
        if start in visited:
            continue
        components += 1
        queue = [start]
        visited.add(start)
        while queue:
            node = queue.pop(0)
            for neighbor in range(n):
                if A_sym[node, neighbor] > 0 and neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
    return components


def compute_spectral_entropy(eigenvalues: np.ndarray) -> float:
    """Compute spectral entropy from positive Laplacian eigenvalues.

    H(lambda) = -sum(p_i * log2(p_i)) where p_i = lambda_i / sum(lambdas)
    over positive eigenvalues.
    """
    positive = eigenvalues[eigenvalues > 1e-10]
    if len(positive) == 0:
        return 0.0
    p = positive / positive.sum()
    # Avoid log(0)
    p = p[p > 0]
    return float(-np.sum(p * np.log2(p)))


def compute_complexity_metrics(
    spectral: SpectralResults,
    coupling: ModuleCouplingResult,
) -> ComplexityMetrics:
    """Compute the Connectome Complexity Index (CCI) and all sub-metrics."""
    n = len(spectral.node_names)
    n_edges = int(spectral.adjacency.sum())  # directed edge count
    n_modules = len(coupling.module_names)
    components = count_connected_components(spectral.adjacency_sym)

    if n <= 1:
        return ComplexityMetrics(
            algebraic_connectivity=0.0,
            normalized_algebraic_connectivity=0.0,
            spectral_entropy=0.0,
            normalized_spectral_entropy=0.0,
            edge_density=0.0,
            cross_module_ratio=0.0,
            spectral_radius=0.0,
            normalized_spectral_radius=0.0,
            cci=0.0,
            n_nodes=n,
            n_edges=n_edges,
            n_modules=n_modules,
            connected_components=components,
        )

    # Sub-metric 1: Normalized algebraic connectivity (lambda_2 / n)
    algebraic_connectivity = spectral.fiedler_value
    norm_alg_conn = algebraic_connectivity / n

    # Sub-metric 2: Spectral entropy
    raw_entropy = compute_spectral_entropy(spectral.eigenvalues)
    positive_count = int(np.sum(spectral.eigenvalues > 1e-10))
    max_entropy = math.log2(positive_count) if positive_count > 1 else 1.0
    norm_entropy = raw_entropy / max_entropy if max_entropy > 0 else 0.0

    # Sub-metric 3: Edge density |E| / (n*(n-1))
    edge_density = n_edges / (n * (n - 1)) if n > 1 else 0.0

    # Sub-metric 4: Cross-module coupling ratio
    cross_ratio = (coupling.cross_module_edges / coupling.total_edges
                   if coupling.total_edges > 0 else 0.0)

    # Sub-metric 5: Normalized spectral radius (max eigenvalue of A_sym / (n-1))
    if spectral.adjacency_sym.shape[0] > 0:
        eig_A = np.linalg.eigvalsh(spectral.adjacency_sym)
        spectral_radius = float(np.max(np.abs(eig_A)))
    else:
        spectral_radius = 0.0
    norm_spec_radius = spectral_radius / (n - 1) if n > 1 else 0.0

    # CCI = weighted sum
    cci = (
        0.25 * norm_alg_conn
        + 0.25 * norm_entropy
        + 0.15 * edge_density
        + 0.20 * cross_ratio
        + 0.15 * norm_spec_radius
    )

    return ComplexityMetrics(
        algebraic_connectivity=algebraic_connectivity,
        normalized_algebraic_connectivity=norm_alg_conn,
        spectral_entropy=raw_entropy,
        normalized_spectral_entropy=norm_entropy,
        edge_density=edge_density,
        cross_module_ratio=cross_ratio,
        spectral_radius=spectral_radius,
        normalized_spectral_radius=norm_spec_radius,
        cci=cci,
        n_nodes=n,
        n_edges=n_edges,
        n_modules=n_modules,
        connected_components=components,
    )


# ─── Full Pipeline ────────────────────────────────────────────────────────────

@dataclass
class AnalysisResult:
    graph: DependencyGraph
    spectral: SpectralResults
    coupling: ModuleCouplingResult
    metrics: ComplexityMetrics


def run_analysis(graph: DependencyGraph) -> AnalysisResult:
    """Run the full spectral analysis pipeline on a DependencyGraph."""
    spectral = compute_spectral(graph)
    coupling = compute_module_coupling(graph)
    metrics = compute_complexity_metrics(spectral, coupling)
    return AnalysisResult(
        graph=graph,
        spectral=spectral,
        coupling=coupling,
        metrics=metrics,
    )


# ─── Text Report ──────────────────────────────────────────────────────────────

def generate_report(result: AnalysisResult) -> str:
    """Generate a text report of the spectral analysis."""
    s = result.spectral
    m = result.metrics
    c = result.coupling
    lines: list[str] = []

    def w(text: str = "") -> None:
        lines.append(text)

    w("=" * 72)
    w("  SPECTRAL ANALYSIS REPORT — Dependency DAG")
    w("=" * 72)
    w()

    # Graph summary
    w("GRAPH SUMMARY")
    w("-" * 40)
    w(f"  Nodes:                {m.n_nodes}")
    w(f"  Directed edges:       {m.n_edges}")
    w(f"  Modules:              {m.n_modules}")
    w(f"  Connected components: {m.connected_components}")
    w(f"  Modules:              {', '.join(c.module_names)}")
    w()

    # Eigenvalue spectrum
    w("LAPLACIAN EIGENVALUE SPECTRUM")
    w("-" * 40)
    for i, ev in enumerate(s.eigenvalues):
        marker = " <-- Fiedler value (lambda_2)" if i == 1 else ""
        w(f"  lambda_{i:2d} = {ev:8.4f}{marker}")
    w()
    if len(s.eigenvalues) > 1:
        spectral_gap = float(s.eigenvalues[-1] - s.eigenvalues[1])
        w(f"  Spectral gap (lambda_max - lambda_2): {spectral_gap:.4f}")
    w(f"  Fiedler value (algebraic connectivity): {s.fiedler_value:.4f}")
    w()

    # Fiedler vector analysis
    if len(s.fiedler_vector) > 0:
        w("FIEDLER VECTOR — SPECTRAL BISECTION")
        w("-" * 40)
        # Sort by fiedler value
        indices = np.argsort(s.fiedler_vector)
        w("  Partition A (Fiedler < 0):")
        for idx in indices:
            if s.fiedler_vector[idx] < 0:
                w(f"    {s.node_names[idx]:25s} [{s.node_modules[idx]:12s}]  "
                  f"f = {s.fiedler_vector[idx]:+.4f}")
        w("  ────────────────────────────────────")
        w("  Partition B (Fiedler >= 0):")
        for idx in indices:
            if s.fiedler_vector[idx] >= 0:
                w(f"    {s.node_names[idx]:25s} [{s.node_modules[idx]:12s}]  "
                  f"f = {s.fiedler_vector[idx]:+.4f}")
        w()

    # Module coupling
    w("MODULE COUPLING MATRIX (directed edge counts)")
    w("-" * 40)
    header = "  " + " " * 14 + "".join(f"{name:>10s}" for name in c.module_names)
    w(header)
    for i, row_name in enumerate(c.module_names):
        row = f"  {row_name:12s}  " + "".join(
            f"{int(c.coupling_matrix[i, j]):10d}" for j in range(len(c.module_names))
        )
        w(row)
    w()
    w(f"  Cross-module edges: {c.cross_module_edges} / {c.total_edges} "
      f"({m.cross_module_ratio:.1%})")
    w()

    # Complexity metrics
    w("CONNECTOME COMPLEXITY INDEX (CCI)")
    w("-" * 40)
    w(f"  {'Sub-metric':<40s} {'Raw':>10s} {'Normalized':>10s} {'Weight':>8s} {'Contrib':>8s}")
    w(f"  {'─' * 40} {'─' * 10} {'─' * 10} {'─' * 8} {'─' * 8}")

    rows = [
        ("Algebraic connectivity (lambda_2/n)",
         f"{m.algebraic_connectivity:.4f}", f"{m.normalized_algebraic_connectivity:.4f}",
         "0.25", f"{0.25 * m.normalized_algebraic_connectivity:.4f}"),
        ("Spectral entropy (H/log2(k))",
         f"{m.spectral_entropy:.4f}", f"{m.normalized_spectral_entropy:.4f}",
         "0.25", f"{0.25 * m.normalized_spectral_entropy:.4f}"),
        ("Edge density (|E|/n(n-1))",
         f"{m.edge_density:.4f}", f"{m.edge_density:.4f}",
         "0.15", f"{0.15 * m.edge_density:.4f}"),
        ("Cross-module coupling ratio",
         f"{m.cross_module_ratio:.4f}", f"{m.cross_module_ratio:.4f}",
         "0.20", f"{0.20 * m.cross_module_ratio:.4f}"),
        ("Spectral radius (rho/(n-1))",
         f"{m.spectral_radius:.4f}", f"{m.normalized_spectral_radius:.4f}",
         "0.15", f"{0.15 * m.normalized_spectral_radius:.4f}"),
    ]
    for label, raw, norm, weight, contrib in rows:
        w(f"  {label:<40s} {raw:>10s} {norm:>10s} {weight:>8s} {contrib:>8s}")
    w(f"  {'─' * 40} {'─' * 10} {'─' * 10} {'─' * 8} {'─' * 8}")
    w(f"  {'CCI (weighted sum)':<40s} {'':>10s} {'':>10s} {'1.00':>8s} {m.cci:8.4f}")
    w()

    # Interpretation
    if m.cci < 0.3:
        interp = "LOW complexity — well-decomposed architecture"
    elif m.cci < 0.6:
        interp = "MODERATE complexity — typical well-structured codebase"
    else:
        interp = "HIGH complexity — consider reviewing module boundaries"
    w(f"  Interpretation: {interp}")
    w()
    w("=" * 72)

    return "\n".join(lines)


# ─── Dashboard Visualization ─────────────────────────────────────────────────

# Module colors matching the depgraph tool
MODULE_COLORS = {
    "error": "#4caf50",
    "config": "#8bc34a",
    "channel": "#ffeb3b",
    "actor": "#2196f3",
    "address_map": "#9c27b0",
    "runtime": "#f44336",
    "worker": "#ff9800",
    "python": "#795548",
}

DEFAULT_COLOR = "#9e9e9e"


def get_module_color(module: str) -> str:
    return MODULE_COLORS.get(module, DEFAULT_COLOR)


def generate_dashboard(result: AnalysisResult, output_path: str) -> None:
    """Generate spectral dashboard PNG (16x12, 150 DPI, dark theme)."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec

    s = result.spectral
    m = result.metrics
    c = result.coupling

    # Dark theme
    plt.rcParams.update({
        "figure.facecolor": "#1a1a2e",
        "axes.facecolor": "#16213e",
        "axes.edgecolor": "#e0e0e0",
        "axes.labelcolor": "#e0e0e0",
        "text.color": "#e0e0e0",
        "xtick.color": "#e0e0e0",
        "ytick.color": "#e0e0e0",
        "grid.color": "#2a2a4a",
        "grid.alpha": 0.5,
    })

    fig = plt.figure(figsize=(16, 12), dpi=150)
    gs = GridSpec(2, 2, figure=fig, hspace=0.35, wspace=0.3,
                  left=0.07, right=0.95, top=0.92, bottom=0.06)

    fig.suptitle("Spectral Analysis Dashboard — Dependency DAG",
                 fontsize=16, fontweight="bold", color="#e0e0e0")

    # ── Top-left: Eigenvalue spectrum ──
    ax1 = fig.add_subplot(gs[0, 0])
    n = len(s.eigenvalues)
    colors_eig = ["#ff4444" if i == 1 else "#4fc3f7" for i in range(n)]
    markerline, stemlines, baseline = ax1.stem(
        range(n), s.eigenvalues, linefmt="-", markerfmt="o", basefmt=" "
    )
    markerline.set_color("#4fc3f7")
    markerline.set_markersize(5)
    stemlines.set_color("#4fc3f7")
    stemlines.set_alpha(0.6)
    # Highlight lambda_2
    if n > 1:
        ax1.plot(1, s.eigenvalues[1], "o", color="#ff4444", markersize=10,
                 zorder=5, label=f"$\\lambda_2$ = {s.fiedler_value:.4f}")
        ax1.legend(fontsize=10, loc="upper left",
                   facecolor="#16213e", edgecolor="#444")
    ax1.set_xlabel("Index")
    ax1.set_ylabel("Eigenvalue")
    ax1.set_title("Laplacian Eigenvalue Spectrum", fontsize=12, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    # ── Top-right: Fiedler vector ──
    ax2 = fig.add_subplot(gs[0, 1])
    if len(s.fiedler_vector) > 0:
        sorted_indices = np.argsort(s.fiedler_vector)
        sorted_values = s.fiedler_vector[sorted_indices]
        sorted_names = [s.node_names[i] for i in sorted_indices]
        sorted_modules = [s.node_modules[i] for i in sorted_indices]
        bar_colors = [get_module_color(mod) for mod in sorted_modules]

        bars = ax2.barh(range(len(sorted_values)), sorted_values,
                        color=bar_colors, edgecolor="none", height=0.8)
        ax2.axvline(x=0, color="#ff4444", linewidth=1.5, linestyle="--",
                    alpha=0.8, label="Bisection boundary")
        ax2.set_yticks(range(len(sorted_names)))
        ax2.set_yticklabels(sorted_names, fontsize=6)
        ax2.set_xlabel("Fiedler value")
        ax2.set_title("Fiedler Vector (spectral bisection)", fontsize=12,
                       fontweight="bold")

        # Legend for modules
        unique_modules = []
        seen = set()
        for mod in sorted_modules:
            if mod not in seen:
                seen.add(mod)
                unique_modules.append(mod)
        from matplotlib.patches import Patch
        legend_patches = [Patch(facecolor=get_module_color(mod), label=mod)
                          for mod in unique_modules]
        ax2.legend(handles=legend_patches, fontsize=7, loc="lower right",
                   facecolor="#16213e", edgecolor="#444", ncol=2)
    else:
        ax2.text(0.5, 0.5, "No Fiedler vector\n(single node graph)",
                 ha="center", va="center", fontsize=14, transform=ax2.transAxes)
        ax2.set_title("Fiedler Vector", fontsize=12, fontweight="bold")

    # ── Bottom-left: Module coupling heatmap ──
    ax3 = fig.add_subplot(gs[1, 0])
    if len(c.module_names) > 0:
        im = ax3.imshow(c.coupling_matrix, cmap="YlOrRd", aspect="auto")
        ax3.set_xticks(range(len(c.module_names)))
        ax3.set_xticklabels(c.module_names, rotation=45, ha="right", fontsize=8)
        ax3.set_yticks(range(len(c.module_names)))
        ax3.set_yticklabels(c.module_names, fontsize=8)
        ax3.set_title("Module Coupling (directed edge counts)", fontsize=12,
                       fontweight="bold")
        ax3.set_xlabel("Target module")
        ax3.set_ylabel("Source module")

        # Annotate cells
        for i in range(len(c.module_names)):
            for j in range(len(c.module_names)):
                val = int(c.coupling_matrix[i, j])
                if val > 0:
                    text_color = "white" if val > c.coupling_matrix.max() * 0.6 else "black"
                    ax3.text(j, i, str(val), ha="center", va="center",
                             fontsize=8, color=text_color, fontweight="bold")

        plt.colorbar(im, ax=ax3, shrink=0.8)
    else:
        ax3.text(0.5, 0.5, "No modules", ha="center", va="center",
                 fontsize=14, transform=ax3.transAxes)
        ax3.set_title("Module Coupling", fontsize=12, fontweight="bold")

    # ── Bottom-right: Metrics panel ──
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.axis("off")

    # CCI interpretation
    if m.cci < 0.3:
        cci_color = "#4caf50"
        cci_label = "LOW"
    elif m.cci < 0.6:
        cci_color = "#ff9800"
        cci_label = "MODERATE"
    else:
        cci_color = "#f44336"
        cci_label = "HIGH"

    text_lines = [
        ("GRAPH", "", False),
        (f"  Nodes: {m.n_nodes}   Edges: {m.n_edges}   "
         f"Modules: {m.n_modules}   Components: {m.connected_components}", "", False),
        ("", "", False),
        ("SPECTRAL METRICS", "", False),
        (f"  Algebraic connectivity (lambda_2):  {m.algebraic_connectivity:.4f}", "", False),
        (f"  Normalized (lambda_2/n):            {m.normalized_algebraic_connectivity:.4f}", "", False),
        (f"  Spectral entropy:                   {m.spectral_entropy:.4f}", "", False),
        (f"  Normalized entropy:                 {m.normalized_spectral_entropy:.4f}", "", False),
        (f"  Spectral radius:                    {m.spectral_radius:.4f}", "", False),
        (f"  Normalized radius:                  {m.normalized_spectral_radius:.4f}", "", False),
        ("", "", False),
        ("COUPLING METRICS", "", False),
        (f"  Edge density:                       {m.edge_density:.4f}", "", False),
        (f"  Cross-module ratio:                 {m.cross_module_ratio:.1%}", "", False),
        ("", "", False),
        (f"  CCI = {m.cci:.4f}  [{cci_label}]", cci_color, True),
    ]

    y = 0.95
    for text, color, bold in text_lines:
        if not text:
            y -= 0.04
            continue
        fontsize = 11 if bold else 9
        weight = "bold" if bold else "normal"
        c_val = color if color else "#e0e0e0"
        ax4.text(0.05, y, text, transform=ax4.transAxes, fontsize=fontsize,
                 fontweight=weight, color=c_val, fontfamily="monospace",
                 verticalalignment="top")
        y -= 0.055

    ax4.set_title("Complexity Metrics", fontsize=12, fontweight="bold")

    plt.savefig(output_path, dpi=150, facecolor=fig.get_facecolor(),
                edgecolor="none", bbox_inches="tight")
    plt.close(fig)


# ─── Interactive HTML Dashboard ───────────────────────────────────────────────

def generate_dashboard_html(
    result: AnalysisResult, output_path: str, *, dot_source: str = ""
) -> None:
    """Generate an interactive HTML dashboard with GraphViz DAG + spectral panels."""
    s = result.spectral
    m = result.metrics
    c = result.coupling

    # Prepare data as JSON for embedding
    sorted_indices = list(np.argsort(s.fiedler_vector)) if len(s.fiedler_vector) > 0 else []
    fiedler_data = []
    for idx in sorted_indices:
        fiedler_data.append({
            "name": s.node_names[idx],
            "module": s.node_modules[idx],
            "value": float(s.fiedler_vector[idx]),
        })

    eigenvalue_data = [{"index": i, "value": float(v)}
                       for i, v in enumerate(s.eigenvalues)]

    coupling_data = {
        "modules": c.module_names,
        "matrix": c.coupling_matrix.tolist(),
    }

    # Module colors
    all_modules = list(dict.fromkeys(n.module for n in result.graph.nodes))
    module_colors_json = {mod: get_module_color(mod) for mod in all_modules}

    # CCI interpretation
    if m.cci < 0.3:
        cci_color = "#4caf50"
        cci_label = "LOW"
        cci_desc = "well-decomposed architecture"
    elif m.cci < 0.6:
        cci_color = "#ff9800"
        cci_label = "MODERATE"
        cci_desc = "typical well-structured codebase"
    else:
        cci_color = "#f44336"
        cci_label = "HIGH"
        cci_desc = "consider reviewing module boundaries"

    metrics_json = {
        "n_nodes": m.n_nodes,
        "n_edges": m.n_edges,
        "n_modules": m.n_modules,
        "connected_components": m.connected_components,
        "algebraic_connectivity": round(m.algebraic_connectivity, 4),
        "normalized_algebraic_connectivity": round(m.normalized_algebraic_connectivity, 4),
        "spectral_entropy": round(m.spectral_entropy, 4),
        "normalized_spectral_entropy": round(m.normalized_spectral_entropy, 4),
        "edge_density": round(m.edge_density, 4),
        "cross_module_ratio": round(m.cross_module_ratio, 4),
        "spectral_radius": round(m.spectral_radius, 4),
        "normalized_spectral_radius": round(m.normalized_spectral_radius, 4),
        "cci": round(m.cci, 4),
        "cci_label": cci_label,
        "cci_color": cci_color,
        "cci_desc": cci_desc,
        "fiedler_value": round(s.fiedler_value, 4),
    }

    data_blob = json.dumps({
        "eigenvalues": eigenvalue_data,
        "fiedler": fiedler_data,
        "coupling": coupling_data,
        "metrics": metrics_json,
        "module_colors": module_colors_json,
    })

    # Escape DOT source for embedding in a JS template literal
    dot_escaped = (dot_source
                   .replace("\\", "\\\\")
                   .replace("`", "\\`")
                   .replace("${", "\\${"))

    html = _DASHBOARD_HTML_TEMPLATE.replace("__DATA_BLOB__", data_blob)
    html = html.replace("__DOT_BLOB__", dot_escaped)

    with open(output_path, "w") as f:
        f.write(html)


_DASHBOARD_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>swactor — dependency analysis</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#1a1a2e; color:#e0e0e0; font-family:system-ui,-apple-system,sans-serif; overflow:hidden; }

/* ─── Tab bar ───────────────────────────────────────────────────────────── */
.tab-bar { display:flex; align-items:center; height:42px; background:#12122a;
           border-bottom:1px solid #2a2a5a; padding:0 16px; gap:8px; }
.tab-bar .title { font-size:14px; font-weight:700; letter-spacing:0.5px; margin-right:18px;
                  color:#8ab4f8; white-space:nowrap; }
.tab { background:none; border:none; color:#888; font-size:13px; padding:8px 16px;
       cursor:pointer; border-bottom:2px solid transparent; transition:color 0.15s; }
.tab:hover { color:#ccc; }
.tab.active { color:#e0e0e0; border-bottom-color:#4fc3f7; }

/* ─── Tab content ───────────────────────────────────────────────────────── */
.tab-content { display:none; }
.tab-content.active { display:block; }

/* ─── DAG tab ───────────────────────────────────────────────────────────── */
#tab-dag { height:calc(100vh - 42px); overflow:hidden; position:relative; }
#dag-viewport { width:100%; height:100%; cursor:grab; }
#dag-viewport:active { cursor:grabbing; }
#dag-viewport svg { display:block; }
#dag-controls { position:absolute; top:12px; left:12px; z-index:10;
                background:rgba(30,30,60,0.9); border-radius:8px; padding:10px 14px;
                color:#ccc; font-size:13px; backdrop-filter:blur(8px); }
#dag-controls button { background:#333; color:#fff; border:1px solid #555;
                       border-radius:4px; padding:4px 10px; cursor:pointer; margin:0 3px; }
#dag-controls button:hover { background:#555; }
#dag-loading { position:absolute; top:50%; left:50%; transform:translate(-50%,-50%);
               color:#ccc; font-size:18px; }

/* ─── Spectral tab ──────────────────────────────────────────────────────── */
#tab-spectral { overflow-y:auto; max-height:calc(100vh - 42px); }

.grid { display:grid; grid-template-columns:1fr 1fr; grid-template-rows:auto auto;
        gap:16px; padding:16px 20px 20px; max-width:1600px; margin:0 auto; }

.panel { background:#16213e; border-radius:10px; border:1px solid #2a2a5a;
         padding:16px; position:relative; min-height:100px; }
.panel h2 { font-size:14px; font-weight:600; margin-bottom:10px; color:#8ab4f8;
            display:flex; align-items:center; gap:8px; }
.panel h2 .icon { font-size:16px; }
.panel svg { width:100%; display:block; }

.tooltip { position:fixed; background:rgba(22,33,62,0.96); border:1px solid #4fc3f7;
           border-radius:6px; padding:8px 12px; font-size:12px; pointer-events:none;
           z-index:100; backdrop-filter:blur(8px); max-width:300px;
           box-shadow:0 4px 20px rgba(0,0,0,0.4); display:none; }
.tooltip .tt-label { font-weight:600; color:#4fc3f7; }
.tooltip .tt-val { color:#e0e0e0; }

svg text { user-select:none; }

/* Metrics panel */
.metrics-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px 20px; }
.metric-item { display:flex; justify-content:space-between; font-size:12px;
               padding:4px 8px; border-radius:4px; }
.metric-item:hover { background:rgba(79,195,247,0.08); }
.metric-label { opacity:0.7; }
.metric-value { font-weight:600; font-family:'SF Mono',monospace; }
.cci-box { grid-column:1/-1; text-align:center; margin-top:10px; padding:14px;
           border-radius:8px; background:rgba(0,0,0,0.25); border:1px solid #333; }
.cci-score { font-size:32px; font-weight:700; }
.cci-label { font-size:14px; margin-top:2px; }
.cci-desc { font-size:11px; opacity:0.6; margin-top:4px; }

.sub-header { font-size:11px; font-weight:600; text-transform:uppercase;
              letter-spacing:1px; opacity:0.4; margin:8px 0 4px; grid-column:1/-1; }

/* Heatmap */
.hm-cell { cursor:pointer; transition:opacity 0.15s; }
.hm-cell:hover { opacity:0.8; stroke:#4fc3f7; stroke-width:2; }

/* Eigenvalue bars */
.ev-bar { cursor:pointer; transition:opacity 0.15s; }
.ev-bar:hover { opacity:0.8; }

/* Fiedler bars */
.fi-bar { cursor:pointer; transition:opacity 0.15s; }
.fi-bar:hover { opacity:0.85; }
</style>
</head>
<body>

<div class="tab-bar">
  <div class="title">swactor &mdash; dependency analysis</div>
  <button class="tab active" data-tab="dag">Dependency DAG</button>
  <button class="tab" data-tab="spectral">Spectral Analysis</button>
</div>

<div class="tab-content active" id="tab-dag">
  <div id="dag-controls">
    <button onclick="zoomIn()">+</button>
    <button onclick="zoomOut()">&minus;</button>
    <button onclick="resetView()">fit</button>
    <span style="margin-left:8px;opacity:0.6">scroll to zoom &middot; drag to pan &middot; click node to focus</span>
  </div>
  <div id="dag-viewport"></div>
  <div id="dag-loading">Loading Graphviz&hellip;</div>
</div>

<div class="tab-content" id="tab-spectral">
  <div class="grid">
    <div class="panel" id="panel-eigenvalues">
      <h2><span class="icon">&#x03BB;</span> Laplacian Eigenvalue Spectrum</h2>
      <svg id="svg-eigenvalues"></svg>
    </div>

    <div class="panel" id="panel-fiedler">
      <h2><span class="icon">&#x2702;</span> Fiedler Vector &mdash; Spectral Bisection</h2>
      <svg id="svg-fiedler"></svg>
    </div>

    <div class="panel" id="panel-heatmap">
      <h2><span class="icon">&#x25A6;</span> Module Coupling (directed edge counts)</h2>
      <svg id="svg-heatmap"></svg>
    </div>

    <div class="panel" id="panel-metrics">
      <h2><span class="icon">&#x2211;</span> Complexity Metrics</h2>
      <div id="metrics-content"></div>
    </div>
  </div>
</div>

<div class="tooltip" id="tooltip"></div>

<!-- ─── Script 1: synchronous — data + tab switching + spectral panels ─── -->
<script>
// ─── Data ──────────────────────────────────────────────────────────────────
const DATA = __DATA_BLOB__;
const { eigenvalues, fiedler, coupling, metrics, module_colors } = DATA;

// ─── Tab switching ─────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
  });
});

// ─── Tooltip ───────────────────────────────────────────────────────────────
const TT = document.getElementById('tooltip');
function showTip(evt, html) {
  TT.innerHTML = html;
  TT.style.display = 'block';
  const x = evt.clientX + 14, y = evt.clientY - 10;
  TT.style.left = Math.min(x, window.innerWidth - TT.offsetWidth - 20) + 'px';
  TT.style.top = Math.min(y, window.innerHeight - TT.offsetHeight - 20) + 'px';
}
function hideTip() { TT.style.display = 'none'; }

function modColor(mod) { return module_colors[mod] || '#9e9e9e'; }

// ─── Eigenvalue Spectrum ───────────────────────────────────────────────────
(function() {
  const svg = document.getElementById('svg-eigenvalues');
  const W = 560, H = 300, M = {t:20,r:20,b:40,l:50};
  const w = W-M.l-M.r, h = H-M.t-M.b;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

  const maxVal = Math.max(...eigenvalues.map(d=>d.value), 1);
  const xScale = i => M.l + (i / (eigenvalues.length-1||1)) * w;
  const yScale = v => M.t + h - (v / maxVal) * h;

  // Grid lines
  for (let tick = 0; tick <= maxVal; tick += Math.ceil(maxVal/5)) {
    const y = yScale(tick);
    const line = document.createElementNS('http://www.w3.org/2000/svg','line');
    Object.entries({x1:M.l,x2:W-M.r,y1:y,y2:y,stroke:'#2a2a5a','stroke-width':0.5}).forEach(([k,v])=>line.setAttribute(k,v));
    svg.appendChild(line);
    const txt = document.createElementNS('http://www.w3.org/2000/svg','text');
    txt.setAttribute('x', M.l-6); txt.setAttribute('y', y+4);
    txt.setAttribute('text-anchor','end'); txt.setAttribute('fill','#888'); txt.setAttribute('font-size','10');
    txt.textContent = tick.toFixed(0);
    svg.appendChild(txt);
  }

  // Axis labels
  const xLabel = document.createElementNS('http://www.w3.org/2000/svg','text');
  xLabel.setAttribute('x', M.l+w/2); xLabel.setAttribute('y', H-4);
  xLabel.setAttribute('text-anchor','middle'); xLabel.setAttribute('fill','#888'); xLabel.setAttribute('font-size','11');
  xLabel.textContent = 'Index';
  svg.appendChild(xLabel);

  const yLabel = document.createElementNS('http://www.w3.org/2000/svg','text');
  yLabel.setAttribute('x', 14); yLabel.setAttribute('y', M.t+h/2);
  yLabel.setAttribute('text-anchor','middle'); yLabel.setAttribute('fill','#888');
  yLabel.setAttribute('font-size','11'); yLabel.setAttribute('transform', `rotate(-90,14,${M.t+h/2})`);
  yLabel.textContent = 'Eigenvalue';
  svg.appendChild(yLabel);

  eigenvalues.forEach((d, i) => {
    const x = xScale(i), y = yScale(d.value), y0 = yScale(0);
    // Stem line
    const line = document.createElementNS('http://www.w3.org/2000/svg','line');
    Object.entries({x1:x,x2:x,y1:y0,y2:y,stroke:i===1?'#ff4444':'#4fc3f7','stroke-width':i===1?2.5:1.5,'stroke-opacity':i===1?1:0.6}).forEach(([k,v])=>line.setAttribute(k,v));
    svg.appendChild(line);
    // Dot
    const circ = document.createElementNS('http://www.w3.org/2000/svg','circle');
    circ.setAttribute('cx',x); circ.setAttribute('cy',y);
    circ.setAttribute('r', i===1?6:3.5);
    circ.setAttribute('fill', i===1?'#ff4444':'#4fc3f7');
    circ.classList.add('ev-bar');
    circ.addEventListener('mousemove', e => showTip(e,
      `<span class="tt-label">&lambda;<sub>${i}</sub></span> = <span class="tt-val">${d.value.toFixed(4)}</span>`
      + (i===1 ? '<br><span style="color:#ff4444">Fiedler value (algebraic connectivity)</span>' : '')
    ));
    circ.addEventListener('mouseleave', hideTip);
    svg.appendChild(circ);
  });

  // Fiedler label
  if (eigenvalues.length > 1) {
    const lbl = document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.setAttribute('x', xScale(1)+10); lbl.setAttribute('y', yScale(eigenvalues[1].value)-6);
    lbl.setAttribute('fill','#ff4444'); lbl.setAttribute('font-size','11'); lbl.setAttribute('font-weight','600');
    lbl.textContent = `\u03BB\u2082 = ${metrics.fiedler_value}`;
    svg.appendChild(lbl);
  }
})();

// ─── Fiedler Vector ────────────────────────────────────────────────────────
(function() {
  const svg = document.getElementById('svg-fiedler');
  const n = fiedler.length;
  const barH = Math.max(12, Math.min(22, 500/n));
  const W = 560, H = Math.max(300, n*barH + 60), M = {t:10,r:20,b:30,l:140};
  const w = W-M.l-M.r, h = H-M.t-M.b;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

  const maxAbs = Math.max(...fiedler.map(d=>Math.abs(d.value)), 0.01);
  const xScale = v => M.l + w/2 + (v/maxAbs) * (w/2);
  const yScale = i => M.t + (i/n) * h + barH/2;

  // Zero line
  const zl = document.createElementNS('http://www.w3.org/2000/svg','line');
  Object.entries({x1:xScale(0),x2:xScale(0),y1:M.t,y2:M.t+h,stroke:'#ff4444','stroke-width':1.5,'stroke-dasharray':'5,3','stroke-opacity':0.7}).forEach(([k,v])=>zl.setAttribute(k,v));
  svg.appendChild(zl);

  // Partition labels
  const negCount = fiedler.filter(d=>d.value<0).length;
  if (negCount > 0 && negCount < n) {
    const lblA = document.createElementNS('http://www.w3.org/2000/svg','text');
    lblA.setAttribute('x', M.l+4); lblA.setAttribute('y', M.t + (negCount/n)*h/2 + barH/2);
    lblA.setAttribute('fill','#ff4444'); lblA.setAttribute('font-size','10'); lblA.setAttribute('opacity','0.5');
    lblA.textContent = 'Partition A';
    svg.appendChild(lblA);
  }

  fiedler.forEach((d, i) => {
    const x0 = xScale(0), x1 = xScale(d.value);
    const y = yScale(i) - barH*0.4;
    const bw = Math.abs(x1-x0);

    const rect = document.createElementNS('http://www.w3.org/2000/svg','rect');
    rect.setAttribute('x', Math.min(x0,x1)); rect.setAttribute('y', y);
    rect.setAttribute('width', Math.max(bw, 1)); rect.setAttribute('height', barH*0.8);
    rect.setAttribute('rx', 2);
    rect.setAttribute('fill', modColor(d.module));
    rect.setAttribute('opacity', 0.85);
    rect.classList.add('fi-bar');
    rect.addEventListener('mousemove', e => showTip(e,
      `<span class="tt-label">${d.name}</span><br>` +
      `Module: <span class="tt-val">${d.module}</span><br>` +
      `Fiedler: <span class="tt-val">${d.value.toFixed(4)}</span><br>` +
      `Partition: <span class="tt-val">${d.value < 0 ? 'A' : 'B'}</span>`
    ));
    rect.addEventListener('mouseleave', hideTip);
    svg.appendChild(rect);

    // Label
    const txt = document.createElementNS('http://www.w3.org/2000/svg','text');
    txt.setAttribute('x', M.l-4); txt.setAttribute('y', yScale(i)+3);
    txt.setAttribute('text-anchor','end'); txt.setAttribute('fill','#ccc');
    txt.setAttribute('font-size', Math.min(11, barH*0.75));
    txt.textContent = d.name;
    svg.appendChild(txt);
  });

  // X axis label
  const xLabel = document.createElementNS('http://www.w3.org/2000/svg','text');
  xLabel.setAttribute('x', M.l+w/2); xLabel.setAttribute('y', H-6);
  xLabel.setAttribute('text-anchor','middle'); xLabel.setAttribute('fill','#888'); xLabel.setAttribute('font-size','11');
  xLabel.textContent = 'Fiedler value';
  svg.appendChild(xLabel);
})();

// ─── Module Coupling Heatmap ───────────────────────────────────────────────
(function() {
  const mods = coupling.modules;
  const mat = coupling.matrix;
  const n = mods.length;
  const svg = document.getElementById('svg-heatmap');
  const cellSz = Math.min(55, 400/n);
  const M = {t:10,r:60,b:80,l:100};
  const W = M.l + n*cellSz + M.r, H = M.t + n*cellSz + M.b;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

  const maxVal = Math.max(...mat.flat(), 1);

  // Color scale: 0=transparent dark, max=deep red
  function heatColor(v) {
    if (v === 0) return '#1a1a2e';
    const t = v / maxVal;
    const r = Math.round(40 + 215*t);
    const g = Math.round(30 + 40*(1-t));
    const b = Math.round(50*(1-t));
    return `rgb(${r},${g},${b})`;
  }

  for (let i = 0; i < n; i++) {
    // Row labels
    const rl = document.createElementNS('http://www.w3.org/2000/svg','text');
    rl.setAttribute('x', M.l-8); rl.setAttribute('y', M.t + i*cellSz + cellSz/2 + 4);
    rl.setAttribute('text-anchor','end'); rl.setAttribute('fill', modColor(mods[i]));
    rl.setAttribute('font-size','11'); rl.setAttribute('font-weight','600');
    rl.textContent = mods[i];
    svg.appendChild(rl);

    // Column labels
    const cl = document.createElementNS('http://www.w3.org/2000/svg','text');
    cl.setAttribute('x', M.l + i*cellSz + cellSz/2);
    cl.setAttribute('y', M.t + n*cellSz + 16);
    cl.setAttribute('text-anchor','end'); cl.setAttribute('fill', modColor(mods[i]));
    cl.setAttribute('font-size','11'); cl.setAttribute('font-weight','600');
    cl.setAttribute('transform', `rotate(-45, ${M.l + i*cellSz + cellSz/2}, ${M.t + n*cellSz + 16})`);
    cl.textContent = mods[i];
    svg.appendChild(cl);

    for (let j = 0; j < n; j++) {
      const v = mat[i][j];
      const rect = document.createElementNS('http://www.w3.org/2000/svg','rect');
      rect.setAttribute('x', M.l + j*cellSz + 1);
      rect.setAttribute('y', M.t + i*cellSz + 1);
      rect.setAttribute('width', cellSz-2); rect.setAttribute('height', cellSz-2);
      rect.setAttribute('rx', 3);
      rect.setAttribute('fill', heatColor(v));
      rect.classList.add('hm-cell');
      rect.addEventListener('mousemove', e => showTip(e,
        `<span class="tt-label">${mods[i]} &rarr; ${mods[j]}</span><br>` +
        `Edges: <span class="tt-val">${v}</span>` +
        (i !== j ? '<br><span style="opacity:0.6">cross-module</span>' : '<br><span style="opacity:0.6">intra-module</span>')
      ));
      rect.addEventListener('mouseleave', hideTip);
      svg.appendChild(rect);

      // Cell text
      if (v > 0) {
        const txt = document.createElementNS('http://www.w3.org/2000/svg','text');
        txt.setAttribute('x', M.l + j*cellSz + cellSz/2);
        txt.setAttribute('y', M.t + i*cellSz + cellSz/2 + 4);
        txt.setAttribute('text-anchor','middle'); txt.setAttribute('font-size','11');
        txt.setAttribute('font-weight','700'); txt.setAttribute('pointer-events','none');
        txt.setAttribute('fill', v > maxVal*0.5 ? '#fff' : '#ccc');
        txt.textContent = v;
        svg.appendChild(txt);
      }
    }
  }

  // Axis labels
  const srcL = document.createElementNS('http://www.w3.org/2000/svg','text');
  srcL.setAttribute('x', 10); srcL.setAttribute('y', M.t + n*cellSz/2);
  srcL.setAttribute('text-anchor','middle'); srcL.setAttribute('fill','#666');
  srcL.setAttribute('font-size','10');
  srcL.setAttribute('transform', `rotate(-90,10,${M.t + n*cellSz/2})`);
  srcL.textContent = 'source module';
  svg.appendChild(srcL);
})();

// ─── Metrics Panel ─────────────────────────────────────────────────────────
(function() {
  const c = document.getElementById('metrics-content');
  const mm = metrics;
  c.innerHTML = `
    <div class="metrics-grid">
      <div class="sub-header">Graph</div>
      <div class="metric-item"><span class="metric-label">Nodes</span><span class="metric-value">${mm.n_nodes}</span></div>
      <div class="metric-item"><span class="metric-label">Directed edges</span><span class="metric-value">${mm.n_edges}</span></div>
      <div class="metric-item"><span class="metric-label">Modules</span><span class="metric-value">${mm.n_modules}</span></div>
      <div class="metric-item"><span class="metric-label">Components</span><span class="metric-value">${mm.connected_components}</span></div>

      <div class="sub-header">Spectral</div>
      <div class="metric-item"><span class="metric-label">&lambda;<sub>2</sub> (alg. connectivity)</span><span class="metric-value">${mm.algebraic_connectivity}</span></div>
      <div class="metric-item"><span class="metric-label">&lambda;<sub>2</sub>/n (normalized)</span><span class="metric-value">${mm.normalized_algebraic_connectivity}</span></div>
      <div class="metric-item"><span class="metric-label">Spectral entropy</span><span class="metric-value">${mm.spectral_entropy}</span></div>
      <div class="metric-item"><span class="metric-label">Norm. entropy</span><span class="metric-value">${mm.normalized_spectral_entropy}</span></div>
      <div class="metric-item"><span class="metric-label">Spectral radius</span><span class="metric-value">${mm.spectral_radius}</span></div>
      <div class="metric-item"><span class="metric-label">Norm. radius</span><span class="metric-value">${mm.normalized_spectral_radius}</span></div>

      <div class="sub-header">Coupling</div>
      <div class="metric-item"><span class="metric-label">Edge density</span><span class="metric-value">${mm.edge_density}</span></div>
      <div class="metric-item"><span class="metric-label">Cross-module ratio</span><span class="metric-value">${(mm.cross_module_ratio*100).toFixed(1)}%</span></div>

      <div class="cci-box">
        <div class="cci-score" style="color:${mm.cci_color}">CCI = ${mm.cci}</div>
        <div class="cci-label" style="color:${mm.cci_color}">${mm.cci_label}</div>
        <div class="cci-desc">${mm.cci_desc}</div>
      </div>
    </div>
  `;
})();
</script>

<!-- ─── Script 2: module — viz-js DAG rendering (async) ─────────────────── -->
<script type="module">
import { instance } from 'https://cdn.jsdelivr.net/npm/@viz-js/viz@3.11.0/lib/viz-standalone.mjs';

const DOT_SOURCE = `__DOT_BLOB__`;

const viz = await instance();
const svg = viz.renderSVGElement(DOT_SOURCE);
document.getElementById('dag-loading').remove();

const vp = document.getElementById('dag-viewport');
vp.appendChild(svg);

// ─── Dark-mode SVG recoloring ──────────────────────────────────────────────
svg.querySelectorAll('polygon[fill="white"]').forEach(el => el.setAttribute('fill','#1a1a2e'));
svg.querySelectorAll('.graph > text, .cluster > text, .edge text').forEach(el => el.setAttribute('fill','#e0e0e0'));
svg.querySelectorAll('.node text').forEach(el => el.setAttribute('fill','#1a1a1a'));

// ─── Click-to-focus ────────────────────────────────────────────────────────
const edges = svg.querySelectorAll('.edge');
const nodes = svg.querySelectorAll('.node');
const clusterChrome = [];
svg.querySelectorAll('.cluster').forEach(c => {
  c.querySelectorAll(':scope > path, :scope > polygon, :scope > text').forEach(el => clusterChrome.push(el));
});

const nodeByTitle = new Map();
nodes.forEach(n => {
  const t = n.querySelector('title');
  if (t) nodeByTitle.set(t.textContent.trim(), n);
});

const nodeToClusterEls = new Map();
svg.querySelectorAll('.cluster').forEach(cluster => {
  const chrome = [...cluster.querySelectorAll(':scope > path, :scope > polygon, :scope > text')];
  cluster.querySelectorAll('.node title').forEach(t => {
    nodeToClusterEls.set(t.textContent.trim(), chrome);
  });
});

const adj = new Map();
edges.forEach(edge => {
  const t = edge.querySelector('title');
  if (!t) return;
  const parts = t.textContent.trim().split('->').map(s => s.trim());
  if (parts.length !== 2) return;
  const [src, dst] = parts;
  if (!adj.has(src)) adj.set(src, { edges: [], neighbors: new Set() });
  if (!adj.has(dst)) adj.set(dst, { edges: [], neighbors: new Set() });
  adj.get(src).edges.push(edge);
  adj.get(src).neighbors.add(dst);
  adj.get(dst).edges.push(edge);
  adj.get(dst).neighbors.add(src);
});

const DIM = 0.08;
let focused = null;

function clearFocus() {
  focused = null;
  nodes.forEach(n => n.style.opacity = '');
  edges.forEach(e => e.style.opacity = '');
  clusterChrome.forEach(el => el.style.opacity = '');
}

function focusNode(title) {
  if (focused === title) { clearFocus(); return; }
  focused = title;
  const info = adj.get(title) || { edges: [], neighbors: new Set() };
  const connected = new Set([title, ...info.neighbors]);

  nodes.forEach(n => n.style.opacity = DIM);
  edges.forEach(e => e.style.opacity = DIM);
  clusterChrome.forEach(el => el.style.opacity = DIM);

  connected.forEach(name => {
    const el = nodeByTitle.get(name);
    if (el) el.style.opacity = 1;
  });

  info.edges.forEach(e => e.style.opacity = 1);

  const seen = new Set();
  connected.forEach(name => {
    const chrome = nodeToClusterEls.get(name);
    if (chrome) chrome.forEach(el => {
      if (!seen.has(el)) { seen.add(el); el.style.opacity = 1; }
    });
  });
}

nodes.forEach(node => {
  node.style.cursor = 'pointer';
  node.addEventListener('click', e => {
    e.stopPropagation();
    const t = node.querySelector('title');
    if (t) focusNode(t.textContent.trim());
  });
});

// ─── Pan & zoom ────────────────────────────────────────────────────────────
let scale = 1, tx = 0, ty = 0, dragging = false, didDrag = false, sx = 0, sy = 0;
function applyTransform() { svg.style.transform = `translate(${tx}px,${ty}px) scale(${scale})`; svg.style.transformOrigin = '0 0'; }

window.resetView = function() {
  const vw = vp.clientWidth, vh = vp.clientHeight;
  const bb = svg.getBBox();
  scale = Math.min(vw / bb.width, vh / bb.height) * 0.92;
  tx = (vw - bb.width * scale) / 2;
  ty = (vh - bb.height * scale) / 2;
  applyTransform();
};
resetView();

window.zoomIn = function() { scale *= 1.3; applyTransform(); };
window.zoomOut = function() { scale *= 0.7; applyTransform(); };

vp.addEventListener('wheel', e => { e.preventDefault(); const f = e.deltaY < 0 ? 1.12 : 0.89; const rect = vp.getBoundingClientRect(); const mx = e.clientX - rect.left; const my = e.clientY - rect.top; tx = mx - f * (mx - tx); ty = my - f * (my - ty); scale *= f; applyTransform(); }, { passive:false });
vp.addEventListener('pointerdown', e => { dragging=true; didDrag=false; sx=e.clientX-tx; sy=e.clientY-ty; vp.setPointerCapture(e.pointerId); });
vp.addEventListener('pointermove', e => { if(!dragging) return; didDrag=true; tx=e.clientX-sx; ty=e.clientY-sy; applyTransform(); });
vp.addEventListener('pointerup', () => dragging=false);
vp.addEventListener('click', e => { if (!didDrag && !e.target.closest('.node')) clearFocus(); });
</script>
</body></html>
"""


# ─── JSON Output ──────────────────────────────────────────────────────────────

def metrics_to_dict(result: AnalysisResult) -> dict[str, Any]:
    """Convert analysis results to a JSON-serializable dict."""
    m = result.metrics
    s = result.spectral
    c = result.coupling

    return {
        "graph": {
            "n_nodes": m.n_nodes,
            "n_edges": m.n_edges,
            "n_modules": m.n_modules,
            "connected_components": m.connected_components,
            "modules": c.module_names,
        },
        "spectral": {
            "eigenvalues": s.eigenvalues.tolist(),
            "fiedler_value": s.fiedler_value,
            "fiedler_vector": s.fiedler_vector.tolist(),
            "node_names": s.node_names,
            "node_modules": s.node_modules,
        },
        "module_coupling": {
            "module_names": c.module_names,
            "coupling_matrix": c.coupling_matrix.tolist(),
            "cross_module_edges": c.cross_module_edges,
            "total_edges": c.total_edges,
        },
        "metrics": {
            "algebraic_connectivity": m.algebraic_connectivity,
            "normalized_algebraic_connectivity": m.normalized_algebraic_connectivity,
            "spectral_entropy": m.spectral_entropy,
            "normalized_spectral_entropy": m.normalized_spectral_entropy,
            "edge_density": m.edge_density,
            "cross_module_ratio": m.cross_module_ratio,
            "spectral_radius": m.spectral_radius,
            "normalized_spectral_radius": m.normalized_spectral_radius,
            "cci": m.cci,
        },
    }


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spectral analysis of dependency DAGs"
    )
    parser.add_argument("dot_file", help="Path to DOT file (from depgraph)")
    parser.add_argument("-o", "--output-dir", default=".",
                        help="Output directory (default: current directory)")
    parser.add_argument("--no-plots", action="store_true",
                        help="Text report only (no matplotlib dependency)")
    parser.add_argument("--json", action="store_true",
                        help="Also output spectral_metrics.json")
    args = parser.parse_args()

    # Read and parse DOT
    dot_text = open(args.dot_file).read()
    graph = parse_dot(dot_text)
    print(f"Parsed {len(graph.nodes)} nodes, {len(graph.edges)} edges, "
          f"{len(graph.modules)} modules")

    # Run analysis
    result = run_analysis(graph)

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Generate report
    report = generate_report(result)
    print(report)
    report_path = os.path.join(args.output_dir, "spectral_report.txt")
    with open(report_path, "w") as f:
        f.write(report)
    print(f"\nReport saved to {report_path}")

    # Generate interactive HTML dashboard
    html_path = os.path.join(args.output_dir, "spectral_dashboard.html")
    generate_dashboard_html(result, html_path, dot_source=dot_text)
    print(f"Interactive dashboard saved to {html_path}")

    # Generate static PNG dashboard
    if not args.no_plots:
        dashboard_path = os.path.join(args.output_dir, "spectral_dashboard.png")
        generate_dashboard(result, dashboard_path)
        print(f"Static dashboard saved to {dashboard_path}")

    # Generate JSON
    if args.json:
        json_path = os.path.join(args.output_dir, "spectral_metrics.json")
        with open(json_path, "w") as f:
            json.dump(metrics_to_dict(result), f, indent=2)
        print(f"JSON saved to {json_path}")


if __name__ == "__main__":
    main()
