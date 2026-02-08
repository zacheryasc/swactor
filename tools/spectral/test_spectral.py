#!/usr/bin/env python3
"""Comprehensive tests for the spectral analysis tool."""

from __future__ import annotations

import copy
import json
import math
import os
import random
import tempfile
import unittest

import numpy as np

from spectral_analysis import (
    AnalysisResult,
    ComplexityMetrics,
    DependencyGraph,
    Edge,
    ModuleCouplingResult,
    Node,
    SpectralResults,
    build_adjacency,
    build_laplacian,
    compute_complexity_metrics,
    compute_module_coupling,
    compute_spectral,
    compute_spectral_entropy,
    count_connected_components,
    generate_report,
    get_node_ordering,
    metrics_to_dict,
    parse_dot,
    run_analysis,
    symmetrize,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_graph(
    names: list[str],
    modules: list[str],
    edge_pairs: list[tuple[str, str]],
    module_order: list[str] | None = None,
) -> DependencyGraph:
    """Build a DependencyGraph from names, module assignments, and edges."""
    assert len(names) == len(modules)
    graph = DependencyGraph()
    seen_modules: list[str] = []
    for name, mod in zip(names, modules):
        graph.nodes.append(Node(name=name, module=mod))
        graph.node_to_module[name] = mod
        if mod not in seen_modules:
            seen_modules.append(mod)
    if module_order is not None:
        graph.modules = module_order
    else:
        graph.modules = seen_modules
    for src, tgt in edge_pairs:
        src_mod = graph.node_to_module.get(src, "")
        tgt_mod = graph.node_to_module.get(tgt, "")
        cross = src_mod != tgt_mod
        graph.edges.append(Edge(
            source=src, target=tgt, label="dep",
            edge_type="field", cross_module=cross,
        ))
    return graph


# ─── DOT Parser Tests ─────────────────────────────────────────────────────────

class TestDotParser(unittest.TestCase):
    def test_minimal_dot(self):
        dot = '''digraph test {
    subgraph cluster_mod1 {
        label="mod1";
        A [label="A", fillcolor="#fff"];
    }
    A -> A [label="self", style=dashed, color="#666", penwidth=1];
}'''
        g = parse_dot(dot)
        self.assertEqual(len(g.nodes), 1)
        self.assertEqual(g.nodes[0].name, "A")
        self.assertEqual(g.nodes[0].module, "mod1")
        self.assertEqual(len(g.edges), 1)

    def test_two_module_dot(self):
        dot = '''digraph test {
    subgraph cluster_alpha {
        label="alpha";
        X [label="X"];
        Y [label="Y"];
    }
    subgraph cluster_beta {
        label="beta";
        Z [label="Z"];
    }
    X -> Y [label="dep", style=dashed, color="#666", penwidth=1];
    X -> Z [label="dep", style=solid, color="#00f", penwidth=1.5];
}'''
        g = parse_dot(dot)
        self.assertEqual(len(g.nodes), 3)
        self.assertEqual(len(g.modules), 2)
        self.assertEqual(g.modules, ["alpha", "beta"])
        self.assertEqual(g.node_to_module["X"], "alpha")
        self.assertEqual(g.node_to_module["Z"], "beta")

        # Edge classification
        intra = [e for e in g.edges if not e.cross_module]
        cross = [e for e in g.edges if e.cross_module]
        self.assertEqual(len(intra), 1)
        self.assertEqual(len(cross), 1)

    def test_trait_impl_classification(self):
        dot = '''digraph test {
    subgraph cluster_m {
        label="m";
        A [label="A"];
        B [label="B"];
    }
    A -> B [label="impl", style=dotted, color="#666", penwidth=1];
}'''
        g = parse_dot(dot)
        self.assertEqual(g.edges[0].edge_type, "trait_impl")

    def test_real_deps_dot(self):
        """Parse the real deps.dot and verify expected counts."""
        dot_path = os.path.join(os.path.dirname(__file__), "..", "..", "deps.dot")
        if not os.path.exists(dot_path):
            self.skipTest("deps.dot not found")
        with open(dot_path) as f:
            dot = f.read()
        g = parse_dot(dot)
        self.assertEqual(len(g.nodes), 36, f"Expected 36 nodes, got {len(g.nodes)}")
        self.assertEqual(len(g.edges), 89, f"Expected 89 edges, got {len(g.edges)}")
        self.assertEqual(len(g.modules), 8, f"Expected 8 modules, got {len(g.modules)}")

    def test_empty_dot(self):
        dot = "digraph empty {}"
        g = parse_dot(dot)
        self.assertEqual(len(g.nodes), 0)
        self.assertEqual(len(g.edges), 0)


# ─── Matrix Construction Tests ────────────────────────────────────────────────

class TestMatrixConstruction(unittest.TestCase):
    def test_two_node_adjacency(self):
        g = _make_graph(["A", "B"], ["m", "m"], [("A", "B")])
        order = get_node_ordering(g)
        A = build_adjacency(g, order)
        self.assertEqual(A.shape, (2, 2))
        idx_a = order.index("A")
        idx_b = order.index("B")
        self.assertEqual(A[idx_a, idx_b], 1.0)
        self.assertEqual(A[idx_b, idx_a], 0.0)

    def test_symmetrize_directed(self):
        A = np.array([[0, 1, 0],
                      [0, 0, 1],
                      [0, 0, 0]], dtype=float)
        S = symmetrize(A)
        expected = np.array([[0, 1, 0],
                             [1, 0, 1],
                             [0, 1, 0]], dtype=float)
        np.testing.assert_array_equal(S, expected)

    def test_symmetrize_idempotent(self):
        """Symmetrizing an already-symmetric matrix should not change it."""
        A = np.array([[0, 1, 1],
                      [1, 0, 1],
                      [1, 1, 0]], dtype=float)
        S = symmetrize(A)
        np.testing.assert_array_equal(S, A)

    def test_laplacian_p3(self):
        """Path graph P3: A-B-C."""
        A_sym = np.array([[0, 1, 0],
                          [1, 0, 1],
                          [0, 1, 0]], dtype=float)
        L = build_laplacian(A_sym)
        expected = np.array([[1, -1, 0],
                             [-1, 2, -1],
                             [0, -1, 1]], dtype=float)
        np.testing.assert_array_equal(L, expected)

    def test_laplacian_k3(self):
        """Complete graph K3."""
        A_sym = np.array([[0, 1, 1],
                          [1, 0, 1],
                          [1, 1, 0]], dtype=float)
        L = build_laplacian(A_sym)
        expected = np.array([[2, -1, -1],
                             [-1, 2, -1],
                             [-1, -1, 2]], dtype=float)
        np.testing.assert_array_equal(L, expected)


# ─── Spectral Analysis Tests ─────────────────────────────────────────────────

class TestSpectralAnalysis(unittest.TestCase):
    def test_p3_eigenvalues(self):
        """Path P3 should have eigenvalues {0, 1, 3}."""
        g = _make_graph(["A", "B", "C"], ["m", "m", "m"],
                        [("A", "B"), ("B", "C")])
        s = compute_spectral(g)
        np.testing.assert_allclose(sorted(s.eigenvalues), [0, 1, 3], atol=1e-10)

    def test_k4_eigenvalues(self):
        """Complete K4 should have eigenvalues {0, 4, 4, 4}."""
        names = ["A", "B", "C", "D"]
        edges = [(a, b) for a in names for b in names if a != b]
        g = _make_graph(names, ["m"] * 4, edges)
        s = compute_spectral(g)
        np.testing.assert_allclose(sorted(s.eigenvalues), [0, 4, 4, 4], atol=1e-10)

    def test_star_s4_fiedler(self):
        """Star graph S4 (center + 3 leaves): lambda_2 = 1."""
        g = _make_graph(
            ["C", "L1", "L2", "L3"], ["m"] * 4,
            [("C", "L1"), ("C", "L2"), ("C", "L3")],
        )
        s = compute_spectral(g)
        self.assertAlmostEqual(s.fiedler_value, 1.0, places=10)

    def test_disconnected_graph(self):
        """Disconnected graph should have lambda_2 = 0."""
        g = _make_graph(
            ["A", "B", "C", "D"], ["m1", "m1", "m2", "m2"],
            [("A", "B"), ("C", "D")],
            module_order=["m1", "m2"],
        )
        s = compute_spectral(g)
        self.assertAlmostEqual(s.fiedler_value, 0.0, places=10)

    def test_barbell_fiedler_separation(self):
        """Barbell graph: two K3 cliques connected by a bridge.

        Fiedler vector should separate the two cliques (different signs).
        """
        # Clique 1: A, B, C fully connected
        # Clique 2: D, E, F fully connected
        # Bridge: C-D
        names = ["A", "B", "C", "D", "E", "F"]
        edges = [
            ("A", "B"), ("A", "C"), ("B", "C"),
            ("D", "E"), ("D", "F"), ("E", "F"),
            ("C", "D"),
        ]
        g = _make_graph(names, ["m1", "m1", "m1", "m2", "m2", "m2"], edges,
                        module_order=["m1", "m2"])
        s = compute_spectral(g)

        # Clique 1 nodes should have same sign, clique 2 opposite
        order = s.node_names
        fv = s.fiedler_vector
        idx = {name: i for i, name in enumerate(order)}

        clique1_signs = [np.sign(fv[idx[n]]) for n in ["A", "B", "C"]]
        clique2_signs = [np.sign(fv[idx[n]]) for n in ["D", "E", "F"]]

        # All in clique 1 should have the same sign
        self.assertTrue(all(s == clique1_signs[0] for s in clique1_signs),
                        f"Clique 1 signs should be uniform: {clique1_signs}")
        # All in clique 2 should have the same sign
        self.assertTrue(all(s == clique2_signs[0] for s in clique2_signs),
                        f"Clique 2 signs should be uniform: {clique2_signs}")
        # The two cliques should have opposite signs
        self.assertNotEqual(clique1_signs[0], clique2_signs[0],
                            "Cliques should have opposite Fiedler signs")

    def test_single_node(self):
        g = _make_graph(["A"], ["m"], [])
        s = compute_spectral(g)
        self.assertEqual(s.fiedler_value, 0.0)
        self.assertEqual(len(s.eigenvalues), 1)

    def test_empty_graph(self):
        g = DependencyGraph()
        s = compute_spectral(g)
        self.assertEqual(s.fiedler_value, 0.0)
        self.assertEqual(len(s.eigenvalues), 0)


# ─── Module Coupling Tests ────────────────────────────────────────────────────

class TestModuleCoupling(unittest.TestCase):
    def test_directed_counts(self):
        g = _make_graph(
            ["A", "B", "C"], ["m1", "m1", "m2"],
            [("A", "C"), ("B", "C"), ("C", "A")],
            module_order=["m1", "m2"],
        )
        c = compute_module_coupling(g)
        # m1->m2: 2 edges (A->C, B->C)
        # m2->m1: 1 edge (C->A)
        idx_m1 = c.module_names.index("m1")
        idx_m2 = c.module_names.index("m2")
        self.assertEqual(c.coupling_matrix[idx_m1, idx_m2], 2.0)
        self.assertEqual(c.coupling_matrix[idx_m2, idx_m1], 1.0)

    def test_cross_module_ratio(self):
        g = _make_graph(
            ["A", "B", "C", "D"], ["m1", "m1", "m2", "m2"],
            [("A", "B"), ("A", "C"), ("C", "D")],
            module_order=["m1", "m2"],
        )
        c = compute_module_coupling(g)
        # 1 cross-module edge (A->C) out of 3 total
        self.assertEqual(c.cross_module_edges, 1)
        self.assertEqual(c.total_edges, 3)

    def test_intra_only(self):
        g = _make_graph(
            ["A", "B"], ["m1", "m1"],
            [("A", "B")],
            module_order=["m1"],
        )
        c = compute_module_coupling(g)
        self.assertEqual(c.cross_module_edges, 0)
        self.assertEqual(c.coupling_matrix[0, 0], 1.0)


# ─── Complexity Metrics Tests ─────────────────────────────────────────────────

class TestComplexityMetrics(unittest.TestCase):
    def test_k4_spectral_entropy(self):
        """K4 has uniform positive eigenvalues {4,4,4} -> entropy = log2(3)."""
        evals = np.array([0.0, 4.0, 4.0, 4.0])
        H = compute_spectral_entropy(evals)
        self.assertAlmostEqual(H, math.log2(3), places=10)

    def test_star_entropy_less_than_complete(self):
        """Star graph has less uniform eigenvalues than complete graph."""
        # Star S4: eigenvalues are 0, 1, 1, 4
        star_evals = np.array([0.0, 1.0, 1.0, 4.0])
        k4_evals = np.array([0.0, 4.0, 4.0, 4.0])
        H_star = compute_spectral_entropy(star_evals)
        H_k4 = compute_spectral_entropy(k4_evals)
        self.assertLess(H_star, H_k4)

    def test_cci_in_range(self):
        """CCI should always be in [0, 1]."""
        for _ in range(20):
            n = random.randint(2, 10)
            names = [f"N{i}" for i in range(n)]
            mods = [f"m{i % 3}" for i in range(n)]
            edges = []
            for _ in range(random.randint(1, n * 2)):
                a, b = random.sample(names, 2)
                edges.append((a, b))
            g = _make_graph(names, mods, edges,
                            module_order=sorted(set(mods)))
            result = run_analysis(g)
            self.assertGreaterEqual(result.metrics.cci, 0.0,
                                    "CCI should be >= 0")
            self.assertLessEqual(result.metrics.cci, 1.0,
                                 "CCI should be <= 1")

    def test_cci_increases_with_coupling(self):
        """Adding cross-module edges should increase CCI."""
        # Base graph: two modules, minimal coupling
        g1 = _make_graph(
            ["A", "B", "C", "D"], ["m1", "m1", "m2", "m2"],
            [("A", "B"), ("C", "D"), ("A", "C")],
            module_order=["m1", "m2"],
        )
        # More coupling
        g2 = _make_graph(
            ["A", "B", "C", "D"], ["m1", "m1", "m2", "m2"],
            [("A", "B"), ("C", "D"), ("A", "C"), ("A", "D"),
             ("B", "C"), ("B", "D"), ("C", "A"), ("D", "B")],
            module_order=["m1", "m2"],
        )
        r1 = run_analysis(g1)
        r2 = run_analysis(g2)
        self.assertLess(r1.metrics.cci, r2.metrics.cci)

    def test_connected_components(self):
        A_sym = np.array([
            [0, 1, 0, 0],
            [1, 0, 0, 0],
            [0, 0, 0, 1],
            [0, 0, 1, 0],
        ], dtype=float)
        self.assertEqual(count_connected_components(A_sym), 2)

    def test_single_component(self):
        A_sym = np.array([
            [0, 1, 1],
            [1, 0, 1],
            [1, 1, 0],
        ], dtype=float)
        self.assertEqual(count_connected_components(A_sym), 1)


# ─── Complexity Ladder ────────────────────────────────────────────────────────

class TestComplexityLadder(unittest.TestCase):
    """Verify CCI correctly orders synthetic codebases of increasing complexity."""

    def _rung1_linear_chain(self) -> DependencyGraph:
        """5 nodes in a single module, linear chain A->B->C->D->E."""
        return _make_graph(
            ["A", "B", "C", "D", "E"],
            ["m1"] * 5,
            [("A", "B"), ("B", "C"), ("C", "D"), ("D", "E")],
            module_order=["m1"],
        )

    def _rung2_clean_tree(self) -> DependencyGraph:
        """6 nodes across 2 modules, tree with mostly intra-module edges."""
        return _make_graph(
            ["R", "A", "B", "C", "D", "E"],
            ["core", "core", "core", "util", "util", "util"],
            [
                ("R", "A"), ("A", "B"), ("R", "C"),  # intra core
                ("D", "E"),  # intra util
                ("R", "D"), ("C", "E"),  # 2 cross edges
            ],
            module_order=["core", "util"],
        )

    def _rung3_layered_dag(self) -> DependencyGraph:
        """8 nodes across 3 modules in a layered architecture."""
        return _make_graph(
            ["C1", "C2", "S1", "S2", "S3", "D1", "D2", "D3"],
            ["ctrl", "ctrl", "svc", "svc", "svc", "data", "data", "data"],
            [
                ("C1", "C2"),  # intra ctrl
                ("S1", "S2"), ("S2", "S3"),  # intra svc
                ("D1", "D2"), ("D2", "D3"),  # intra data
                ("C1", "S1"), ("C1", "S2"), ("C2", "S3"),  # ctrl->svc
                ("S1", "D1"), ("S2", "D2"), ("S3", "D3"),  # svc->data
            ],
            module_order=["ctrl", "svc", "data"],
        )

    def _rung4_diamond_cross(self) -> DependencyGraph:
        """10 nodes across 5 modules with diamond patterns and cross-coupling."""
        return _make_graph(
            ["A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "E1", "E2"],
            ["ma", "ma", "mb", "mb", "mc", "mc", "md", "md", "me", "me"],
            [
                ("A1", "A2"), ("B1", "B2"), ("C1", "C2"),  # intra
                ("D1", "D2"), ("E1", "E2"),  # intra
                # Diamonds across modules
                ("A1", "B1"), ("A1", "C1"), ("B1", "D1"), ("C1", "D1"),
                ("A2", "B2"), ("A2", "C2"), ("B2", "D2"), ("C2", "D2"),
                # Extra cross-coupling
                ("D1", "E1"), ("D2", "E2"), ("B1", "E1"),
            ],
            module_order=["ma", "mb", "mc", "md", "me"],
        )

    def _rung5_hub_backlinks(self) -> DependencyGraph:
        """10 nodes across 5 modules, hub-dominated with back-edges."""
        return _make_graph(
            ["Hub", "A1", "A2", "B1", "B2", "C1", "C2", "D1", "D2", "D3"],
            ["core", "sa", "sa", "sb", "sb", "sc", "sc", "sd", "sd", "sd"],
            [
                ("A1", "A2"), ("B1", "B2"), ("C1", "C2"),  # intra
                ("D1", "D2"), ("D2", "D3"),  # intra
                # Hub connections (cross-module)
                ("Hub", "A1"), ("Hub", "B1"), ("Hub", "C1"), ("Hub", "D1"),
                ("A1", "Hub"), ("B1", "Hub"), ("C1", "Hub"),
                # Additional cross-module
                ("A1", "B1"), ("B1", "C1"), ("C1", "D1"),
                ("A2", "B2"), ("B2", "C2"), ("C2", "D2"),
                ("A1", "D1"), ("B2", "D3"),
            ],
            module_order=["core", "sa", "sb", "sc", "sd"],
        )

    def _rung6_dense_mesh(self) -> DependencyGraph:
        """10 nodes across 4 modules with heavy cross-module coupling."""
        names = ["X1", "X2", "X3", "Y1", "Y2", "Y3", "Z1", "Z2", "W1", "W2"]
        mods = ["mx", "mx", "mx", "my", "my", "my", "mz", "mz", "mw", "mw"]
        # Dense cross-module edges
        edges = [
            # intra
            ("X1", "X2"), ("X2", "X3"), ("Y1", "Y2"), ("Y2", "Y3"),
            ("Z1", "Z2"), ("W1", "W2"),
            # cross - nearly every module to every other
            ("X1", "Y1"), ("X1", "Z1"), ("X1", "W1"),
            ("X2", "Y2"), ("X2", "Z2"), ("X2", "W2"),
            ("X3", "Y3"), ("X3", "Z1"),
            ("Y1", "X1"), ("Y1", "Z1"), ("Y1", "W1"),
            ("Y2", "X2"), ("Y2", "Z2"),
            ("Y3", "X3"), ("Y3", "W2"),
            ("Z1", "X1"), ("Z1", "Y1"), ("Z1", "W1"),
            ("Z2", "X2"), ("Z2", "Y2"), ("Z2", "W2"),
            ("W1", "X1"), ("W1", "Y1"), ("W1", "Z1"),
            ("W2", "X2"), ("W2", "Y2"), ("W2", "Z2"),
        ]
        return _make_graph(names, mods, edges,
                           module_order=["mx", "my", "mz", "mw"])

    def test_complexity_ladder(self):
        """CCI must strictly increase across the ladder rungs."""
        ladder = [
            self._rung1_linear_chain(),
            self._rung2_clean_tree(),
            self._rung3_layered_dag(),
            self._rung4_diamond_cross(),
            self._rung5_hub_backlinks(),
            self._rung6_dense_mesh(),
        ]
        ccis = [run_analysis(g).metrics.cci for g in ladder]
        for i in range(len(ccis) - 1):
            self.assertLess(
                ccis[i], ccis[i + 1],
                f"Rung {i + 1} (CCI={ccis[i]:.4f}) should be less complex "
                f"than rung {i + 2} (CCI={ccis[i + 1]:.4f})"
            )


# ─── Perturbation Tests ──────────────────────────────────────────────────────

class TestPerturbation(unittest.TestCase):
    """Test that CCI responds correctly to architectural changes on the real graph."""

    def _load_real_graph(self) -> DependencyGraph:
        dot_path = os.path.join(os.path.dirname(__file__), "..", "..", "deps.dot")
        if not os.path.exists(dot_path):
            self.skipTest("deps.dot not found")
        with open(dot_path) as f:
            return parse_dot(f.read())

    def test_remove_most_coupled_module(self):
        """Removing the runtime module should decrease CCI."""
        g = self._load_real_graph()
        original_cci = run_analysis(g).metrics.cci

        # Remove runtime nodes and their edges
        g2 = DependencyGraph()
        g2.modules = [m for m in g.modules if m != "runtime"]
        for node in g.nodes:
            if node.module != "runtime":
                g2.nodes.append(node)
                g2.node_to_module[node.name] = node.module
        runtime_nodes = {n.name for n in g.nodes if n.module == "runtime"}
        for edge in g.edges:
            if edge.source not in runtime_nodes and edge.target not in runtime_nodes:
                src_mod = g2.node_to_module.get(edge.source, "")
                tgt_mod = g2.node_to_module.get(edge.target, "")
                g2.edges.append(Edge(
                    source=edge.source, target=edge.target, label=edge.label,
                    edge_type=edge.edge_type,
                    cross_module=src_mod != tgt_mod,
                ))

        reduced_cci = run_analysis(g2).metrics.cci
        self.assertLess(reduced_cci, original_cci,
                        f"Removing runtime should decrease CCI: "
                        f"{reduced_cci:.4f} vs {original_cci:.4f}")

    def test_add_random_cross_edges(self):
        """Adding 10 random cross-module edges should increase CCI."""
        g = self._load_real_graph()
        original_cci = run_analysis(g).metrics.cci

        g2 = copy.deepcopy(g)
        random.seed(42)
        node_names = [n.name for n in g2.nodes]
        added = 0
        attempts = 0
        while added < 10 and attempts < 100:
            src, tgt = random.sample(node_names, 2)
            src_mod = g2.node_to_module[src]
            tgt_mod = g2.node_to_module[tgt]
            if src_mod != tgt_mod:
                g2.edges.append(Edge(
                    source=src, target=tgt, label="added",
                    edge_type="field", cross_module=True,
                ))
                added += 1
            attempts += 1

        augmented_cci = run_analysis(g2).metrics.cci
        self.assertGreater(augmented_cci, original_cci,
                           f"Adding cross-module edges should increase CCI: "
                           f"{augmented_cci:.4f} vs {original_cci:.4f}")

    def test_merge_modules_decreases_cci(self):
        """Merging two small modules into one should decrease CCI.

        Merging error + config into a single module reduces cross-module
        edges (their mutual and outward coupling consolidates), lowering CCI.
        """
        g = self._load_real_graph()
        original_cci = run_analysis(g).metrics.cci

        # Merge error and config into "error_config"
        merge_set = {"error", "config"}
        merged_name = "error_config"

        g2 = DependencyGraph()
        g2.modules = [merged_name if m in merge_set else m
                       for m in g.modules if m not in merge_set]
        if merged_name not in g2.modules:
            g2.modules.insert(0, merged_name)
        # Deduplicate
        seen = set()
        g2.modules = [m for m in g2.modules if not (m in seen or seen.add(m))]

        for node in g.nodes:
            new_mod = merged_name if node.module in merge_set else node.module
            g2.nodes.append(Node(name=node.name, module=new_mod))
            g2.node_to_module[node.name] = new_mod

        for edge in g.edges:
            src_mod = g2.node_to_module.get(edge.source, "")
            tgt_mod = g2.node_to_module.get(edge.target, "")
            g2.edges.append(Edge(
                source=edge.source, target=edge.target, label=edge.label,
                edge_type=edge.edge_type,
                cross_module=src_mod != tgt_mod,
            ))

        merged_cci = run_analysis(g2).metrics.cci
        self.assertLess(merged_cci, original_cci,
                        f"Merging error+config should decrease CCI: "
                        f"{merged_cci:.4f} vs {original_cci:.4f}")


# ─── Integration Tests ────────────────────────────────────────────────────────

class TestIntegration(unittest.TestCase):
    def test_full_pipeline_real_graph(self):
        """Run full pipeline on real deps.dot and sanity-check outputs."""
        dot_path = os.path.join(os.path.dirname(__file__), "..", "..", "deps.dot")
        if not os.path.exists(dot_path):
            self.skipTest("deps.dot not found")
        with open(dot_path) as f:
            graph = parse_dot(f.read())

        result = run_analysis(graph)

        # Basic sanity checks
        self.assertEqual(result.metrics.n_nodes, 36)
        self.assertEqual(result.metrics.n_edges, 89)
        self.assertEqual(result.metrics.n_modules, 8)

        # Connected graph -> lambda_2 > 0
        self.assertGreater(result.spectral.fiedler_value, 0,
                           "Connected graph should have lambda_2 > 0")

        # CCI should be in a reasonable range for a well-structured codebase
        self.assertGreater(result.metrics.cci, 0.05)
        self.assertLess(result.metrics.cci, 0.9)

        # Eigenvalues should be non-negative (Laplacian property)
        self.assertTrue(np.all(result.spectral.eigenvalues >= -1e-10),
                        "Laplacian eigenvalues should be non-negative")

        # First eigenvalue should be 0
        self.assertAlmostEqual(result.spectral.eigenvalues[0], 0.0, places=8)

    def test_report_generation(self):
        """Verify report contains expected sections."""
        dot_path = os.path.join(os.path.dirname(__file__), "..", "..", "deps.dot")
        if not os.path.exists(dot_path):
            self.skipTest("deps.dot not found")
        with open(dot_path) as f:
            graph = parse_dot(f.read())
        result = run_analysis(graph)
        report = generate_report(result)

        self.assertIn("GRAPH SUMMARY", report)
        self.assertIn("LAPLACIAN EIGENVALUE SPECTRUM", report)
        self.assertIn("FIEDLER VECTOR", report)
        self.assertIn("MODULE COUPLING MATRIX", report)
        self.assertIn("CONNECTOME COMPLEXITY INDEX", report)

    def test_json_output(self):
        """Verify JSON output is well-formed and contains expected keys."""
        g = _make_graph(
            ["A", "B", "C"], ["m1", "m1", "m2"],
            [("A", "B"), ("A", "C")],
            module_order=["m1", "m2"],
        )
        result = run_analysis(g)
        d = metrics_to_dict(result)

        self.assertIn("graph", d)
        self.assertIn("structural", d)
        self.assertIn("module_coupling", d)
        self.assertIn("module_cohesion", d)
        self.assertIn("metrics", d)
        self.assertEqual(d["graph"]["n_nodes"], 3)
        self.assertIsInstance(d["structural"]["dag_depth"], int)
        self.assertIsInstance(d["metrics"]["cci"], float)

        # Should be JSON-serializable
        json_str = json.dumps(d)
        self.assertIsInstance(json_str, str)

    def test_dashboard_generation(self):
        """Verify dashboard PNG can be generated without errors."""
        try:
            import matplotlib
        except ImportError:
            self.skipTest("matplotlib not available")

        g = _make_graph(
            ["A", "B", "C", "D"], ["m1", "m1", "m2", "m2"],
            [("A", "B"), ("A", "C"), ("C", "D")],
            module_order=["m1", "m2"],
        )
        result = run_analysis(g)

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            path = f.name
        try:
            from spectral_analysis import generate_dashboard
            generate_dashboard(result, path)
            self.assertTrue(os.path.exists(path))
            self.assertGreater(os.path.getsize(path), 1000,
                               "Dashboard should be a non-trivial PNG")
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
