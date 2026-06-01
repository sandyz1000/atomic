"""Tests for atomic_compute.Graph (P4.3 — Python graph bindings)."""
import pytest
import atomic_compute


# ── Construction ──────────────────────────────────────────────────────────────

def test_empty_graph():
    g = atomic_compute.Graph([], [])
    assert g.num_vertices() == 0
    assert g.num_edges() == 0


def test_basic_construction():
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0)],
    )
    assert g.num_vertices() == 3
    assert g.num_edges() == 2


def test_vertices_roundtrip():
    verts = [(1, 0.5), (2, 0.3)]
    g = atomic_compute.Graph(verts, [])
    result = {v[0]: v[1] for v in g.vertices()}
    assert abs(result[1] - 0.5) < 1e-9
    assert abs(result[2] - 0.3) < 1e-9


def test_edges_roundtrip():
    edges = [(1, 2, 0.8), (2, 3, 0.5)]
    g = atomic_compute.Graph([(1, 1.0), (2, 1.0), (3, 1.0)], edges)
    result = g.edges()
    assert len(result) == 2


def test_graph_repr():
    g = atomic_compute.Graph([(1, 1.0)], [])
    r = repr(g)
    assert "Graph" in r
    assert "1" in r


def test_edge_to_unknown_vertex_dropped():
    """Edges referencing unknown vertex IDs are silently dropped."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0)],
        [(1, 2, 1.0), (1, 99, 1.0)],  # 99 not in vertex list
    )
    assert g.num_edges() == 1


# ── PageRank ──────────────────────────────────────────────────────────────────

def test_page_rank_cycle():
    """3-node cycle: all ranks should be approximately equal."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0)],
    )
    ranks = g.page_rank(num_iter=50, reset_prob=0.15)
    assert set(ranks.keys()) == {1, 2, 3}
    vals = list(ranks.values())
    assert max(vals) - min(vals) < 0.01


def test_page_rank_sink():
    """Vertex 3 has no outgoing edges — should accumulate more rank."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0)],
    )
    ranks = g.page_rank(num_iter=20, reset_prob=0.15)
    assert ranks[3] > ranks[1]


def test_page_rank_returns_all_vertices():
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0)],
    )
    ranks = g.page_rank()
    assert len(ranks) == 3


# ── Connected components ──────────────────────────────────────────────────────

def test_connected_components_single():
    """All vertices reachable from each other → one component."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0)],
    )
    cc = g.connected_components()
    assert len(set(cc.values())) == 1


def test_connected_components_two_clusters():
    """Two separate edges form two distinct components."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)],
        [(1, 2, 1.0), (3, 4, 1.0)],
    )
    cc = g.connected_components()
    components = set(cc.values())
    assert len(components) == 2


def test_connected_components_isolated_vertex():
    """An isolated vertex forms its own component."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (99, 1.0)],
        [(1, 2, 1.0)],
    )
    cc = g.connected_components()
    assert cc[99] != cc[1]


# ── Triangle count ────────────────────────────────────────────────────────────

def test_triangle_count_basic():
    """Full bidirectional triangle: each vertex participates in ≥1 triangle."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [
            (1, 2, 1.0), (2, 1, 1.0),
            (2, 3, 1.0), (3, 2, 1.0),
            (3, 1, 1.0), (1, 3, 1.0),
        ],
    )
    tc = g.triangle_count()
    assert all(v > 0 for v in tc.values())


def test_triangle_count_no_triangles():
    """Path graph has no triangles."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0)],
    )
    tc = g.triangle_count()
    assert all(v == 0 for v in tc.values())


# ── Shortest path ─────────────────────────────────────────────────────────────

def test_shortest_path_triangle():
    """1→2→3 is shorter than 1→3 direct when weights differ."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 2.0), (1, 3, 5.0)],
    )
    sp = g.shortest_path([3])
    # 1→2 costs 1.0; 2→3 costs 2.0 → total 3.0 < direct 5.0
    assert sp[1][3] == pytest.approx(3.0)
    assert sp[2][3] == pytest.approx(2.0)
    assert sp[3][3] == pytest.approx(0.0)


def test_shortest_path_self_distance_zero():
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0)],
        [(1, 2, 1.0)],
    )
    sp = g.shortest_path([1])
    assert sp[1][1] == pytest.approx(0.0)


def test_shortest_path_disconnected():
    """Unreachable vertex should not appear in landmark's distance map."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0)],  # vertex 3 unreachable from 1
    )
    sp = g.shortest_path([1])
    assert 1 in sp[1]
    assert 2 in sp[1]
    # vertex 3 cannot reach landmark 1
    assert 1 not in sp.get(3, {})


# ── Label propagation ─────────────────────────────────────────────────────────

def test_label_propagation_two_clusters():
    """Two tightly connected cliques should receive distinct labels."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)],
        [(1, 2, 1.0), (2, 1, 1.0), (3, 4, 1.0), (4, 3, 1.0)],
    )
    labels = g.label_propagation(max_iter=20)
    assert labels[1] == labels[2]
    assert labels[3] == labels[4]


def test_label_propagation_all_vertices_labeled():
    """Every vertex must receive a label."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0)],
    )
    labels = g.label_propagation(max_iter=5)
    assert len(labels) == 3


# ── Strongly connected components ────────────────────────────────────────────

def test_strongly_connected_cycle():
    """Directed cycle: all vertices in one SCC."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0)],
    )
    scc = g.strongly_connected_components()
    assert len(set(scc.values())) == 1


def test_strongly_connected_dag():
    """DAG with no back-edges: each vertex is its own SCC."""
    g = atomic_compute.Graph(
        [(1, 1.0), (2, 1.0), (3, 1.0)],
        [(1, 2, 1.0), (2, 3, 1.0)],
    )
    scc = g.strongly_connected_components()
    # All labels should be distinct in a pure DAG.
    assert len(set(scc.values())) == 3
