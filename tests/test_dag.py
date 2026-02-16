import pytest
from pipeline.dag_executor import topo_sort

def test_topo_sort_linear():
    """Test a simple linear dependency: A -> B -> C"""
    dag = {
        "A": {"depends_on": []},
        "B": {"depends_on": ["A"]},
        "C": {"depends_on": ["B"]},
    }
    order = topo_sort(dag)
    assert order == ["A", "B", "C"]

def test_topo_sort_branching():
    """Test branching: A -> B, A -> C, (B,C) -> D"""
    dag = {
        "A": {"depends_on": []},
        "B": {"depends_on": ["A"]},
        "C": {"depends_on": ["A"]},
        "D": {"depends_on": ["B", "C"]},
    }
    order = topo_sort(dag)
    # A must be first, D must be last. B and C can be in any order in between.
    assert order[0] == "A"
    assert order[-1] == "D"
    assert set(order[1:3]) == {"B", "C"}

def test_topo_sort_cycle():
    """Test that cycles raise a RecursionError"""
    dag = {
        "A": {"depends_on": ["B"]},
        "B": {"depends_on": ["A"]},
    }
    with pytest.raises(RecursionError):
        topo_sort(dag)