"""DAG execution utilities."""

from collections.abc import Mapping
from typing import Any


def topo_sort(dag: Mapping[str, dict[str, Any]]) -> list[str]:
    """Return a topologically sorted list of stage names.

    Performs a depth-first search over the dependency graph defined by
    ``dag[stage][\"depends_on\"]`` and produces an execution order where every
    stage appears after all of its dependencies.

    Args:
        dag: Mapping from stage name to its metadata, including a
            ``\"depends_on\"`` list of prerequisite stages.

    Returns:
        List of stage names in a valid execution order.

    Raises:
        KeyError: If a dependency name is missing from the DAG.
        RecursionError: If there is a cycle in the DAG (not allowed).
    """
    visited: set[str] = set()
    order: list[str] = []

    def visit(target_node: str) -> None:
        if target_node in visited:
            return
        for dep in dag[target_node]["depends_on"]:
            visit(dep)
        visited.add(target_node)
        order.append(target_node)

    for node in dag:
        visit(node)

    return order

