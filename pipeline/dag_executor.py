def topo_sort(dag: dict[str, dict]) -> list[str]:
    visited = set()
    order = []

    def visit(target_node): # Changed name here
        if target_node in visited:
            return
        for dep in dag[target_node]["depends_on"]:
            visit(dep)
        visited.add(target_node)
        order.append(target_node)

    for node in dag:
        visit(node)

    return order

