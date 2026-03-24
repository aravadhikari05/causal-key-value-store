from typing import Dict, List, Optional, Tuple
from kvstore.node import Node


class Shard:
    def __init__(self, shard_id: int):
        self.shard_id = shard_id
        self.nodes: Dict[int, Node] = {}

    def add_node(self, node: Node):
        self.nodes[node.node_id] = node

    def remove_node(self, node_id: int) -> Optional[Node]:
        return self.nodes.pop(node_id, None)

    def get_alive_nodes(self) -> List[Node]:
        return [n for n in self.nodes.values() if n.alive]

    def get_any_alive_node(self) -> Optional[Node]:
        alive = self.get_alive_nodes()
        return alive[0] if alive else None

    def run_local_gossip(self) -> List[str]:
        alive = self.get_alive_nodes()
        if len(alive) < 2:
            return []

        log = []
        for i, node_a in enumerate(alive):
            for node_b in alive[i + 1:]:
                entries = node_a.receive_local_gossip(node_b)
                log.extend(entries)
                entries = node_b.receive_local_gossip(node_a)
                log.extend(entries)

        return log

    def route_put(self, key: str, value: str, client_clock: dict) -> Tuple[Dict, Node]:
        node = self.get_any_alive_node()
        if node is None:
            return {"error": f"Shard {self.shard_id} has no alive nodes"}, None
        result = node.put(key, value, client_clock)
        return result, node

    def route_get(self, key: str, client_clock: dict) -> Tuple[Dict, Node]:
        node = self.get_any_alive_node()
        if node is None:
            return {"error": f"Shard {self.shard_id} has no alive nodes"}, None
        result = node.get(key, client_clock)
        return result, node

    def is_alive(self) -> bool:
        return len(self.get_alive_nodes()) > 0

    def __repr__(self):
        alive = len(self.get_alive_nodes())
        total = len(self.nodes)
        return f"Shard-{self.shard_id} ({alive}/{total} nodes alive)"
