import copy
import hashlib
from typing import Dict, List, Optional, Tuple
from kvstore.node import Node, TOMBSTONE
from kvstore.shard import Shard
from kvstore.value import Value


class Cluster:
    def __init__(self):
        self.shards: Dict[int, Shard] = {}
        self.total_shards = 0
        self.next_node_id = 0

    def initialize(self, num_shards: int, nodes_per_shard: int) -> List[str]:
        self.shards.clear()
        self.total_shards = num_shards
        self.next_node_id = 0

        log = []
        for s in range(num_shards):
            shard = Shard(s)
            node_ids = []
            for _ in range(nodes_per_shard):
                node = Node(self.next_node_id, s)
                shard.add_node(node)
                node_ids.append(f"Node-{node.node_id}")
                self.next_node_id += 1
            self.shards[s] = shard
            log.append(f"  Shard {s}: [{', '.join(node_ids)}]")

        return log

    def hash_key(self, key: str) -> int:
        hashed_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hashed_value % self.total_shards

    def put(self, key: str, value: str, client_clock: Dict) -> Dict:
        shard_id = self.hash_key(key)
        shard = self.shards.get(shard_id)

        if shard is None or not shard.is_alive():
            return {"error": f"Shard {shard_id} is unavailable (all nodes dead)"}

        result, node = shard.route_put(key, value, client_clock)
        if "error" in result:
            return result

        result["shard_id"] = shard_id
        result["node_id"] = node.node_id
        return result

    def get(self, key: str, client_clock: Dict) -> Dict:
        shard_id = self.hash_key(key)
        shard = self.shards.get(shard_id)

        if shard is None or not shard.is_alive():
            return {"error": f"Shard {shard_id} is unavailable (all nodes dead)"}

        result, node = shard.route_get(key, client_clock)
        if node:
            result["shard_id"] = shard_id
            result["node_id"] = node.node_id
        return result

    def delete(self, key: str, client_clock: Dict) -> Dict:
        return self.put(key, TOMBSTONE, client_clock)

    def kill_node(self, node_id: int) -> Dict:
        node = self._find_node(node_id)
        if node is None:
            return {"error": f"Node-{node_id} does not exist"}
        if not node.alive:
            return {"error": f"Node-{node_id} is already dead"}

        node.alive = False
        shard = self.shards[node.shard_id]
        alive = shard.get_alive_nodes()
        remaining = [f"Node-{n.node_id}" for n in alive]

        result = {
            "node_id": node_id,
            "shard_id": node.shard_id,
            "keys_on_node": node.key_count(),
        }

        if alive:
            result["status"] = f"Shard {node.shard_id} still has {len(alive)} alive node(s): {', '.join(remaining)}"

            replicated = self._check_replication(node, alive)
            result["replication"] = replicated
        else:
            result["status"] = f"Shard {node.shard_id} has NO alive nodes — keys on this shard are UNAVAILABLE"

        return result

    def kill_shard(self, shard_id: int) -> Dict:
        shard = self.shards.get(shard_id)
        if shard is None:
            return {"error": f"Shard {shard_id} does not exist"}

        killed = []
        for node in shard.nodes.values():
            if node.alive:
                node.alive = False
                killed.append(f"Node-{node.node_id}")

        if not killed:
            return {"error": f"All nodes in Shard {shard_id} are already dead"}

        # count affected keys
        any_node = list(shard.nodes.values())[0]
        key_count = any_node.key_count()

        return {
            "shard_id": shard_id,
            "killed_nodes": killed,
            "keys_affected": key_count,
            "status": f"All nodes killed. {key_count} key(s) on Shard {shard_id} are now UNAVAILABLE",
        }

    def revive_node(self, node_id: int) -> Dict:
        node = self._find_node(node_id)
        if node is None:
            return {"error": f"Node-{node_id} does not exist"}
        if node.alive:
            return {"error": f"Node-{node_id} is already alive"}

        node.alive = True
        # gossip to catch up node
        gossip_log = self._run_full_gossip()
        return {
            "node_id": node_id,
            "shard_id": node.shard_id,
            "status": f"Node-{node_id} is back online in Shard {node.shard_id}",
            "gossip_log": gossip_log,
        }

    def add_node(self, shard_id: int) -> Dict:
        shard = self.shards.get(shard_id)
        if shard is None:
            return {"error": f"Shard {shard_id} does not exist"}

        node = Node(self.next_node_id, shard_id)
        shard.add_node(node)
        self.next_node_id += 1

        # intentional gossip to bring new node up to speed
        gossip_log = self._run_full_gossip()

        return {
            "node_id": node.node_id,
            "shard_id": shard_id,
            "status": f"Node-{node.node_id} added to Shard {shard_id}",
            "gossip_log": gossip_log,
        }

    def add_shard(self) -> Dict:
        new_shard_id = self.total_shards
        new_shard = Shard(new_shard_id)

        node = Node(self.next_node_id, new_shard_id)
        new_shard.add_node(node)
        self.next_node_id += 1

        self.shards[new_shard_id] = new_shard
        old_total = self.total_shards
        self.total_shards += 1

        migration_log = self._reshard(old_total)

        # Gossip after resharding
        gossip_log = self._run_full_gossip()

        return {
            "shard_id": new_shard_id,
            "node_id": node.node_id,
            "old_shards": old_total,
            "new_shards": self.total_shards,
            "migration_log": migration_log,
            "gossip_log": gossip_log,
        }

    def remove_shard(self, shard_id: int) -> Dict:
        if shard_id not in self.shards:
            return {"error": f"Shard {shard_id} does not exist"}
        if self.total_shards <= 1:
            return {"error": "Cannot remove the last shard"}

        # collect all keys from shard thats being removed
        removed_shard = self.shards[shard_id]
        keys_to_migrate = {}
        clocks_to_migrate = {}
        for node in removed_shard.nodes.values():
            for key, val in node.kv_store.items():
                if key not in keys_to_migrate:
                    keys_to_migrate[key] = copy.deepcopy(val)
                    clocks_to_migrate[key] = copy.deepcopy(node.local_clock.get(key, {}))


        del self.shards[shard_id]

        # reassign shard Ids
        old_shards = dict(self.shards)
        self.shards.clear()
        shard_id_map = {}
        for new_id, (old_id, shard) in enumerate(sorted(old_shards.items())):
            shard.shard_id = new_id
            for node in shard.nodes.values():
                node.shard_id = new_id
            self.shards[new_id] = shard
            shard_id_map[old_id] = new_id

        self.total_shards = len(self.shards)

        # move keys to new nodes
        migration_log = []
        for key, val in keys_to_migrate.items():
            new_shard_id = self.hash_key(key)
            target_shard = self.shards[new_shard_id]
            target_node = target_shard.get_any_alive_node()
            if target_node:
                target_node.import_key(key, copy.deepcopy(val), copy.deepcopy(clocks_to_migrate[key]))
                migration_log.append(f"  Key \"{key}\" -> Shard {new_shard_id} (Node-{target_node.node_id})")

        # Reshard existing keys
        reshard_log = self._reshard_existing()

        gossip_log = self._run_full_gossip()

        return {
            "removed_shard": shard_id,
            "new_total": self.total_shards,
            "migration_log": migration_log + reshard_log,
            "gossip_log": gossip_log,
        }

    def reshard_to(self, new_count: int) -> Dict:
        if new_count < 1:
            return {"error": "Must have at least 1 shard"}
        if new_count == self.total_shards:
            return {"error": f"Already at {new_count} shard(s)"}

        old_total = self.total_shards
        shards_added = []
        shards_removed = []
        collected_keys = {}
        collected_clocks = {}

        if new_count > old_total:
            # add empty shards
            for s in range(old_total, new_count):
                shard = Shard(s)
                node = Node(self.next_node_id, s)
                shard.add_node(node)
                self.next_node_id += 1
                self.shards[s] = shard
                shards_added.append({"shard_id": s, "node_id": node.node_id})
        else:
            # delete keys
            for s in range(new_count, old_total):
                shard = self.shards[s]
                shards_removed.append(s)
                for node in shard.nodes.values():
                    for key, val in node.kv_store.items():
                        if key not in collected_keys:
                            collected_keys[key] = copy.deepcopy(val)
                            collected_clocks[key] = copy.deepcopy(node.local_clock.get(key, {}))
                del self.shards[s]

        self.total_shards = new_count

        migration_log = self._reshard(old_total)

        # add keys from removed shards into their  new nodes
        for key, val in collected_keys.items():
            new_shard_id = self.hash_key(key)
            target_shard = self.shards[new_shard_id]
            target_node = target_shard.get_any_alive_node()
            if target_node:
                target_node.import_key(key, val, collected_clocks[key])
                migration_log.append(f"  Key \"{key}\": removed shard -> Shard {new_shard_id} (Node-{target_node.node_id})")

        gossip_log = self._run_full_gossip()

        return {
            "old_shards": old_total,
            "new_shards": new_count,
            "shards_added": shards_added,
            "shards_removed": shards_removed,
            "migration_log": migration_log,
            "gossip_log": gossip_log,
        }

    def run_gossip(self) -> List[str]:
        # for manually trigger gossip
        return self._run_full_gossip()

    def get_all_nodes(self) -> List[Node]:
        nodes = []
        for shard in self.shards.values():
            nodes.extend(shard.nodes.values())
        return nodes

    def get_all_keys(self) -> Dict[str, List[int]]:
        keys = {}
        for shard in self.shards.values():
            for node in shard.nodes.values():
                for key in node.kv_store:
                    if node.kv_store[key].get_value() != TOMBSTONE:
                        if key not in keys:
                            keys[key] = shard.shard_id
        return keys

    # --- HELPERS ----

    def _find_node(self, node_id: int) -> Optional[Node]:
        for shard in self.shards.values():
            if node_id in shard.nodes:
                return shard.nodes[node_id]
        return None

    def _run_full_gossip(self) -> List[str]:
        log = []

        # local gossip
        for shard in self.shards.values():
            entries = shard.run_local_gossip()
            if entries:
                log.extend(entries)

        # global gossip
        shard_list = list(self.shards.values())
        for i, shard_a in enumerate(shard_list):
            node_a = shard_a.get_any_alive_node()
            if node_a is None:
                continue
            for shard_b in shard_list[i + 1:]:
                node_b = shard_b.get_any_alive_node()
                if node_b is None:
                    continue
                entries = node_a.receive_global_gossip(copy.deepcopy(node_b.local_clock))
                log.extend(entries)
                entries = node_b.receive_global_gossip(copy.deepcopy(node_a.local_clock))
                log.extend(entries)

        return log

    def _check_replication(self, dead_node: Node, alive_nodes: List[Node]) -> str:
        missing = []
        for key in dead_node.kv_store:
            if dead_node.kv_store[key].get_value() == TOMBSTONE:
                continue
            found = any(key in n.kv_store for n in alive_nodes)
            if not found:
                missing.append(key)

        if missing:
            return f"WARNING: {len(missing)} key(s) NOT replicated: {', '.join(missing)}"
        return "All keys were replicated via gossip"

    def _reshard(self, old_total: int) -> List[str]:
        log = []
        for shard in self.shards.values():
            for node in shard.nodes.values():
                keys_to_remove = []
                for key in list(node.kv_store.keys()):
                    new_shard_id = self.hash_key(key)
                    if new_shard_id != shard.shard_id:
                        #migrate this key
                        target_shard = self.shards[new_shard_id]
                        target_node = target_shard.get_any_alive_node()
                        if target_node:
                            target_node.import_key(
                                key,
                                copy.deepcopy(node.kv_store[key]),
                                copy.deepcopy(node.local_clock.get(key, {})),
                            )
                            log.append(f"  Key \"{key}\": Shard {shard.shard_id} -> Shard {new_shard_id} (Node-{target_node.node_id})")
                        keys_to_remove.append(key)

                for key in keys_to_remove:
                    del node.kv_store[key]
                    node.local_clock.pop(key, None)

        return log

    def _reshard_existing(self) -> List[str]:
        return self._reshard(self.total_shards)
