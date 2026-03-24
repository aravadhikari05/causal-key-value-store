import copy
import time
from typing import Dict, List, Optional, Tuple
from kvstore.value import Value
from kvstore import clock

TOMBSTONE = "__TOMBSTONE__"


class Node:
    def __init__(self, node_id: int, shard_id: int):
        self.node_id = node_id
        self.shard_id = shard_id
        self.kv_store: Dict[str, Value] = {}
        self.local_clock: Dict[str, Dict[int, int]] = {}
        self.alive = True

    def put(self, key: str, value: str, client_clock: Dict[str, Dict[int, int]]) -> Dict:
        if not self.alive:
            return {"error": "Node is dead"}

        clock_before = copy.deepcopy(self.local_clock.get(key, {}))

        if key not in client_clock:
            client_clock[key] = {}
        if key not in self.local_clock:
            self.local_clock[key] = {}

        # merge IF client clock ahead
        if clock.vc_happens_before(self.local_clock[key], client_clock[key]):
            self.local_clock[key] = clock.vc_merge(self.local_clock[key], client_clock[key])

        # increment node entry
        clock.vc_increment(self.local_clock[key], self.node_id)

        
        if key not in self.kv_store:
            self.kv_store[key] = Value(value)
        else:
            self.kv_store[key].set_value(value)

        self.kv_store[key].set_time(time.time())

        #causal dependencies
        for k in client_clock:
            if k != key:
                self.kv_store[key].add_dep(k, client_clock[k])

        # build metadata 
        new_meta = client_clock.copy()
        new_meta[key] = copy.deepcopy(self.local_clock[key])
        for dep in self.kv_store[key].get_deps():
            new_meta[dep] = self.kv_store[key].get_clock_at(dep)

        return {
            "metadata": new_meta,
            "clock_before": clock_before,
            "clock_after": copy.deepcopy(self.local_clock[key]),
        }

    def get(self, key: str, client_clock: Dict[str, Dict[int, int]]) -> Dict:
        if not self.alive:
            return {"error": "Node is dead"}

        if key not in client_clock:
            client_clock[key] = {}
        if key not in self.local_clock:
            self.local_clock[key] = {}


        ready = (
            clock.vc_happens_before(client_clock[key], self.local_clock[key])
            or clock.vc_is_equal(client_clock[key], self.local_clock[key])
        )

        if not ready:
            return {
                "error": "not_ready",
                "detail": "Causal dependencies not yet satisfied",
                "client_clock": copy.deepcopy(client_clock[key]),
                "node_clock": copy.deepcopy(self.local_clock[key]),
            }

        if key not in self.kv_store:
            return {"error": "not_found", "detail": f'Key "{key}" does not exist'}

        if self.kv_store[key].get_value() == TOMBSTONE:
            return {"error": "deleted", "detail": f'Key "{key}" has been deleted'}

        #add causal dependencies
        for k in client_clock:
            if k != key:
                self.kv_store[key].add_dep(k, client_clock[k])

        new_meta = {key: copy.deepcopy(self.local_clock[key])}
        for dep in self.kv_store[key].get_deps():
            new_meta[dep] = self.kv_store[key].get_clock_at(dep)

        return {
            "value": self.kv_store[key].get_value(),
            "metadata": new_meta,
        }

    def receive_local_gossip(self, other: "Node") -> List[str]:
        if not self.alive or not other.alive:
            return []

        log = []

        for key in other.kv_store:
            if key in self.kv_store:
                my_ck = self.local_clock.get(key, {})
                their_ck = other.local_clock.get(key, {})

                if clock.vc_is_equal(my_ck, their_ck):
                    pass
                elif clock.vc_happens_before(my_ck, their_ck): #if their version newer
                    self.kv_store[key].set_value(other.kv_store[key].get_value())
                    self.kv_store[key].set_deps(copy.deepcopy(other.kv_store[key].get_deps()))
                    self.kv_store[key].set_time(other.kv_store[key].get_time())
                    self.local_clock[key] = clock.vc_merge(my_ck, their_ck)
                    log.append(f"  Node-{self.node_id}: updated key \"{key}\" from Node-{other.node_id} (their clock was ahead)")

                elif clock.vc_is_concurrent(my_ck, their_ck):
                    # resolve by timestamp, then node id
                    t1 = self.kv_store[key].get_time()
                    t2 = other.kv_store[key].get_time()
                    if (t1 < t2) or (t1 == t2 and other.node_id > self.node_id):
                        self.kv_store[key].set_value(other.kv_store[key].get_value())
                        self.kv_store[key].set_deps(copy.deepcopy(other.kv_store[key].get_deps()))
                        self.kv_store[key].set_time(other.kv_store[key].get_time())
                        self.local_clock[key] = clock.vc_merge(my_ck, their_ck)
                        log.append(f"  Node-{self.node_id}: resolved concurrent write for \"{key}\" — took Node-{other.node_id}'s value")
                else:
                    # merge if we are ahead or equal
                    self.local_clock[key] = clock.vc_merge(my_ck, their_ck)
            else:
                v = other.kv_store[key]
                self.kv_store[key] = Value(v.get_value(), v.get_time())
                self.kv_store[key].set_deps(copy.deepcopy(v.get_deps()))
                self.local_clock[key] = copy.deepcopy(other.local_clock.get(key, {}))
                log.append(f"  Node-{self.node_id}: received new key \"{key}\" from Node-{other.node_id}")

        return log

    def receive_global_gossip(self, other_clock: Dict[str, Dict[int, int]]) -> List[str]:
        if not self.alive:
            return []

        log = []
        for key in other_clock:
            if key not in self.local_clock:
                self.local_clock[key] = {}
            old = copy.deepcopy(self.local_clock[key])
            self.local_clock[key] = clock.vc_merge(self.local_clock[key], other_clock[key])
            if old != self.local_clock[key]:
                log.append(f"  Node-{self.node_id}: merged global clock for \"{key}\"")
        return log

    def export_state(self) -> Tuple[Dict[str, Value], Dict[str, Dict[int, int]]]:
        return copy.deepcopy(self.kv_store), copy.deepcopy(self.local_clock)

    def import_key(self, key: str, value: Value, key_clock: Dict[int, int]):
        self.kv_store[key] = value
        self.local_clock[key] = key_clock

    def key_count(self) -> int:
        return len([k for k, v in self.kv_store.items() if v.get_value() != TOMBSTONE])

    def __repr__(self):
        status = "ALIVE" if self.alive else "DEAD"
        return f"Node-{self.node_id} (Shard {self.shard_id}, {status}, {self.key_count()} keys)"
