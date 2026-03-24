#Taken and adapted from testing framework provided in class


import copy
import time
from dataclasses import dataclass
from typing import List, Callable
from rich.console import Console
from rich.text import Text

from kvstore import clock
from kvstore.value import Value
from kvstore.node import Node
from kvstore.shard import Shard
from kvstore.cluster import Cluster

console = Console()


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str = ""


def run_test(name: str, fn: Callable) -> TestResult:
    """Run a single test function and return result."""
    try:
        fn()
        return TestResult(name, True)
    except AssertionError as e:
        return TestResult(name, False, str(e))
    except Exception as e:
        return TestResult(name, False, f"{type(e).__name__}: {e}")


def assert_eq(actual, expected, msg=""):
    """Assert equality."""
    if actual != expected:
        raise AssertionError(f"{msg} | expected {expected}, got {actual}")


def assert_in(needle, haystack, msg=""):
    """Assert needle in haystack."""
    if needle not in haystack:
        raise AssertionError(f"{msg} | expected {needle} in {haystack}")


def assert_true(condition, msg=""):
    """Assert condition is True."""
    if not condition:
        raise AssertionError(f"{msg} | expected True")


def assert_false(condition, msg=""):
    """Assert condition is False."""
    if condition:
        raise AssertionError(f"{msg} | expected False")


# ============================================================================
# Vector Clock Tests
# ============================================================================

def run_clock_tests() -> List[TestResult]:
    results = []

    # vc_increment tests
    def test_clock_increment_new_node():
        vc = {}
        clock.vc_increment(vc, 0)
        assert_eq(vc[0], 1)

    def test_clock_increment_existing_node():
        vc = {0: 1}
        clock.vc_increment(vc, 0)
        assert_eq(vc[0], 2)

    def test_clock_increment_multiple_nodes():
        vc = {0: 1}
        clock.vc_increment(vc, 1)
        assert_eq(vc[0], 1)
        assert_eq(vc[1], 1)

    def test_clock_increment_twice():
        vc = {}
        clock.vc_increment(vc, 0)
        clock.vc_increment(vc, 0)
        assert_eq(vc[0], 2)

    results.extend([
        run_test("clock_increment_new_node", test_clock_increment_new_node),
        run_test("clock_increment_existing_node", test_clock_increment_existing_node),
        run_test("clock_increment_multiple_nodes", test_clock_increment_multiple_nodes),
        run_test("clock_increment_twice", test_clock_increment_twice),
    ])

    # vc_happens_before tests
    def test_clock_hb_empty_empty():
        assert_false(clock.vc_happens_before({}, {}))

    def test_clock_hb_empty_nonempty():
        assert_false(clock.vc_happens_before({}, {0: 1}))

    def test_clock_hb_strictly_before():
        assert_true(clock.vc_happens_before({0: 1}, {0: 2}))

    def test_clock_hb_strictly_before_multinode():
        assert_true(clock.vc_happens_before({0: 1, 1: 1}, {0: 2, 1: 1}))

    def test_clock_hb_equal_not_before():
        assert_false(clock.vc_happens_before({0: 1}, {0: 1}))

    def test_clock_hb_concurrent():
        assert_false(clock.vc_happens_before({0: 2, 1: 1}, {0: 1, 1: 2}))

    def test_clock_hb_missing_key_in_this():
        assert_false(clock.vc_happens_before({0: 1}, {0: 1, 1: 1}))

    def test_clock_hb_missing_key_in_other():
        assert_false(clock.vc_happens_before({0: 2}, {0: 1}))

    def test_clock_hb_zero_values_stripped():
        assert_eq(
            clock.vc_happens_before({0: 1, 1: 0}, {0: 2}),
            clock.vc_happens_before({0: 1}, {0: 2})
        )

    results.extend([
        run_test("clock_hb_empty_empty", test_clock_hb_empty_empty),
        run_test("clock_hb_empty_nonempty", test_clock_hb_empty_nonempty),
        run_test("clock_hb_strictly_before", test_clock_hb_strictly_before),
        run_test("clock_hb_strictly_before_multinode", test_clock_hb_strictly_before_multinode),
        run_test("clock_hb_equal_not_before", test_clock_hb_equal_not_before),
        run_test("clock_hb_concurrent", test_clock_hb_concurrent),
        run_test("clock_hb_missing_key_in_this", test_clock_hb_missing_key_in_this),
        run_test("clock_hb_missing_key_in_other", test_clock_hb_missing_key_in_other),
        run_test("clock_hb_zero_values_stripped", test_clock_hb_zero_values_stripped),
    ])

    # vc_is_equal tests
    def test_clock_equal_same_dict():
        assert_true(clock.vc_is_equal({0: 1, 1: 2}, {0: 1, 1: 2}))

    def test_clock_equal_with_zeros():
        assert_true(clock.vc_is_equal({0: 1, 1: 0}, {0: 1}))

    def test_clock_not_equal_different_values():
        assert_false(clock.vc_is_equal({0: 1}, {0: 2}))

    def test_clock_equal_empty():
        assert_true(clock.vc_is_equal({}, {}))

    results.extend([
        run_test("clock_equal_same_dict", test_clock_equal_same_dict),
        run_test("clock_equal_with_zeros", test_clock_equal_with_zeros),
        run_test("clock_not_equal_different_values", test_clock_not_equal_different_values),
        run_test("clock_equal_empty", test_clock_equal_empty),
    ])

    # vc_is_concurrent tests
    def test_clock_concurrent_basic():
        assert_true(clock.vc_is_concurrent({0: 1}, {1: 1}))

    def test_clock_concurrent_cross_increments():
        assert_true(clock.vc_is_concurrent({0: 2, 1: 1}, {0: 1, 1: 2}))

    def test_clock_not_concurrent_ordered():
        assert_false(clock.vc_is_concurrent({0: 1}, {0: 2}))

    results.extend([
        run_test("clock_concurrent_basic", test_clock_concurrent_basic),
        run_test("clock_concurrent_cross_increments", test_clock_concurrent_cross_increments),
        run_test("clock_not_concurrent_ordered", test_clock_not_concurrent_ordered),
    ])

    # vc_merge tests
    def test_clock_merge_disjoint():
        result = clock.vc_merge({0: 1}, {1: 2})
        assert_eq(result, {0: 1, 1: 2})

    def test_clock_merge_overlapping():
        result = clock.vc_merge({0: 3, 1: 1}, {0: 1, 1: 4})
        assert_eq(result, {0: 3, 1: 4})

    def test_clock_merge_subset():
        result = clock.vc_merge({0: 1}, {0: 3, 1: 2})
        assert_eq(result, {0: 3, 1: 2})

    def test_clock_merge_empty_left():
        result = clock.vc_merge({}, {0: 1})
        assert_eq(result, {0: 1})

    def test_clock_merge_empty_right():
        result = clock.vc_merge({0: 1}, {})
        assert_eq(result, {0: 1})

    def test_clock_merge_does_not_mutate():
        left = {0: 1}
        right = {1: 2}
        clock.vc_merge(left, right)
        assert_eq(left, {0: 1})
        assert_eq(right, {1: 2})

    results.extend([
        run_test("clock_merge_disjoint", test_clock_merge_disjoint),
        run_test("clock_merge_overlapping", test_clock_merge_overlapping),
        run_test("clock_merge_subset", test_clock_merge_subset),
        run_test("clock_merge_empty_left", test_clock_merge_empty_left),
        run_test("clock_merge_empty_right", test_clock_merge_empty_right),
        run_test("clock_merge_does_not_mutate", test_clock_merge_does_not_mutate),
    ])

    # vc_to_str tests
    def test_clock_to_str_empty():
        assert_eq(clock.vc_to_str({}), "{}")

    def test_clock_to_str_single():
        assert_eq(clock.vc_to_str({0: 1}), "{N0:1}")

    def test_clock_to_str_sorted():
        result = clock.vc_to_str({2: 3, 0: 1})
        assert_eq(result, "{N0:1, N2:3}")

    results.extend([
        run_test("clock_to_str_empty", test_clock_to_str_empty),
        run_test("clock_to_str_single", test_clock_to_str_single),
        run_test("clock_to_str_sorted", test_clock_to_str_sorted),
    ])

    return results


# ============================================================================
# Value Tests
# ============================================================================

def run_value_tests() -> List[TestResult]:
    results = []

    def test_value_init_defaults():
        v = Value("hello")
        assert_eq(v.get_value(), "hello")
        assert_eq(v.get_deps(), {})

    def test_value_init_with_timestamp():
        v = Value("hello", 123.0)
        assert_eq(v.get_time(), 123.0)

    def test_value_set_get_value():
        v = Value("a")
        v.set_value("b")
        assert_eq(v.get_value(), "b")

    def test_value_dep_length_empty():
        v = Value("x")
        assert_eq(v.dep_length(), 0)

    def test_value_add_dep():
        v = Value("x")
        v.add_dep("k", {0: 1})
        assert_eq(v.dep_length(), 1)
        assert_eq(v.get_clock_at("k"), {0: 1})

    def test_value_set_deps():
        v = Value("x")
        v.set_deps({"a": {0: 1}, "b": {1: 2}})
        assert_eq(v.dep_length(), 2)

    def test_value_set_get_time():
        v = Value("x")
        v.set_time(42.0)
        assert_eq(v.get_time(), 42.0)

    def test_value_to_dict():
        v = Value("hello", 1.0)
        d = v.to_dict()
        assert_in("value", d)
        assert_in("timestamp", d)
        assert_in("dependencies", d)
        assert_eq(d["value"], "hello")

    def test_value_from_dict_round_trip():
        v1 = Value("test", 99.0)
        v1.add_dep("k", {0: 1})
        d = v1.to_dict()
        v2 = Value.from_dict(d)
        assert_eq(v2.get_value(), "test")
        assert_eq(v2.get_time(), 99.0)
        assert_eq(v2.get_clock_at("k"), {0: 1})

    def test_value_from_dict_missing_timestamp():
        d = {"value": "x"}
        v = Value.from_dict(d)
        assert_eq(v.get_value(), "x")

    def test_value_from_dict_missing_deps():
        d = {"value": "x", "timestamp": 1.0}
        v = Value.from_dict(d)
        assert_eq(v.get_deps(), {})

    results.extend([
        run_test("value_init_defaults", test_value_init_defaults),
        run_test("value_init_with_timestamp", test_value_init_with_timestamp),
        run_test("value_set_get_value", test_value_set_get_value),
        run_test("value_dep_length_empty", test_value_dep_length_empty),
        run_test("value_add_dep", test_value_add_dep),
        run_test("value_set_deps", test_value_set_deps),
        run_test("value_set_get_time", test_value_set_get_time),
        run_test("value_to_dict", test_value_to_dict),
        run_test("value_from_dict_round_trip", test_value_from_dict_round_trip),
        run_test("value_from_dict_missing_timestamp", test_value_from_dict_missing_timestamp),
        run_test("value_from_dict_missing_deps", test_value_from_dict_missing_deps),
    ])

    return results


# ============================================================================
# Node Tests
# ============================================================================

def run_node_tests() -> List[TestResult]:
    results = []

    # Basic put/get
    def test_node_put_basic():
        n = Node(0, 0)
        result = n.put("k", "v", {})
        assert_in("metadata", result)
        assert_in("clock_after", result)

    def test_node_put_increments_clock():
        n = Node(0, 0)
        result = n.put("k", "v", {})
        assert_eq(result["clock_after"][0], 1)

    def test_node_put_stores_value():
        n = Node(0, 0)
        n.put("k", "v", {})
        assert_eq(n.kv_store["k"].get_value(), "v")

    def test_node_get_basic():
        n = Node(0, 0)
        put_result = n.put("k", "v", {})
        result = n.get("k", put_result["metadata"])
        assert_eq(result["value"], "v")

    def test_node_get_not_found():
        n = Node(0, 0)
        result = n.get("missing", {})
        assert_eq(result["error"], "not_found")

    def test_node_put_overwrites():
        n = Node(0, 0)
        n.put("k", "v1", {})
        n.put("k", "v2", {})
        assert_eq(n.kv_store["k"].get_value(), "v2")

    def test_node_put_second_increments():
        n = Node(0, 0)
        n.put("k", "v1", {})
        result = n.put("k", "v2", {})
        assert_eq(result["clock_after"][0], 2)

    results.extend([
        run_test("node_put_basic", test_node_put_basic),
        run_test("node_put_increments_clock", test_node_put_increments_clock),
        run_test("node_put_stores_value", test_node_put_stores_value),
        run_test("node_get_basic", test_node_get_basic),
        run_test("node_get_not_found", test_node_get_not_found),
        run_test("node_put_overwrites", test_node_put_overwrites),
        run_test("node_put_second_increments", test_node_put_second_increments),
    ])

    # Dead node
    def test_node_dead_put_returns_error():
        n = Node(0, 0)
        n.alive = False
        result = n.put("k", "v", {})
        assert_eq(result["error"], "Node is dead")

    def test_node_dead_get_returns_error():
        n = Node(0, 0)
        n.alive = False
        result = n.get("k", {})
        assert_eq(result["error"], "Node is dead")

    def test_node_dead_gossip_returns_empty():
        n1 = Node(0, 0)
        n2 = Node(1, 0)
        n1.alive = False
        result = n1.receive_local_gossip(n2)
        assert_eq(result, [])

    results.extend([
        run_test("node_dead_put_returns_error", test_node_dead_put_returns_error),
        run_test("node_dead_get_returns_error", test_node_dead_get_returns_error),
        run_test("node_dead_gossip_returns_empty", test_node_dead_gossip_returns_empty),
    ])

    # Causal readiness
    def test_node_get_not_ready_when_ahead():
        n = Node(0, 0)
        n.put("k", "v", {})
        client_clock = {"k": {1: 999}}  # Far ahead
        result = n.get("k", client_clock)
        assert_eq(result["error"], "not_ready")

    def test_node_get_ready_when_equal():
        n = Node(0, 0)
        result1 = n.put("k", "v", {})
        client_clock = result1["metadata"]
        result2 = n.get("k", client_clock)
        assert_eq(result2["value"], "v")

    results.extend([
        run_test("node_get_not_ready_when_ahead", test_node_get_not_ready_when_ahead),
        run_test("node_get_ready_when_equal", test_node_get_ready_when_equal),
    ])

    # Dependencies
    def test_node_put_tracks_deps():
        n = Node(0, 0)
        n.put("a", "v", {})
        client_clock = {"a": {0: 1}}
        n.put("b", "v2", client_clock)
        assert_in("a", n.kv_store["b"].get_deps())

    results.extend([
        run_test("node_put_tracks_deps", test_node_put_tracks_deps),
    ])

    # key_count
    def test_node_key_count_basic():
        n = Node(0, 0)
        n.put("k1", "v1", {})
        n.put("k2", "v2", {})
        assert_eq(n.key_count(), 2)

    def test_node_key_count_excludes_tombstones():
        n = Node(0, 0)
        n.put("k", "v", {})
        n.put("k", "__TOMBSTONE__", {})
        assert_eq(n.key_count(), 0)

    results.extend([
        run_test("node_key_count_basic", test_node_key_count_basic),
        run_test("node_key_count_excludes_tombstones", test_node_key_count_excludes_tombstones),
    ])

    # export/import
    def test_node_export_state_deep_copy():
        n = Node(0, 0)
        n.put("k", "v", {})
        kv, clocks = n.export_state()
        kv["k"].set_value("modified")
        assert_eq(n.kv_store["k"].get_value(), "v")

    def test_node_import_key():
        n = Node(0, 0)
        v = Value("imported")
        vc = {1: 5}
        n.import_key("k", v, vc)
        assert_eq(n.kv_store["k"].get_value(), "imported")
        assert_eq(n.local_clock["k"], {1: 5})

    results.extend([
        run_test("node_export_state_deep_copy", test_node_export_state_deep_copy),
        run_test("node_import_key", test_node_import_key),
    ])

    return results


# ============================================================================
# Shard Tests
# ============================================================================

def run_shard_tests() -> List[TestResult]:
    results = []

    def test_shard_add_remove_node():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        assert_in(0, s.nodes)
        s.remove_node(0)
        assert_false(0 in s.nodes)

    def test_shard_get_alive_nodes_filters_dead():
        s = Shard(0)
        n1 = Node(0, 0)
        n2 = Node(1, 0)
        s.add_node(n1)
        s.add_node(n2)
        n1.alive = False
        alive = s.get_alive_nodes()
        assert_eq(len(alive), 1)
        assert_eq(alive[0].node_id, 1)

    def test_shard_get_any_alive_node_none():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        n.alive = False
        assert_eq(s.get_any_alive_node(), None)

    def test_shard_get_any_alive_node_exists():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        assert_true(s.get_any_alive_node() is not None)

    def test_shard_is_alive_true():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        assert_true(s.is_alive())

    def test_shard_is_alive_false():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        n.alive = False
        assert_false(s.is_alive())

    def test_shard_route_put_success():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        result, node = s.route_put("k", "v", {})
        assert_true("error" not in result)

    def test_shard_route_get_not_found():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        result, _ = s.route_get("missing", {})
        assert_eq(result["error"], "not_found")

    def test_shard_route_get_success():
        s = Shard(0)
        n = Node(0, 0)
        s.add_node(n)
        put_result, _ = s.route_put("k", "v", {})
        result, _ = s.route_get("k", put_result["metadata"])
        assert_eq(result["value"], "v")

    def test_shard_local_gossip_propagates():
        s = Shard(0)
        n1 = Node(0, 0)
        n2 = Node(1, 0)
        s.add_node(n1)
        s.add_node(n2)
        n1.put("k", "v", {})
        s.run_local_gossip()
        assert_in("k", n2.kv_store)

    results.extend([
        run_test("shard_add_remove_node", test_shard_add_remove_node),
        run_test("shard_get_alive_nodes_filters_dead", test_shard_get_alive_nodes_filters_dead),
        run_test("shard_get_any_alive_node_none", test_shard_get_any_alive_node_none),
        run_test("shard_get_any_alive_node_exists", test_shard_get_any_alive_node_exists),
        run_test("shard_is_alive_true", test_shard_is_alive_true),
        run_test("shard_is_alive_false", test_shard_is_alive_false),
        run_test("shard_route_put_success", test_shard_route_put_success),
        run_test("shard_route_get_not_found", test_shard_route_get_not_found),
        run_test("shard_route_get_success", test_shard_route_get_success),
        run_test("shard_local_gossip_propagates", test_shard_local_gossip_propagates),
    ])

    return results


# ============================================================================
# Cluster Basic Tests
# ============================================================================

def run_cluster_basic_tests() -> List[TestResult]:
    results = []

    def test_cluster_initialize_creates_shards():
        c = Cluster()
        c.initialize(3, 2)
        assert_eq(c.total_shards, 3)
        assert_eq(len(c.shards), 3)

    def test_cluster_initialize_correct_node_count():
        c = Cluster()
        c.initialize(2, 3)
        for shard in c.shards.values():
            assert_eq(len(shard.nodes), 3)

    def test_cluster_put_returns_metadata():
        c = Cluster()
        c.initialize(1, 1)
        result = c.put("k", "v", {})
        assert_in("metadata", result)

    def test_cluster_put_returns_shard_node_id():
        c = Cluster()
        c.initialize(1, 1)
        result = c.put("k", "v", {})
        assert_in("shard_id", result)
        assert_in("node_id", result)

    def test_cluster_get_after_put():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "v", {})
        get_result = c.get("k", put_result["metadata"])
        assert_eq(get_result["value"], "v")

    def test_cluster_hash_key_deterministic():
        c = Cluster()
        c.initialize(2, 1)
        h1 = c.hash_key("test")
        h2 = c.hash_key("test")
        assert_eq(h1, h2)

    def test_cluster_hash_key_in_range():
        c = Cluster()
        c.initialize(5, 1)
        h = c.hash_key("anything")
        assert_true(0 <= h < 5)

    def test_cluster_delete_writes_tombstone():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "v", {})
        del_result = c.delete("k", put_result["metadata"])
        result = c.get("k", del_result["metadata"])
        assert_eq(result["error"], "deleted")

    def test_cluster_get_all_keys_excludes_tombstones():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k1", "v1", {})
        c.put("k2", "v2", {})
        c.delete("k1", {})
        keys = c.get_all_keys()
        assert_in("k2", keys)
        assert_false("k1" in keys)

    def test_cluster_get_all_nodes_returns_all():
        c = Cluster()
        c.initialize(3, 2)
        nodes = c.get_all_nodes()
        assert_eq(len(nodes), 6)

    results.extend([
        run_test("cluster_initialize_creates_shards", test_cluster_initialize_creates_shards),
        run_test("cluster_initialize_correct_node_count", test_cluster_initialize_correct_node_count),
        run_test("cluster_put_returns_metadata", test_cluster_put_returns_metadata),
        run_test("cluster_put_returns_shard_node_id", test_cluster_put_returns_shard_node_id),
        run_test("cluster_get_after_put", test_cluster_get_after_put),
        run_test("cluster_hash_key_deterministic", test_cluster_hash_key_deterministic),
        run_test("cluster_hash_key_in_range", test_cluster_hash_key_in_range),
        run_test("cluster_delete_writes_tombstone", test_cluster_delete_writes_tombstone),
        run_test("cluster_get_all_keys_excludes_tombstones", test_cluster_get_all_keys_excludes_tombstones),
        run_test("cluster_get_all_nodes_returns_all", test_cluster_get_all_nodes_returns_all),
    ])

    return results


# ============================================================================
# Fault Tolerance Tests
# ============================================================================

def run_fault_tolerance_tests() -> List[TestResult]:
    results = []

    def test_cluster_kill_node_marks_dead():
        c = Cluster()
        c.initialize(1, 2)
        c.kill_node(0)
        node = c._find_node(0)
        assert_false(node.alive)

    def test_cluster_kill_node_nonexistent_error():
        c = Cluster()
        c.initialize(1, 1)
        result = c.kill_node(999)
        assert_in("error", result)

    def test_cluster_kill_node_shard_still_alive():
        c = Cluster()
        c.initialize(1, 2)
        result = c.kill_node(0)
        assert_in("still has", result["status"])

    def test_cluster_revive_node_marks_alive():
        c = Cluster()
        c.initialize(1, 1)
        c.kill_node(0)
        c.revive_node(0)
        node = c._find_node(0)
        assert_true(node.alive)

    def test_cluster_revive_nonexistent_error():
        c = Cluster()
        c.initialize(1, 1)
        result = c.revive_node(999)
        assert_in("error", result)

    def test_cluster_revive_already_alive_error():
        c = Cluster()
        c.initialize(1, 1)
        result = c.revive_node(0)
        assert_in("error", result)

    def test_cluster_revive_triggers_gossip():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n1 = c._find_node(1)
        n0.put("k", "v", {})
        n0.alive = False
        c.revive_node(0)
        assert_in("k", n0.kv_store)

    def test_cluster_kill_shard_all_nodes_dead():
        c = Cluster()
        c.initialize(2, 1)
        c.kill_shard(0)
        for node in c.shards[0].nodes.values():
            assert_false(node.alive)

    def test_cluster_kill_shard_nonexistent_error():
        c = Cluster()
        c.initialize(1, 1)
        result = c.kill_shard(999)
        assert_in("error", result)

    results.extend([
        run_test("cluster_kill_node_marks_dead", test_cluster_kill_node_marks_dead),
        run_test("cluster_kill_node_nonexistent_error", test_cluster_kill_node_nonexistent_error),
        run_test("cluster_kill_node_shard_still_alive", test_cluster_kill_node_shard_still_alive),
        run_test("cluster_revive_node_marks_alive", test_cluster_revive_node_marks_alive),
        run_test("cluster_revive_nonexistent_error", test_cluster_revive_nonexistent_error),
        run_test("cluster_revive_already_alive_error", test_cluster_revive_already_alive_error),
        run_test("cluster_revive_triggers_gossip", test_cluster_revive_triggers_gossip),
        run_test("cluster_kill_shard_all_nodes_dead", test_cluster_kill_shard_all_nodes_dead),
        run_test("cluster_kill_shard_nonexistent_error", test_cluster_kill_shard_nonexistent_error),
    ])

    return results


# ============================================================================
# Gossip Tests
# ============================================================================

def run_gossip_tests() -> List[TestResult]:
    results = []

    def test_gossip_local_propagates():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n1 = c._find_node(1)
        n0.put("k", "v", {})
        c.run_gossip()
        assert_in("k", n1.kv_store)

    def test_gossip_newer_clock_wins():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n1 = c._find_node(1)
        n0.put("k", "v1", {})
        n1.put("k", "v2", {})
        n1.kv_store["k"].set_time(time.time() + 100)
        c.run_gossip()
        assert_eq(n0.kv_store["k"].get_value(), "v2")

    def test_gossip_dead_node_excluded():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n1 = c._find_node(1)
        n0.put("k", "v", {})
        n1.alive = False
        c.run_gossip()
        assert_false("k" in n1.kv_store)

    def test_gossip_idempotent():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n0.put("k", "v", {})
        c.run_gossip()
        log1 = c.run_gossip()
        log2 = c.run_gossip()
        assert_eq(len(log2), 0, msg="second gossip should have no changes")

    def test_gossip_global_clock_merges():
        c = Cluster()
        c.initialize(2, 1)
        shard0_node = list(c.shards[0].nodes.values())[0]
        shard1_node = list(c.shards[1].nodes.values())[0]
        shard0_node.put("k_in_0", "v", {})
        shard1_node.put("k_in_1", "v", {})
        c.run_gossip()
        assert_in("k_in_0", shard1_node.local_clock)
        assert_in("k_in_1", shard0_node.local_clock)

    def test_gossip_tombstone_propagates():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n1 = c._find_node(1)
        put_result = n0.put("k", "v", {})
        c.run_gossip()
        # Now delete on n0
        del_result = n0.put("k", "__TOMBSTONE__", put_result["metadata"])
        c.run_gossip()
        # After gossip, n1 should have the tombstone
        assert_eq(n1.kv_store["k"].get_value(), "__TOMBSTONE__")

    results.extend([
        run_test("gossip_local_propagates", test_gossip_local_propagates),
        run_test("gossip_newer_clock_wins", test_gossip_newer_clock_wins),
        run_test("gossip_dead_node_excluded", test_gossip_dead_node_excluded),
        run_test("gossip_idempotent", test_gossip_idempotent),
        run_test("gossip_global_clock_merges", test_gossip_global_clock_merges),
        run_test("gossip_tombstone_propagates", test_gossip_tombstone_propagates),
    ])

    return results


# ============================================================================
# Resharding Tests
# ============================================================================

def run_resharding_tests() -> List[TestResult]:
    results = []

    def test_reshard_to_same_count_errors():
        c = Cluster()
        c.initialize(3, 1)
        result = c.reshard_to(3)
        assert_in("error", result)

    def test_reshard_to_zero_errors():
        c = Cluster()
        c.initialize(1, 1)
        result = c.reshard_to(0)
        assert_in("error", result)

    def test_reshard_scale_up_adds_shards():
        c = Cluster()
        c.initialize(2, 1)
        c.reshard_to(4)
        assert_eq(c.total_shards, 4)

    def test_reshard_scale_down_removes_shards():
        c = Cluster()
        c.initialize(3, 1)
        c.reshard_to(1)
        assert_eq(c.total_shards, 1)

    def test_reshard_keys_accessible_after_scale_up():
        c = Cluster()
        c.initialize(1, 1)
        put1 = c.put("k1", "v1", {})
        put2 = c.put("k2", "v2", {})
        c.reshard_to(3)
        c.run_gossip()
        r1 = c.get("k1", put1["metadata"])
        r2 = c.get("k2", put2["metadata"])
        assert_eq(r1["value"], "v1")
        assert_eq(r2["value"], "v2")

    def test_reshard_scale_down_keys_accessible():
        c = Cluster()
        c.initialize(3, 1)
        put1 = c.put("k1", "v1", {})
        put2 = c.put("k2", "v2", {})
        c.reshard_to(1)
        c.run_gossip()
        r1 = c.get("k1", put1["metadata"])
        r2 = c.get("k2", put2["metadata"])
        assert_eq(r1["value"], "v1")
        assert_eq(r2["value"], "v2")

    def test_reshard_keys_land_on_correct_shard():
        c = Cluster()
        c.initialize(2, 1)
        c.put("k", "v", {})
        expected_shard = c.hash_key("k")
        for shard_id, shard in c.shards.items():
            if "k" in shard.nodes[list(shard.nodes.keys())[0]].kv_store:
                assert_eq(shard_id, expected_shard)
                break

    def test_reshard_tombstones_migrate():
        c = Cluster()
        c.initialize(2, 1)
        put_result = c.put("k", "v", {})
        del_result = c.delete("k", put_result["metadata"])
        c.reshard_to(3)
        c.run_gossip()
        result = c.get("k", del_result["metadata"])
        assert_eq(result["error"], "deleted")

    def test_add_shard_increases_total():
        c = Cluster()
        c.initialize(2, 1)
        c.add_shard()
        assert_eq(c.total_shards, 3)

    def test_add_shard_keys_migrate():
        c = Cluster()
        c.initialize(1, 1)
        put1 = c.put("k1", "v1", {})
        put2 = c.put("k2", "v2", {})
        c.add_shard()
        c.run_gossip()
        r1 = c.get("k1", put1["metadata"])
        r2 = c.get("k2", put2["metadata"])
        assert_eq(r1["value"], "v1")
        assert_eq(r2["value"], "v2")

    def test_remove_shard_decreases_total():
        c = Cluster()
        c.initialize(3, 1)
        c.remove_shard(1)
        assert_eq(c.total_shards, 2)

    def test_remove_shard_last_errors():
        c = Cluster()
        c.initialize(1, 1)
        result = c.remove_shard(0)
        assert_in("error", result)

    def test_remove_shard_nonexistent_errors():
        c = Cluster()
        c.initialize(2, 1)
        result = c.remove_shard(999)
        assert_in("error", result)

    def test_remove_shard_keys_migrated():
        c = Cluster()
        c.initialize(2, 1)
        c.put("k", "v", {})
        c.remove_shard(0)
        result = c.get("k", {})
        assert_true("error" not in result or result["error"] != "Shard")

    results.extend([
        run_test("reshard_to_same_count_errors", test_reshard_to_same_count_errors),
        run_test("reshard_to_zero_errors", test_reshard_to_zero_errors),
        run_test("reshard_scale_up_adds_shards", test_reshard_scale_up_adds_shards),
        run_test("reshard_scale_down_removes_shards", test_reshard_scale_down_removes_shards),
        run_test("reshard_keys_accessible_after_scale_up", test_reshard_keys_accessible_after_scale_up),
        run_test("reshard_scale_down_keys_accessible", test_reshard_scale_down_keys_accessible),
        run_test("reshard_keys_land_on_correct_shard", test_reshard_keys_land_on_correct_shard),
        run_test("reshard_tombstones_migrate", test_reshard_tombstones_migrate),
        run_test("add_shard_increases_total", test_add_shard_increases_total),
        run_test("add_shard_keys_migrate", test_add_shard_keys_migrate),
        run_test("remove_shard_decreases_total", test_remove_shard_decreases_total),
        run_test("remove_shard_last_errors", test_remove_shard_last_errors),
        run_test("remove_shard_nonexistent_errors", test_remove_shard_nonexistent_errors),
        run_test("remove_shard_keys_migrated", test_remove_shard_keys_migrated),
    ])

    return results


# ============================================================================
# Causal Consistency Tests
# ============================================================================

def run_causal_consistency_tests() -> List[TestResult]:
    results = []

    def test_causal_read_your_writes():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "v", {})
        get_result = c.get("k", put_result["metadata"])
        assert_eq(get_result["value"], "v")

    def test_causal_monotonic_reads():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "v1", {})
        result1 = c.get("k", put_result["metadata"])
        result2 = c.get("k", result1["metadata"])
        assert_eq(result2["value"], "v1")

    def test_causal_write_follows_read():
        c = Cluster()
        c.initialize(1, 1)
        put_a = c.put("a", "va", {})
        result_a = c.get("a", put_a["metadata"])
        put_b = c.put("b", "vb", result_a["metadata"])
        b_node = c._find_node(put_b.get("node_id", 0))
        if b_node and "b" in b_node.kv_store:
            assert_in("a", b_node.kv_store["b"].get_deps())

    def test_causal_get_blocked_until_ready():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k", "v", {})
        client_clock = {"k": {99: 999}}
        result = c.get("k", client_clock)
        assert_eq(result["error"], "not_ready")

    results.extend([
        run_test("causal_read_your_writes", test_causal_read_your_writes),
        run_test("causal_monotonic_reads", test_causal_monotonic_reads),
        run_test("causal_write_follows_read", test_causal_write_follows_read),
        run_test("causal_get_blocked_until_ready", test_causal_get_blocked_until_ready),
    ])

    return results


# ============================================================================
# Tombstone Tests
# ============================================================================

def run_tombstone_tests() -> List[TestResult]:
    results = []

    def test_tombstone_delete_writes_tombstone():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k", "v", {})
        c.delete("k", {})
        node = c._find_node(0)
        assert_eq(node.kv_store["k"].get_value(), "__TOMBSTONE__")

    def test_tombstone_get_returns_deleted():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "v", {})
        del_result = c.delete("k", put_result["metadata"])
        result = c.get("k", del_result["metadata"])
        assert_eq(result["error"], "deleted")

    def test_tombstone_not_in_key_count():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k1", "v1", {})
        c.put("k2", "v2", {})
        c.delete("k1", {})
        node = c._find_node(0)
        assert_eq(node.key_count(), 1)

    def test_tombstone_not_in_get_all_keys():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k", "v", {})
        c.delete("k", {})
        keys = c.get_all_keys()
        assert_false("k" in keys)

    def test_tombstone_propagates_via_gossip():
        c = Cluster()
        c.initialize(1, 2)
        n0 = c._find_node(0)
        n1 = c._find_node(1)
        put_result = n0.put("k", "v", {})
        c.run_gossip()
        del_result = n0.put("k", "__TOMBSTONE__", put_result["metadata"])
        c.run_gossip()
        # After gossip, n1 should have the tombstone with updated clock
        assert_eq(n1.kv_store["k"].get_value(), "__TOMBSTONE__")

    def test_tombstone_overwrite_after_delete():
        c = Cluster()
        c.initialize(1, 1)
        put1 = c.put("k", "v1", {})
        del_result = c.delete("k", put1["metadata"])
        put2 = c.put("k", "v2", del_result["metadata"])
        result = c.get("k", put2["metadata"])
        assert_eq(result["value"], "v2")

    results.extend([
        run_test("tombstone_delete_writes_tombstone", test_tombstone_delete_writes_tombstone),
        run_test("tombstone_get_returns_deleted", test_tombstone_get_returns_deleted),
        run_test("tombstone_not_in_key_count", test_tombstone_not_in_key_count),
        run_test("tombstone_not_in_get_all_keys", test_tombstone_not_in_get_all_keys),
        run_test("tombstone_propagates_via_gossip", test_tombstone_propagates_via_gossip),
        run_test("tombstone_overwrite_after_delete", test_tombstone_overwrite_after_delete),
    ])

    return results


# ============================================================================
# Edge Cases Tests
# ============================================================================

def run_edge_cases_tests() -> List[TestResult]:
    results = []

    def test_edge_empty_string_value():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "", {})
        result = c.get("k", put_result["metadata"])
        assert_eq(result["value"], "")

    def test_edge_special_chars_in_key():
        c = Cluster()
        c.initialize(1, 1)
        key = "café/latte"
        put_result = c.put(key, "v", {})
        result = c.get(key, put_result["metadata"])
        assert_eq(result["value"], "v")

    def test_edge_large_value():
        c = Cluster()
        c.initialize(1, 1)
        large = "x" * 10000
        put_result = c.put("k", large, {})
        result = c.get("k", put_result["metadata"])
        assert_eq(result["value"], large)

    def test_edge_many_keys_same_node():
        c = Cluster()
        c.initialize(1, 1)
        for i in range(50):
            c.put(f"k{i}", f"v{i}", {})
        node = c._find_node(0)
        assert_eq(node.key_count(), 50)

    def test_edge_overwrite_no_duplicate():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k", "v1", {})
        c.put("k", "v2", {})
        c.put("k", "v3", {})
        node = c._find_node(0)
        assert_eq(node.key_count(), 1)

    def test_edge_reinitialize_clears_state():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k", "v", {})
        c.initialize(1, 1)
        result = c.get("k", {})
        assert_eq(result["error"], "not_found")

    def test_edge_single_shard_single_node():
        c = Cluster()
        c.initialize(1, 1)
        put_result = c.put("k", "v", {})
        result = c.get("k", put_result["metadata"])
        assert_eq(result["value"], "v")

    def test_edge_many_shards_single_node():
        c = Cluster()
        c.initialize(10, 1)
        put1 = c.put("k1", "v1", {})
        put2 = c.put("k2", "v2", {})
        result = c.get("k1", put1["metadata"])
        assert_eq(result["value"], "v1")

    def test_edge_empty_string_key():
        c = Cluster()
        c.initialize(1, 1)
        try:
            put_result = c.put("", "v", {})
            result = c.get("", put_result["metadata"])
            assert_eq(result["value"], "v")
        except Exception as e:
            raise AssertionError(f"empty string key should work or fail gracefully: {e}")

    def test_edge_concurrent_puts_same_key():
        c = Cluster()
        c.initialize(1, 1)
        c.put("k", "v1", {})
        c.put("k", "v2", {})
        node = c._find_node(0)
        assert_eq(node.local_clock["k"][0], 2)

    results.extend([
        run_test("edge_empty_string_value", test_edge_empty_string_value),
        run_test("edge_special_chars_in_key", test_edge_special_chars_in_key),
        run_test("edge_large_value", test_edge_large_value),
        run_test("edge_many_keys_same_node", test_edge_many_keys_same_node),
        run_test("edge_overwrite_no_duplicate", test_edge_overwrite_no_duplicate),
        run_test("edge_reinitialize_clears_state", test_edge_reinitialize_clears_state),
        run_test("edge_single_shard_single_node", test_edge_single_shard_single_node),
        run_test("edge_many_shards_single_node", test_edge_many_shards_single_node),
        run_test("edge_empty_string_key", test_edge_empty_string_key),
        run_test("edge_concurrent_puts_same_key", test_edge_concurrent_puts_same_key),
    ])

    return results


# ============================================================================
# Main Test Runner
# ============================================================================

def run_all_tests() -> List[TestResult]:
    """Run all test categories and return aggregated results."""
    all_results = []

    categories = [
        ("Vector Clock Tests", run_clock_tests),
        ("Value Tests", run_value_tests),
        ("Node Tests", run_node_tests),
        ("Shard Tests", run_shard_tests),
        ("Cluster Basic Tests", run_cluster_basic_tests),
        ("Fault Tolerance Tests", run_fault_tolerance_tests),
        ("Gossip Tests", run_gossip_tests),
        ("Resharding Tests", run_resharding_tests),
        ("Causal Consistency Tests", run_causal_consistency_tests),
        ("Tombstone Tests", run_tombstone_tests),
        ("Edge Cases Tests", run_edge_cases_tests),
    ]

    for category_name, test_func in categories:
        results = test_func()
        all_results.extend(results)

    return all_results


def print_results(results: List[TestResult]):
    """Print test results in a formatted table with summary."""
    categories = {}

    # Group results by category prefix
    for result in results:
        # Extract category from test name (e.g., "clock_*" -> "clock")
        prefix = result.name.split("_")[0]
        if prefix not in categories:
            categories[prefix] = []
        categories[prefix].append(result)

    console.print()

    # Print each category
    for prefix in sorted(categories.keys()):
        cat_results = categories[prefix]
        console.rule(f"[bold cyan]{prefix.upper()}[/bold cyan]", style="cyan")

        for result in cat_results:
            if result.passed:
                console.print(f"  [green]✓[/green] {result.name}")
            else:
                console.print(f"  [red]✗[/red] {result.name}")
                if result.message:
                    console.print(f"      [dim]{result.message}[/dim]")

        console.print()

    # Print summary
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    color = "green" if passed == total else "red"
    console.print(f"[{color}]{passed}/{total} tests passed[/{color}]")
    console.print()
