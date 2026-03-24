import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from typing import Dict, List
from kvstore.clock import vc_to_str
from kvstore.node import TOMBSTONE

console = Console()


def print_welcome():
    console.print(Panel(
        "[bold]Causal Key-Value Store Simulator[/bold]\n"
        "An interactive demo of distributed systems concepts:\n"
        "vector clocks, gossip protocol, sharding, and replication.\n\n"
        "Type [bold cyan]help[/bold cyan] for available commands.",
        border_style="blue",
    ))


def print_init(log: List[str], num_shards: int, nodes_per_shard: int):
    console.print(f"\n[bold green]Created cluster:[/bold green] {num_shards} shard(s), {nodes_per_shard} node(s) each")
    for line in log:
        console.print(line)
    console.print()


def print_put_result(result: Dict, key: str, value: str):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]PUT[/bold green] \"{key}\" = \"{value}\"")
    console.print(f"  Routed to [cyan]Shard {result['shard_id']}[/cyan] -> [cyan]Node-{result['node_id']}[/cyan]")
    console.print(f"  Clock: {vc_to_str(result['clock_before'])} -> {vc_to_str(result['clock_after'])}")
    console.print()


def print_get_result(result: Dict, key: str):
    if "error" in result:
        if result["error"] == "not_ready":
            console.print(f"\n[bold yellow]GET \"{key}\" — NOT READY[/bold yellow]")
            console.print(f"  Client clock: {vc_to_str(result.get('client_clock', {}))}")
            console.print(f"  Node clock:   {vc_to_str(result.get('node_clock', {}))}")
            console.print("  The node hasn't received all causal dependencies yet.")
            console.print("  Try running [bold cyan]gossip[/bold cyan] and retrying.\n")
        elif result["error"] == "not_found":
            console.print(f"\n[bold yellow]GET \"{key}\" — NOT FOUND[/bold yellow]")
            console.print(f"  {result.get('detail', 'Key does not exist')}\n")
        elif result["error"] == "deleted":
            console.print(f"\n[bold yellow]GET \"{key}\" — DELETED[/bold yellow]")
            console.print(f"  {result.get('detail', 'Key was deleted')}\n")
        else:
            console.print(f"\n[bold red]Error:[/bold red] {result['error']}\n")
        return

    console.print(f"\n[bold green]GET[/bold green] \"{key}\"")
    console.print(f"  Routed to [cyan]Shard {result['shard_id']}[/cyan] -> [cyan]Node-{result['node_id']}[/cyan]")
    console.print(f"  Value: [bold]\"{result['value']}\"[/bold]")
    console.print()


def print_delete_result(result: Dict, key: str):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]DELETE[/bold green] \"{key}\"")
    console.print(f"  Routed to [cyan]Shard {result['shard_id']}[/cyan] -> [cyan]Node-{result['node_id']}[/cyan]")
    console.print(f"  Key marked as deleted (tombstone written)")
    console.print()


def print_kill_node_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold red]KILL NODE[/bold red] Node-{result['node_id']}")
    console.print(f"  Was in Shard {result['shard_id']} with {result['keys_on_node']} key(s)")
    console.print(f"  {result['status']}")
    if "replication" in result:
        if "WARNING" in result["replication"]:
            console.print(f"  [bold yellow]{result['replication']}[/bold yellow]")
        else:
            console.print(f"  [green]{result['replication']}[/green]")
    console.print()


def print_kill_shard_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold red]KILL SHARD[/bold red] Shard {result['shard_id']}")
    console.print(f"  Killed: {', '.join(result['killed_nodes'])}")
    console.print(f"  [bold yellow]{result['status']}[/bold yellow]")
    console.print()


def print_revive_node_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]REVIVE NODE[/bold green] Node-{result['node_id']}")
    console.print(f"  {result['status']}")
    _print_gossip(result.get("gossip_log", []))
    console.print()


def print_add_node_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]ADD NODE[/bold green] Node-{result['node_id']}")
    console.print(f"  {result['status']}")
    _print_gossip(result.get("gossip_log", []))
    console.print()


def print_add_shard_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]ADD SHARD[/bold green] Shard {result['shard_id']}")
    console.print(f"  Node-{result['node_id']} created for the new shard")
    console.print(f"  Shards: {result['old_shards']} -> {result['new_shards']}")
    if result.get("migration_log"):
        console.print("  [bold]Key migrations:[/bold]")
        for line in result["migration_log"]:
            console.print(f"  {line}")
    _print_gossip(result.get("gossip_log", []))
    console.print()


def print_remove_shard_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]REMOVE SHARD[/bold green] Shard {result['removed_shard']}")
    console.print(f"  New total: {result['new_total']} shard(s)")
    if result.get("migration_log"):
        console.print("  [bold]Key migrations:[/bold]")
        for line in result["migration_log"]:
            console.print(f"  {line}")
    _print_gossip(result.get("gossip_log", []))
    console.print()


def print_reshard_result(result: Dict):
    if "error" in result:
        console.print(f"[bold red]Error:[/bold red] {result['error']}")
        return

    console.print(f"\n[bold green]RESHARD[/bold green] {result['old_shards']} -> {result['new_shards']} shard(s)")
    if result.get("shards_added"):
        added = ", ".join(f"Shard {s['shard_id']} (Node-{s['node_id']})" for s in result["shards_added"])
        console.print(f"  Added: {added}")
    if result.get("shards_removed"):
        removed = ", ".join(f"Shard {s}" for s in result["shards_removed"])
        console.print(f"  Removed: {removed}")
    if result.get("migration_log"):
        console.print("  [bold]Key migrations:[/bold]")
        for line in result["migration_log"]:
            console.print(f"  {line}")
    else:
        console.print("  [dim]No key migrations needed[/dim]")
    _print_gossip(result.get("gossip_log", []))
    console.print()


def print_gossip_result(log: List[str]):
    if not log:
        console.print("\n[dim]Gossip round complete — no changes needed.[/dim]\n")
    else:
        console.print(f"\n[bold cyan]Gossip round:[/bold cyan] {len(log)} update(s)")
        for line in log:
            console.print(line)
        console.print()


def print_cluster_state(cluster):
    """Print a table showing all shards, nodes, keys, and clocks."""
    table = Table(title="Cluster State", box=box.ROUNDED, show_lines=True)
    table.add_column("Shard", style="bold")
    table.add_column("Node", style="cyan")
    table.add_column("Status")
    table.add_column("Keys", justify="right")
    table.add_column("Store Contents")
    table.add_column("Vector Clocks")

    for shard_id in sorted(cluster.shards.keys()):
        shard = cluster.shards[shard_id]
        for node in sorted(shard.nodes.values(), key=lambda n: n.node_id):
            status = "[green]ALIVE[/green]" if node.alive else "[red]DEAD[/red]"

            # Store contents
            store_parts = []
            for k, v in sorted(node.kv_store.items()):
                val = v.get_value()
                if val == TOMBSTONE:
                    store_parts.append(f"{k}: [dim](deleted)[/dim]")
                else:
                    store_parts.append(f"{k}: \"{val}\"")
            store_str = ", ".join(store_parts) if store_parts else "[dim]empty[/dim]"

            # Clocks
            clock_parts = []
            for k in sorted(node.local_clock.keys()):
                clock_parts.append(f"{k}: {vc_to_str(node.local_clock[k])}")
            clock_str = "\n".join(clock_parts) if clock_parts else "[dim]none[/dim]"

            table.add_row(
                f"Shard {shard_id}",
                f"Node-{node.node_id}",
                status,
                str(node.key_count()),
                store_str,
                clock_str,
            )

    console.print()
    console.print(table)
    console.print()


def print_background_gossip(log: List[str]):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    if not log:
        console.print(f"[green]● Gossip [{ts}] — in sync[/green]")
    else:
        console.print(f"[red]● Gossip [{ts}] — {len(log)} change(s) detected[/red]")
        for line in log:
            console.print(f"[red]  {line}[/red]")


def print_error(msg: str):
    console.print(f"[bold red]Error:[/bold red] {msg}")


def _print_gossip(log: List[str]):
    if log:
        console.print(f"  [bold cyan]Gossip:[/bold cyan] {len(log)} update(s)")
        for line in log:
            console.print(f"  {line}")
    else:
        console.print(f"  [dim]Gossip: all nodes already in sync[/dim]")
