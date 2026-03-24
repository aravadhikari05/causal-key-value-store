import cmd
import copy
import threading
from kvstore.cluster import Cluster
from kvstore import display

GOSSIP_INTERVAL = 10  #seconds


class KVShell(cmd.Cmd):
    prompt = "kvs> "

    def __init__(self):
        super().__init__()
        self.cluster = Cluster()
        self.client_clock = {}
        self._initialized = False
        self._gossip_stop = threading.Event()
        self._gossip_thread = None

    def _start_gossip_loop(self):
        self._stop_gossip_loop()
        self._gossip_stop.clear()

        def loop():
            while not self._gossip_stop.wait(GOSSIP_INTERVAL):
                if self._initialized:
                    log = self.cluster.run_gossip()
                    display.print_background_gossip(log)
                    display.console.print(self.prompt, end="")

        self._gossip_thread = threading.Thread(target=loop, daemon=True)
        self._gossip_thread.start()

    def _stop_gossip_loop(self):
        if self._gossip_thread and self._gossip_thread.is_alive():
            self._gossip_stop.set()
            self._gossip_thread.join(timeout=1)

    def preloop(self):
        display.print_welcome()

    def _require_init(self) -> bool:
        if not self._initialized:
            display.print_error("Cluster not initialized. Run: init <shards> <nodes_per_shard>")
            return False
        return True

    def do_init(self, arg):
        parts = arg.split()
        if len(parts) != 2:
            display.print_error("Usage: init <shards> <nodes_per_shard>")
            return
        try:
            num_shards = int(parts[0])
            nodes_per_shard = int(parts[1])
        except ValueError:
            display.print_error("Arguments must be integers")
            return

        if num_shards < 1 or nodes_per_shard < 1:
            display.print_error("Must have at least 1 shard and 1 node per shard")
            return

        self.client_clock = {}
        log = self.cluster.initialize(num_shards, nodes_per_shard)
        self._initialized = True
        display.print_init(log, num_shards, nodes_per_shard)
        self._start_gossip_loop()

    def do_put(self, arg):
        if not self._require_init():
            return
        parts = arg.split(None, 1)
        if len(parts) != 2:
            display.print_error("Usage: put <key> <value>")
            return

        key, value = parts
        client_clock = copy.deepcopy(self.client_clock)
        result = self.cluster.put(key, value, client_clock)

        if "metadata" in result:
            self.client_clock = result["metadata"]

        display.print_put_result(result, key, value)

    def do_get(self, arg):
        if not self._require_init():
            return
        key = arg.strip()
        if not key:
            display.print_error("Usage: get <key>")
            return

        client_clock = copy.deepcopy(self.client_clock)
        result = self.cluster.get(key, client_clock)

        if "metadata" in result:
            self.client_clock.update(result["metadata"])

        display.print_get_result(result, key)

    def do_delete(self, arg):
        if not self._require_init():
            return
        key = arg.strip()
        if not key:
            display.print_error("Usage: delete <key>")
            return

        client_clock = copy.deepcopy(self.client_clock)
        result = self.cluster.delete(key, client_clock)

        if "metadata" in result:
            self.client_clock = result["metadata"]

        display.print_delete_result(result, key)

    def do_kill_node(self, arg):
        if not self._require_init():
            return
        try:
            node_id = int(arg.strip())
        except ValueError:
            display.print_error("Usage: kill_node <node_id>")
            return

        result = self.cluster.kill_node(node_id)
        display.print_kill_node_result(result)

    def do_kill_shard(self, arg):
        if not self._require_init():
            return
        try:
            shard_id = int(arg.strip())
        except ValueError:
            display.print_error("Usage: kill_shard <shard_id>")
            return

        result = self.cluster.kill_shard(shard_id)
        display.print_kill_shard_result(result)

    def do_revive_node(self, arg):
        if not self._require_init():
            return
        try:
            node_id = int(arg.strip())
        except ValueError:
            display.print_error("Usage: revive_node <node_id>")
            return

        result = self.cluster.revive_node(node_id)
        display.print_revive_node_result(result)

    def do_add_node(self, arg):
        if not self._require_init():
            return
        try:
            shard_id = int(arg.strip())
        except ValueError:
            display.print_error("Usage: add_node <shard_id>")
            return

        result = self.cluster.add_node(shard_id)
        display.print_add_node_result(result)

    def do_add_shard(self, arg):
        if not self._require_init():
            return

        result = self.cluster.add_shard()
        display.print_add_shard_result(result)

    def do_remove_shard(self, arg):
        if not self._require_init():
            return
        try:
            shard_id = int(arg.strip())
        except ValueError:
            display.print_error("Usage: remove_shard <shard_id>")
            return

        result = self.cluster.remove_shard(shard_id)
        display.print_remove_shard_result(result)

    def do_reshard(self, arg):
        if not self._require_init():
            return
        try:
            new_count = int(arg.strip())
        except ValueError:
            display.print_error("Usage: reshard <shard_count>")
            return

        result = self.cluster.reshard_to(new_count)
        display.print_reshard_result(result)

    def do_gossip(self, arg):
        if not self._require_init():
            return

        log = self.cluster.run_gossip()
        display.print_gossip_result(log)

    def do_state(self, arg):
        if not self._require_init():
            return

        display.print_cluster_state(self.cluster)

    def do_test(self, arg):
        from tests.tests import run_all_tests, print_results
        results = run_all_tests()
        print_results(results)

    def do_help(self, arg):
        if arg:
            super().do_help(arg)
            return

        from rich.table import Table
        from rich import box as rbox
        table = Table(box=rbox.SIMPLE, show_header=True, header_style="bold")
        table.add_column("Category", style="bold cyan", no_wrap=True)
        table.add_column("Command", style="bold", no_wrap=True)
        table.add_column("Description", no_wrap=True)

        data = [
            ("put <key> <value>", "store a key-value pair"),
            ("get <key>", "retrieve a value"),
            ("delete <key>", "delete a key"),
        ]
        cluster = [
            ("init <shards> <nodes_per_shard>", "create a new cluster"),
            ("state", "show full cluster state"),
            ("gossip", "run a manual gossip round"),
            ("quit", "exit"),
        ]
        nodes_shards = [
            ("kill_node <node_id>", "simulate a node failure"),
            ("kill_shard <shard_id>", "kill all nodes in a shard"),
            ("revive_node <node_id>", "bring a dead node back online"),
            ("add_node <shard_id>", "add a node to a shard"),
            ("add_shard", "add a new shard and reshard"),
            ("remove_shard <shard_id>", "remove a shard and reshard"),
            ("reshard <shard_count>", "change the number of shards"),
        ]
        testing = [
            ("test", "run the full test suite"),
        ]

        for i, (cmd_str, desc) in enumerate(cluster):
            table.add_row("Cluster" if i == 0 else "", cmd_str, desc)
        table.add_row("", "", "")
        for i, (cmd_str, desc) in enumerate(data):
            table.add_row("Data" if i == 0 else "", cmd_str, desc)
        table.add_row("", "", "")
        for i, (cmd_str, desc) in enumerate(nodes_shards):
            table.add_row("Nodes & Shards" if i == 0 else "", cmd_str, desc)
        table.add_row("", "", "")
        for i, (cmd_str, desc) in enumerate(testing):
            table.add_row("Testing" if i == 0 else "", cmd_str, desc)

        display.console.print()
        display.console.print(table)

    def do_exit(self, arg):
        self._stop_gossip_loop()
        display.console.print("\n[dim]Goodbye![/dim]")
        return True

    def emptyline(self):
        pass

    def default(self, line):
        display.print_error(f"Unknown command: {line}. Type 'help' for available commands.")
