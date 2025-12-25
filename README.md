# Distributed Key–Value Store with Causal Consistency

This project implements a distributed, sharded key–value store that provides causal consistency using vector clocks and gossip-based replication. The system supports dynamic view changes, resharding, and fault tolerance through asynchronous communication between nodes.

The code was developed for a distributed systems class. So far, this is my favorite class I've taken so far, and I learned many concepts:
* Potential Causality
* Ordered Delivery (FIFO, Caual, Total Order)
* Clocks (Lamport, Vector)
* Hashing (Consistent Hashing)
* Agreement Problems (Two-phase, Leader Election, Consensus, Failure-detection)
* Replication Protocols (Primary Backup, Chain Replication, Quorum, State-machine)
* Weak Consistency (Eventual, Read-your-writes, Causal)
* Paxos Algorithm

This was the final assignment of the class

---

## Causal Consistency Model

Clients attach causal metadata (vector clocks) to requests.

* **Reads block** (up to a timeout) until the node’s state satisfies the client’s causal dependencies
* **Writes increment** the local vector clock and propagate dependencies
* **Concurrent writes** are resolved deterministically using:

  1. Logical timestamp
  2. Node ID tie-breaker

---

## Gossip Protocol

Two forms of gossip are used:

1. **Local shard gossip**

   * Exchanges key-value data and vector clocks
2. **Global gossip**

   * Shares vector clocks across shards to preserve causality

Gossip runs periodically in the background and ensures eventual convergence.

---

## Sharding & View Changes

* Keys are assigned to shards using a hash-based partitioning function
* The `/view` endpoint updates the system membership
* On view changes:

  * Keys may be migrated to new shards
  * Vector clocks and dependencies are preserved
  * Gossip restarts once the new view stabilizes

---

## API Overview

### Client Operations

* `PUT /data/{key}`
  Stores a value with causal metadata

* `GET /data/{key}`
  Retrieves a value once causal dependencies are satisfied

* `GET /data`
  Retrieves all locally owned keys

### Internal Operations

* `/gossip` — shard-level gossip
* `/global_gossip` — cross-shard clock sharing
* `/view` — update cluster membership
* `/move` — key migration during resharding

---

## Running the System

Each node expects environment variables:

```bash
export NODE_IDENTIFIER=<unique_int_id>
```

Nodes are started independently and joined using the `/view` endpoint.

---

