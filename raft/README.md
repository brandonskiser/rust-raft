# raft
This is a work-in-progress Rust 🦀 implementation of the [Raft consensus protocol](https://raft.github.io/raft.pdf). An in-memory running Raft is provided, and I plan on adding configuration changes and snapshots in the future. Tests are (not surprisingly) provided in the `tests` directory, and I still need to write more fleshed-out tests to catch the edge cases regarding client processing of logs. For now, basic behavior seems to be functioning, *mostly* :)

## Design/Approach
Raft provides all of the logic for determining whether or not an entry - that is, a state machine input provided by the client - can be applied to the state machine. It makes no assumptions about storage or networking, and these must be provided by the client. In the interim, an in-memory storage implementation of the log is provided for simple testing.

Although Raft is a "simple" consensus algorithm, it's still pretty complex and has some tricky edge cases. I still need to write more tests to call out all of its nuances, but overall I tried to take a deterministic, test-driven, easily verifiable approach.

There are three primary functions a client will call in a running Raft: `tick`, `step`, and `propose`. Since timeouts are central to Raft's implementation, time is maintained logically and incremented by calling `tick` in an even interval. This ensures determinism which allows for simpler verifiability. `step` is called to send an RPC request/response to the node, and `propose` is called to give a client command to be processed and replicated by the running Raft.

These functions return an object containing the messages to be sent to other nodes in the clusters (ie, peers), entries to be committed to nonvolatile storage, and entries to be applied to the state machine.

## Usage
All interaction with the Raft implementation occurs through an instance of `Node`. An instance can be created by passing a `Config` which describes characteristics of the cluster (node id, peers) and the node itself (heartbeat/election timeout duration, etc).
```
let cfg = Config::new_with_defaults(
    // id of this node in the cluster
    1,

    // id's of this node's peers
    vec![2, 3],

    // A type satisfying the Storage trait. This is used to provide
    // access to the log within Raft. An implementation for in-memory
    // storage is provided.
    MemoryStorage::new(),
);

let node = Node::new(cfg);
```

A `Node` has three primary methods to be called: `tick`, `step`, and `propose`.
Example:
```
// Increments the logical clock internally used for heartbeat and election timeouts.
// This should be called on an interval (e.g., every 50 ms)
node.tick();

// Sends a message containing a Raft RPC request or response to be processed.
node.step(&message);

// Propose a command (ie, an input to the state machine) to be replicated by the cluster.
node.propose(cmd);
```

Question is, how does the client know when messages must be sent, or entries can be applied to the state machine? These three methods all return an `Option<Ready>` that contains any messages to be sent to peers, any entries to be appended to the log, and finally any entries that can be applied to the state machine.

```
let Ready { messages, entries, committed_entries } = node.step(&msg).unwrap().unwrap();
```

The server is expected to commit any entries/committed_entries to stable store before sending any messages.

## TODO
In order of importance:
- Async Storage implementation
- Implement `advance`, a method the client must call to ensure that entries have been committed/applied to nonvolatile storage.
- Snapshot RPCs
