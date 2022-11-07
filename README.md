# rust-raft
This is a work-in-progress Rust 🦀 implementation of the [Raft consensus protocol](https://raft.github.io/raft.pdf). This repo is organized as a workspace, and the `raft` package contains the actual implementation of Raft. See the README in the `raft` folder for more info regarding design and usage.

The goal is to provide a strongly-consistent distributed service (in the vein of etcd, Consul, ZooKeeper, etc.) built on top of Raft.

## Why?
For learning, and for fun :) grokking consensus and why it's needed in a distributed system seems pretty important, and no better way to learn than trying to make it yourself.
