pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub electionTickCount: u32,
    pub electionTickRand: (u32, u32),
    pub heartbeatTickCount: u32,
}

/// Ready encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed, or sent to other peers.
/// Taken from etcd Raft implementation.
pub struct Ready {
    pub messages: Vec<Message>,
}

pub type NodeId = u32;

pub struct Message {
    pub from: NodeId,
    pub to: NodeId,
}

/// Represents a single node participating in a Raft cluster.
pub struct Node {
    pub id: NodeId,
    pub state: NodeState,
}

impl Node {
    pub fn new(cfg: Config) -> Self {
        Node {
            id: cfg.id,
            state: NodeState::FOLLOWER,
        }
    }
    pub fn tick(&mut self) -> Option<Ready> {
        unimplemented!()
    }
    pub fn step(&mut self, m: Message) -> Option<Ready> {
        unimplemented!()
    }
    pub fn send(&self) -> () {
        unimplemented!()
    }
    pub fn stop(&self) -> () {
        unimplemented!()
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}
