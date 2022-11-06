use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;

use raft::Config;
use raft::node::*;
use raft::message::*;
use raft::storage::*;

const NODE_IDS: [NodeId; 3] = [1, 2, 3];
const ELECTION_TIMEOUT: u32 = 20;
const HEARTBEAT_TIMEOUT: u32 = 3;

#[derive(Debug)]
struct Server {
    node: Node<Rc<MemoryStorage>>,
    storage: Rc<MemoryStorage>,
}

impl Server {
    fn new(id: NodeId) -> Self {
        let storage = Rc::new(MemoryStorage::new());
        let cfg = Config {
            id,
            peers: NODE_IDS
                .into_iter()
                .filter(|peer_id| id != *peer_id)
                .collect(),
            storage: Rc::clone(&storage),
            election_tick_timeout: ELECTION_TIMEOUT,
            election_tick_rand: 9,
            heartbeat_tick_timeout: HEARTBEAT_TIMEOUT,
        };
        Server {
            node: Node::new(cfg),
            storage,
        }
    }

    fn persist_entries(&mut self, rdy: &mut Ready) {
        self.storage.append_entries(&mut rdy.entries);
    }
}

struct Cluster {
    servers: Vec<Server>,
}

impl Cluster {
    fn new() -> Cluster {
        let servers = NODE_IDS
            .into_iter()
            .map(|id| Server::new(id))
            .collect::<Vec<Server>>();

        println!("Created test cluster: {:?}", servers);

        Cluster { servers }
    }

    /// Return true if cluster has a leader, false if timed out.
    fn tick_while_no_leader(
        &mut self,
        tick_timeout: u32,
    ) -> (bool, Vec<impl IntoIterator + Debug>) {
        let mut ticks = 0;
        let mut state_history: Vec<Vec<NodeState>> = vec![];
        while ticks < tick_timeout && !self.has_leader() {
            self.tick_all();
            ticks += 1;
            state_history.push(self.servers.iter().map(|s| s.node.state()).collect());
        }
        (self.has_leader(), state_history)
    }

    fn tick_all(&mut self) {
        let rdys = self
            .servers
            .iter_mut()
            .filter_map(|s| {
                if let Some(mut rdy) = s.node.tick() {
                    s.persist_entries(&mut rdy);
                    Some(rdy)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        println!("tick_all got rdys: {:?}", rdys);
        self.handle_readys(rdys.into_iter());
    }

    /// Keep applying Ready objects until all nodes no longer have any updates to apply.
    fn handle_readys(&mut self, rdys: impl Iterator<Item = Ready>) {
        let mut queue = rdys.flat_map(|r| r.messages).collect::<VecDeque<_>>();

        while let Some(msg) = queue.pop_front() {
            let to = msg.to();
            println!("Sending message: {:?}", msg);
            if let Some(mut rdy) = self.send_msg(msg) {
                println!("Post message rdy: {:?}", rdy);
                self.get_server_mut(to).persist_entries(&mut rdy);
                for m in rdy.messages {
                    queue.push_back(m);
                }
            }
        }
    }

    /// Calls `step(msg)` on the message's `to()` node. This method does NOT
    /// persist the log entries returned by the Ready `entries` vector.
    fn send_msg(&mut self, msg: Message) -> Option<Ready> {
        let to = self.get_server_mut(msg.to());
        to.node.step(&msg)
    }

    fn has_leader(&self) -> bool {
        self.servers
            .iter()
            .any(|s| s.node.state() == NodeState::LEADER)
    }

    fn get_server_mut(&mut self, node_id: NodeId) -> &mut Server {
        self.servers
            .iter_mut()
            .find(|s| s.node.id() == node_id)
            .unwrap()
    }
}

#[test]
fn new_cluster_will_select_leader() {
    let mut cluster = Cluster::new();
    let (has_leader, history) = cluster.tick_while_no_leader(100);
    assert!(
        has_leader,
        "Expected cluster to have leader. History: {:?}",
        history
    )
}
