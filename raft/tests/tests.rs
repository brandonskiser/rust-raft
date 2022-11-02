use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;

use raft::message::*;
use raft::storage::*;
use raft::*;

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
            .filter_map(|s| s.node.tick())
            .collect::<Vec<_>>();
        println!("tick_all got rdys: {:?}", rdys);
        self.handle_readys(rdys.into_iter());
    }

    /// Keep applying Ready objects until all nodes no longer have any updates to apply.
    fn handle_readys(&mut self, rdys: impl Iterator<Item = Ready>) {
        let mut queue = rdys.flat_map(|r| r.messages).collect::<VecDeque<_>>();

        while let Some(msg) = queue.pop_front() {
            println!("Sending message: {:?}", msg);
            if let Some(rdy) = self.send_msg(msg) {
                println!("Post message rdy: {:?}", rdy);
                for m in rdy.messages {
                    queue.push_back(m);
                }
            }
        }
    }

    fn send_msg(&mut self, msg: Message) -> Option<Ready> {
        let to = self
            .servers
            .iter_mut()
            .find(|s| s.node.id() == msg.to())
            .expect(format!("node with id {} doesn't exist", msg.to()).as_str());

        to.node.step(&msg)
    }

    fn has_leader(&self) -> bool {
        self.servers
            .iter()
            .any(|s| s.node.state() == NodeState::LEADER)
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
