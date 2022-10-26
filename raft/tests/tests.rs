use std::collections::VecDeque;

use raft::*;

struct Cluster {
    nodes: Vec<Node>,
}

impl Cluster {
    fn new(nodes: Vec<Node>) -> Cluster {
        Cluster { nodes }
    }

    /// Return true if cluster has a leader, false if timed out.
    fn tick_while_no_leader(&mut self, tick_timeout: u32) -> bool {
        let mut ticks = 0;
        while ticks < tick_timeout && !self.has_leader() {
            self.tick_all();
            ticks += 1;
        }
        self.has_leader()
    }

    fn tick_all(&mut self) {
        let rdys = self
            .nodes
            .iter_mut()
            .filter_map(|n| n.tick())
            .collect::<Vec<_>>();
        self.handle_readys(rdys.into_iter());
    }

    /// Keep applying Ready objects until all nodes no longer have any updates to apply.
    fn handle_readys(&mut self, rdys: impl Iterator<Item = Ready>) {
        let mut queue = rdys.flat_map(|r| r.messages).collect::<VecDeque<_>>();

        while let Some(msg) = queue.pop_front() {
            if let Some(rdy) = self.send_msg(msg) {
                for m in rdy.messages {
                    queue.push_back(m);
                }
            }
        }
    }

    fn send_msg(&mut self, msg: Message) -> Option<Ready> {
        let to = self
            .nodes
            .iter_mut()
            .find(|n| n.id == msg.to)
            .expect(format!("node with id {} doesn't exist", msg.to).as_str());
        
        to.step(msg)
    }

    fn has_leader(&self) -> bool {
        self.nodes.iter().any(|n| n.state == NodeState::LEADER)
    }
}

fn new_default_cfg(id: NodeId) -> Config {
    assert!(id >= 1 && id <= 3, "id should be between 1 and 3");
    Config {
        id,
        peers: vec![1, 2, 3]
            .into_iter()
            .filter(|peer_id| id == *peer_id)
            .collect(),
        electionTickCount: 10,
        electionTickRand: (0, 20),
        heartbeatTickCount: 10,
    }
}

fn new_test_cluster() -> Cluster {
    let nodes = (1..4)
        .into_iter()
        .map(|id| Node::new(new_default_cfg(id)))
        .collect::<Vec<Node>>();

    Cluster::new(nodes)
}

#[test]
fn new_cluster_will_select_leader() {
    let mut cluster = new_test_cluster();
    let has_leader = cluster.tick_while_no_leader(100);
    assert!(has_leader)
}
