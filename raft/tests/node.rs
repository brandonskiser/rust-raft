use std::rc::Rc;

use raft::message::*;
use raft::storage::*;
use raft::*;

const CLUSTER_SIZE: u32 = 3;
const ELECTION_TIMEOUT: u32 = 10;
const HEARTBEAT_TIMEOUT: u32 = 1;

struct NodeTester {
    node: Node<Rc<MemoryStorage>>,
    storage: Rc<MemoryStorage>,
}

impl NodeTester {
    fn new_initial_node() -> Self {
        let storage = Rc::new(MemoryStorage::new());
        Self {
            node: Node::new(Config {
                id: 0,
                peers: (1..CLUSTER_SIZE).collect(),
                storage: Rc::clone(&storage),
                election_tick_timeout: ELECTION_TIMEOUT,
                election_tick_rand: 0,
                heartbeat_tick_timeout: HEARTBEAT_TIMEOUT,
            }),
            storage,
        }
    }

    fn new_candidate_node() -> (Self, Ready) {
        let mut nt = NodeTester::new_initial_node();
        let rdy = nt.tick_for(ELECTION_TIMEOUT);
        (nt, rdy[0].clone())
    }

    fn new_leader_node() -> (Self, Ready) {
        let (mut nt, rdy) = NodeTester::new_candidate_node();

        // Send enough RequestVoteResp(true) to trigger the quorum condition.
        // Note that the node already votes for itself.
        let quorum = CLUSTER_SIZE / 2 + 1;
        let msgs = rdy
            .messages
            .iter()
            .take(quorum as usize - 1) // Candidate should vote for itself, so subtract 1.
            .map(|m| {
                Message::new(
                    MessageBody {
                        term: 0,
                        variant: MessageRPC::RequestVoteResp(true),
                    },
                    MessageMetadata {
                        rpc_id: m.metadata().rpc_id,
                        from: m.to(),
                        to: 0,
                    },
                )
            })
            .collect::<Vec<_>>();
        for m in msgs[0..msgs.len() - 1].iter() {
            nt.node.step(m);
        }
        let rdy = nt
            .node
            .step(&msgs[msgs.len() - 1])
            .expect("Should have Ready after quorum");

        (nt, rdy)
    }

    fn assert_election_started(&self, rdy: &Ready, term: u32) {
        assert_eq!(
            self.node.state(),
            NodeState::CANDIDATE,
            "Expected node to be CANDIDATE"
        );
        assert_eq!(rdy.messages.len() as u32, CLUSTER_SIZE - 1);
        for (i, m) in rdy.messages.iter().enumerate() {
            assert_eq!(
                m.body(),
                &MessageBody {
                    term,
                    variant: MessageRPC::RequestVote(RequestVoteArgs {
                        candidate_id: 0,
                        last_log_index: 0,
                        last_log_term: 0
                    })
                }
            );
            assert_eq!(m.from(), 0);
            assert_eq!(m.to(), i as u32 + 1);
        }
    }

    fn assert_became_leader(&self, rdy: &Ready, term: u32) {
        assert_eq!(self.node.state(), NodeState::LEADER);
        assert_eq!(rdy.messages.len() as u32, CLUSTER_SIZE - 1);
        let noop = Entry {
            term,
            index: 1,
            noop: true,
            data: vec![],
        };
        for (i, m) in rdy.messages.iter().enumerate() {
            assert_eq!(
                m.body(),
                &MessageBody {
                    term,
                    variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                        leader_id: 0,
                        // TODO: Update this config for other test cases.
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: vec![noop.clone()],
                        leader_commit: 0
                    })
                }
            )
        }
        // Leader makes no-op entry.
        assert_eq!(rdy.entries.len(), 1);
        assert_eq!(rdy.entries[0], noop);
    }

    fn tick_for(&mut self, times: u32) -> Vec<Ready> {
        let mut rdys: Vec<Option<Ready>> = vec![];
        for _ in 0..times {
            rdys.push(self.node.tick());
        }
        rdys.into_iter().filter_map(|v| v).collect()
    }
}

#[test]
fn node_init_state() {
    let node = NodeTester::new_initial_node().node;
    assert_eq!(node.id(), 0);
    assert_eq!(node.state(), NodeState::FOLLOWER);
}

#[test]
fn new_follower_starts_election_after_timeout() {
    let mut nt = NodeTester::new_initial_node();

    assert_eq!(nt.tick_for(ELECTION_TIMEOUT - 1).len(), 0);
    let rdy = nt.node.tick().expect("Should have Ready after timeout");

    nt.assert_election_started(&rdy, 1);
}

#[test]
fn new_candidate_starts_election_after_timeout() {
    let (mut nt, _) = NodeTester::new_candidate_node();

    assert_eq!(nt.tick_for(ELECTION_TIMEOUT - 1).len(), 0);
    let rdy = nt.node.tick().expect("Should have Ready after timeout");

    nt.assert_election_started(&rdy, 2);
}

#[test]
fn candidate_ignores_vote_from_previous_election() {
    let (mut nt, rdy) = NodeTester::new_candidate_node();

    // Timeout, and start a second election.
    nt.tick_for(ELECTION_TIMEOUT);

    // Create peer responses from the first election.
    let resps = rdy
        .messages
        .iter()
        .map(|m| {
            Message::new(
                MessageBody {
                    term: 1,
                    variant: MessageRPC::RequestVoteResp(true),
                },
                MessageMetadata {
                    rpc_id: m.metadata().rpc_id,
                    from: m.to(),
                    to: 0,
                },
            )
        })
        .collect::<Vec<_>>();

    for (i, resp) in resps.iter().enumerate() {
        assert!(
            nt.node.step(resp).is_none(),
            "Received unexpected Ready in iteration {}",
            i
        );
    }
}

#[test]
fn follower_with_no_logs_responds_to_heartbeat() {
    let mut nt = NodeTester::new_initial_node();

    let heartbeat = Message::new(
        MessageBody {
            term: 0,
            variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        },
        MessageMetadata {
            rpc_id: 0,
            from: 1,
            to: 0,
        },
    );

    let rdy = nt
        .node
        .step(&heartbeat)
        .expect("Should have Ready with AppendEntriesResp");

    assert_eq!(rdy.messages.len(), 1);
    assert_eq!(
        rdy.messages[0].body(),
        &MessageBody {
            term: 0,
            variant: MessageRPC::AppendEntriesResp(true)
        }
    );
}

#[test]
fn follower_with_no_logs_votes_for_candidate() {
    let mut nt = NodeTester::new_initial_node();

    let reqvote = Message::new(
        MessageBody {
            term: 1,
            variant: MessageRPC::RequestVote(RequestVoteArgs {
                candidate_id: 1,
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        MessageMetadata {
            rpc_id: 0,
            from: 1,
            to: 0,
        },
    );
    let rdy = nt
        .node
        .step(&reqvote)
        .expect("Should have Ready with RequestVoteResp");

    assert_eq!(rdy.messages.len(), 1);
    assert_eq!(
        rdy.messages[0].body(),
        &MessageBody {
            term: 1,
            variant: MessageRPC::RequestVoteResp(true)
        }
    );
}

#[test]
fn candidate_with_quorum_becomes_leader() {
    let (nt, rdy) = NodeTester::new_leader_node();
    nt.assert_became_leader(&rdy, 1);
}

#[test]
fn candidate_becomes_follower_on_msg_from_new_leader() {
    let (mut nt, _) = NodeTester::new_candidate_node();
    let msg = Message::new(
        MessageBody {
            term: 1,
            variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 0,
                entries: vec![],
            }),
        },
        MessageMetadata {
            rpc_id: 0,
            from: 1,
            to: 0,
        },
    );
    nt.node.step(&msg);
    assert_eq!(nt.node.state(), NodeState::FOLLOWER);
}

#[test]
fn leader_sends_heartbeats_after_timeout() {
    let (mut nt, _) = NodeTester::new_leader_node();

    assert_eq!(nt.tick_for(HEARTBEAT_TIMEOUT - 1).len(), 0);
    let rdy = nt
        .node
        .tick()
        .expect("Leader should have Ready after heartbeat timeout");

    assert_eq!(rdy.messages.len() as u32, CLUSTER_SIZE - 1);
    for m in rdy.messages {
        assert_eq!(
            m.body(),
            &MessageBody {
                term: 1,
                variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                    leader_id: 0,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0
                })
            }
        )
    }
}

#[test]
fn all_servers_become_follower_on_msg_with_higher_term() {
    let (mut nt_candidate, _) = NodeTester::new_candidate_node();
    let (mut nt_leader, _) = NodeTester::new_leader_node();

    let msg = Message::new(
        MessageBody {
            term: 3,
            variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                leader_id: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            }),
        },
        MessageMetadata {
            rpc_id: 0,
            from: 1,
            to: 0,
        },
    );

    nt_candidate.node.step(&msg);
    nt_leader.node.step(&msg);
    assert_eq!(nt_candidate.node.state(), NodeState::FOLLOWER);
    assert_eq!(nt_leader.node.state(), NodeState::FOLLOWER);
}

#[test]
fn leader_takes_client_command() {
    let (mut nt, mut rdy) = NodeTester::new_leader_node();
    nt.storage.append_entries(&mut rdy.entries);

    let cmd = "test".as_bytes().to_vec();
    let rdy = nt.node.propose("test".as_bytes().to_vec().clone()).unwrap();

    // Assert messages.
    assert_eq!(rdy.messages.len() as u32, CLUSTER_SIZE - 1);
    for m in rdy.messages {
        assert_eq!(
            m.body(),
            &MessageBody {
                term: 1,
                variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                    leader_id: 0,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![Entry {
                        term: 1,
                        index: 2,
                        noop: false,
                        data: cmd.clone()
                    }],
                    leader_commit: 0
                })
            }
        )
    }

    // Assert entries.
    assert_eq!(rdy.entries.len(), 1);
    assert_eq!(
        rdy.entries[0],
        Entry {
            term: 1,
            index: 2,
            noop: false,
            data: cmd.clone()
        }
    );
}
