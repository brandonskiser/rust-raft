use raft::*;

const CLUSTER_SIZE: u32 = 3;
const ELECTION_TIMEOUT: u32 = 10;
const HEARTBEAT_TIMEOUT: u32 = 2;

fn test_cfg() -> Config {
    Config {
        peers: (1..CLUSTER_SIZE).collect(),
        election_tick_timeout: ELECTION_TIMEOUT,
        election_tick_rand: Some((0, 0)),
        heartbeat_tick_timeout: HEARTBEAT_TIMEOUT,
        ..Default::default()
    }
}

fn tick_for(node: &mut Node, times: u32) -> Vec<Ready> {
    let mut rdys: Vec<Option<Ready>> = vec![];
    for _ in 0..times {
        rdys.push(node.tick());
    }
    rdys.into_iter().filter_map(|v| v).collect()
}

fn make_initial_node() -> Node {
    Node::new(test_cfg())
}

fn make_candidate_node() -> Node {
    let mut n = make_initial_node();
    tick_for(&mut n, ELECTION_TIMEOUT);
    n
}

fn assert_election_started(node: &Node, rdy: &Ready, term: u32) {
    assert_eq!(node.state(), NodeState::CANDIDATE);
    assert_eq!(rdy.messages.len(), 2);
    for (i, m) in rdy.messages.iter().enumerate() {
        assert_eq!(
            m,
            &Message {
                from: 0,
                to: i as u32 + 1,
                term,
                variant: MessageRPC::RequestVote(RequestVoteArgs {
                    candidate_id: 0,
                    last_log_index: 0,
                    last_log_term: 0
                })
            }
        );
    }
}

fn assert_became_leader(node: &Node, rdy: &Ready, term: u32) {
    assert_eq!(node.state(), NodeState::LEADER);
    assert_eq!(rdy.messages.len(), 2);
    for (i, m) in rdy.messages.iter().enumerate() {
        assert_eq!(
            m,
            &Message {
                from: 0,
                to: i as u32 + 1,
                term,
                variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                    leader_id: 0,
                    // TODO: Update this config for other test cases.
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
fn node_init_state() {
    let node = make_initial_node();
    assert_eq!(node.id(), 0);
    assert_eq!(node.state(), NodeState::FOLLOWER);
}

#[test]
fn new_follower_starts_election_after_timeout() {
    let mut node = make_initial_node();

    assert_eq!(tick_for(&mut node, ELECTION_TIMEOUT - 1).len(), 0);
    let rdy = node.tick().expect("Should have Ready after timeout");

    assert_election_started(&node, &rdy, 1);
}

#[test]
fn new_candidate_starts_election_after_timeout() {
    let mut node = make_candidate_node();

    assert_eq!(tick_for(&mut node, ELECTION_TIMEOUT - 1).len(), 0);
    let rdy = node.tick().expect("Should have Ready after timeout");

    assert_election_started(&node, &rdy, 2);
}

#[test]
fn follower_with_no_logs_responds_to_heartbeat() {
    let mut node = make_initial_node();

    let heartbeat = Message {
        from: 1,
        to: 0,
        term: 0,
        variant: MessageRPC::AppendEntries(AppendEntriesArgs {
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        }),
    };

    let rdy = node
        .step(&heartbeat)
        .expect("Should have Ready with AppendEntriesResp");

    assert_eq!(rdy.messages.len(), 1);
    assert_eq!(
        rdy.messages[0],
        Message {
            from: 0,
            to: 1,
            term: 0,
            variant: MessageRPC::AppendEntriesResp(true)
        }
    );
}

#[test]
fn follower_with_no_logs_votes_for_candidate() {
    let mut node = make_initial_node();

    let reqvote = Message {
        from: 1,
        to: 0,
        term: 1,
        variant: MessageRPC::RequestVote(RequestVoteArgs {
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        }),
    };
    let rdy = node
        .step(&reqvote)
        .expect("Should have Ready with RequestVoteResp");

    assert_eq!(rdy.messages.len(), 1);
    assert_eq!(
        rdy.messages[0],
        Message {
            from: 0,
            to: 1,
            term: 0,
            variant: MessageRPC::RequestVoteResp(true)
        }
    );
}

#[test]
fn candidate_with_quorum_becomes_leader() {
    let mut node = make_candidate_node();

    // Send enough RequestVoteResp(true) to trigger the quorum condition.
    // Note that the node already votes for itself.
    let quorum = CLUSTER_SIZE / 2 + 1;
    let msgs = (1..quorum)
        .map(|peer| Message {
            from: peer,
            to: 0,
            term: 0,
            variant: MessageRPC::RequestVoteResp(true),
        })
        .collect::<Vec<_>>();
    for m in msgs[0..msgs.len() - 1].iter() {
        node.step(m);
    }
    let rdy = node
        .step(&msgs[msgs.len() - 1])
        .expect("Should have Ready after quorum");

    assert_became_leader(&node, &rdy, 1);
}
