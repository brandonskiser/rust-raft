pub mod message;
pub mod storage;

use std::collections::{HashMap, HashSet};

use message::*;
use storage::*;

pub struct Config {
    pub id: NodeId,
    pub peers: Vec<NodeId>,

    pub storage: Box<dyn Storage>,

    /// The number of ticks that must pass until a node starts an election.
    pub election_tick_timeout: u32,

    /// TODO: decide on this name/behavior. This should probably default to a number
    /// [0, electionTickTimeout - 1] so that the actual election timeout will be
    /// [electionTickTimeout, 2 * electionTickTimeout - 1] like in etcd Raft.
    pub election_tick_rand: Option<(u32, u32)>,

    /// The number of ticks that must pass until a leader sends a heartbeat message.
    pub heartbeat_tick_timeout: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            id: 0,
            peers: vec![],
            storage: Box::new(MemoryStorage::new()),
            election_tick_timeout: 10,
            election_tick_rand: None,
            heartbeat_tick_timeout: 1,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

struct MsgStore {
    term: u32,

    /// Map
    sent: HashMap<NodeId, u32>,
}

impl MsgStore {
    fn insert(&mut self, term: u32, msg: &Message) {
        if term > self.term {
            self.sent.clear();
        }
        self.sent.insert(msg.to(), msg.metadata().rpc_id);
    }

    fn matches(&self, term: u32, msg: &Message) -> bool {
        if let Some(rpc_id) = self.sent.get(&msg.from()) {
            *rpc_id == msg.metadata().rpc_id
        } else {
            false
        }
    }
}

/// Ready encapsulates the entries that are expected to be saved to stable storage, applied
/// to the state machine, and the messages that are expected to be sent to other peers.
/// Heavily inspired from the etcd Raft implementation.
#[derive(Clone, Default, Debug)]
pub struct Ready {
    /// Messages to be sent to peers.
    pub messages: Vec<Message>,

    /// Entries to be persisted to stable store.
    pub entries: Vec<Entry>,

    /// Entries to be applied to the state machine. Note that these are expected
    /// to have been previously persisted to the log.
    pub committed_entries: Vec<Entry>,
}

impl Ready {
    fn builder() -> ReadyBuilder {
        ReadyBuilder::new()
    }
}

struct ReadyBuilder {
    messages: Vec<Message>,
    entries: Vec<Entry>,
    committed_entries: Vec<Entry>,
}

impl ReadyBuilder {
    fn new() -> Self {
        Self {
            messages: vec![],
            entries: vec![],
            committed_entries: vec![],
        }
    }

    fn with_messages(mut self, m: Vec<Message>) -> Self {
        self.messages = m;
        self
    }

    fn with_response(mut self, raft: &Raft, m: &Message, resp: MessageRPC) -> Self {
        self.messages.push(Message::new(
            MessageBody {
                term: raft.current_term,
                variant: resp,
            },
            MessageMetadata {
                rpc_id: 0,
                from: raft.id,
                to: m.from(),
            },
        ));
        self
    }

    fn with_entries(mut self, entries: Vec<Entry>) -> Self {
        self.entries = entries;
        self
    }

    fn build(self) -> Ready {
        Ready {
            messages: self.messages,
            entries: self.entries,
            committed_entries: self.committed_entries,
        }
    }
}

pub type NodeId = u32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u32,
    pub index: u32,
    pub data: Vec<u8>,
}

struct Election {
    term: u32,
    responded_peers: HashSet<NodeId>,
    votes_received: u32,
    quorum: u32,
}

impl Election {
    fn new(raft: &Raft) -> Self {
        Self {
            term: raft.current_term,
            // Node always votes for itself.
            votes_received: 1,
            responded_peers: HashSet::<NodeId>::with_capacity(raft.peers.len()),
            quorum: raft.peers.len() as u32 / 2 + 1,
        }
    }

    fn has_quorum(&self) -> bool {
        self.votes_received >= self.quorum
    }

    fn apply_response(&mut self, peer: NodeId, vote_granted: bool) {
        if self.responded_peers.insert(peer) && vote_granted {
            self.votes_received += 1;
        }
    }
}

/// Represents a single node participating in a Raft cluster.
pub struct Node {
    raft: Raft,
    // msg_store: MsgStore,
}

impl Node {
    pub fn new(cfg: Config) -> Self {
        // TODO: validate cfg
        let raft = Raft {
            id: cfg.id,
            state: NodeState::FOLLOWER,
            peers: cfg.peers,
            storage: cfg.storage,

            current_term: 0,
            voted_for: None,
            election: None,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![],
            match_index: vec![],

            default_election_timeout: cfg.election_tick_timeout,
            election_tick_rand: cfg
                .election_tick_rand
                .unwrap_or((0, cfg.election_tick_timeout - 1)),
            heartbeat_timeout: cfg.heartbeat_tick_timeout,

            election_ticks_elapsed: 0,
            heartbeat_ticks_elapsed: 0,
        };

        Node { raft }
    }

    pub fn id(&self) -> NodeId {
        self.raft.id
    }

    pub fn state(&self) -> NodeState {
        self.raft.state
    }

    pub fn tick(&mut self) -> Option<Ready> {
        match self.raft.state {
            NodeState::FOLLOWER => self.raft.tick_follower(),
            NodeState::CANDIDATE => self.raft.tick_candidate(),
            NodeState::LEADER => self.raft.tick_leader(),
        }
    }

    pub fn step(&mut self, m: &Message) -> Option<Ready> {
        match self.raft.state {
            NodeState::FOLLOWER => self.raft.step_follower(m),
            NodeState::CANDIDATE => self.raft.step_candidate(m),
            NodeState::LEADER => self.raft.step_leader(m),
        }
    }

    pub fn send(&self) -> () {
        unimplemented!()
    }

    pub fn stop(&self) -> () {
        unimplemented!()
    }
}

struct Raft {
    id: NodeId,
    state: NodeState,
    peers: Vec<NodeId>,

    // Persistent state on all servers (updated on stable storage before responding to RPCs).
    current_term: u32,
    voted_for: Option<NodeId>,
    storage: Box<dyn Storage>,

    // Persistent state for candidates.
    election: Option<Election>,

    // Volatile state on all servers.
    commit_index: u32,
    last_applied: u32,

    // Volatile state specific to leaders.
    next_index: Vec<u32>,
    match_index: Vec<u32>,

    // Cluster behavior determined from Config.
    default_election_timeout: u32,
    election_tick_rand: (u32, u32),
    heartbeat_timeout: u32,

    election_ticks_elapsed: u32,
    heartbeat_ticks_elapsed: u32,
}

impl Raft {
    fn tick_follower(&mut self) -> Option<Ready> {
        self.election_ticks_elapsed += 1;
        if self.election_timed_out() {
            Some(self.start_election())
        } else {
            None
        }
    }

    fn tick_candidate(&mut self) -> Option<Ready> {
        self.election_ticks_elapsed += 1;
        if self.election_timed_out() {
            Some(self.start_election())
        } else {
            None
        }
    }

    fn tick_leader(&mut self) -> Option<Ready> {
        if self.heartbeat_timed_out() {
            self.reset_heartbeat_timer();
            Some(self.send_heartbeat())
        } else {
            None
        }
    }

    fn election_timed_out(&self) -> bool {
        self.election_ticks_elapsed >= self.default_election_timeout
    }

    fn heartbeat_timed_out(&self) -> bool {
        self.heartbeat_ticks_elapsed >= self.heartbeat_timeout
    }

    fn reset_election_timer(&mut self) {
        self.election_ticks_elapsed = 0;
    }

    fn reset_heartbeat_timer(&mut self) {
        self.heartbeat_ticks_elapsed = 0;
    }

    fn become_follower(&mut self, term: u32) {
        self.current_term = term;
        self.reset_election_timer();
        self.state = NodeState::FOLLOWER
    }

    fn start_election(&mut self) -> Ready {
        if self.state == NodeState::LEADER {
            panic!("Cannot start election from the leader state");
        }

        self.state = NodeState::CANDIDATE;
        self.reset_election_timer();
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.election = Some(Election::new(&self));

        let messages = self.broadcast(MessageRPC::RequestVote(RequestVoteArgs {
            candidate_id: self.id,
            last_log_index: 0,
            last_log_term: 0,
        }));
        Ready {
            messages,
            ..Default::default()
        }
    }

    fn broadcast(&self, variant: MessageRPC) -> Vec<Message> {
        self.peers
            .iter()
            .map(|to| {
                Message::new(
                    MessageBody {
                        term: self.current_term,
                        variant: variant.clone(),
                    },
                    MessageMetadata {
                        rpc_id: 0,
                        from: self.id,
                        to: *to,
                    },
                )
            })
            .collect::<Vec<_>>()
    }

    /// If the message's term is greater than this node's current_term,
    /// update current_term, convert to follower, and return true.
    /// Else, return false.
    fn check_incoming_term(&mut self, m: &Message) -> bool {
        if m.term() > self.current_term {
            self.become_follower(m.term());
            true
        } else {
            false
        }
    }

    fn check_log_consistency(&self, ae: &AppendEntriesArgs) -> bool {
        if ae.prev_log_index == 0 {
            true
        } else if let Some(i) = self.storage.term(ae.prev_log_index) {
            i != ae.prev_log_term
        } else {
            false
        }
    }

    fn handle_request_vote(&mut self, m: &Message, args: &RequestVoteArgs) -> Option<Ready> {
        if m.term() < self.current_term {
            return Some(
                Ready::builder()
                    .with_response(&self, m, MessageRPC::RequestVoteResp(false))
                    .build(),
            );
        }

        let vote_granted = if self.voted_for.is_none() {
            true
        } else {
            // Check if the log is at least up-to-date as this node's log.
            // Specifically, check if candidate's lastLogTerm is >, or if
            // equal, lastLogIndex is >=
            let last_term = self
                .storage
                .term(self.storage.last_index())
                .expect("Always have a log entry");
            if args.last_log_term > last_term
                || args.last_log_term == last_term
                    && args.last_log_index >= self.storage.last_index()
            {
                true
            } else {
                false
            }
        };

        Some(
            Ready::builder()
                .with_response(&self, &m, MessageRPC::RequestVoteResp(vote_granted))
                .build(),
        )
    }

    fn step_follower(&mut self, m: &Message) -> Option<Ready> {
        match m.body().variant {
            MessageRPC::AppendEntries(ref args) => {
                if m.term() < self.current_term {
                    return Some(
                        Ready::builder()
                            .with_response(&self, m, MessageRPC::AppendEntriesResp(false))
                            .build(),
                    );
                }

                // Handle the false check; namely, if the message is from a previous term
                // or we fail the log consistency check.
                if m.term() < self.current_term || !self.check_log_consistency(args) {
                    return Some(
                        Ready::builder()
                            .with_response(&self, &m, MessageRPC::AppendEntriesResp(false))
                            .build(),
                    );
                }

                // Find the first index that conflicts with a new one (same index, different terms),
                // replacing it and all that follows with the new entries.
                let mut entries: Vec<Entry> = vec![];
                if args.entries.len() > 0 {
                    let mut start_i = args.entries.first().expect("Impossible").index as usize;
                    for (arg_i, entry) in args.entries.iter().enumerate() {
                        match self.storage.term(arg_i as u32) {
                            Some(term) => {
                                if term != entry.term {
                                    start_i = arg_i;
                                    break;
                                }
                            }
                            _ => {
                                start_i = arg_i;
                                break;
                            }
                        }
                    }
                    entries = args.entries[start_i..].to_vec();
                }

                self.reset_election_timer();
                Some(
                    Ready::builder()
                        .with_response(&self, &m, MessageRPC::AppendEntriesResp(true))
                        .with_entries(entries)
                        .build(),
                )
            }

            MessageRPC::RequestVote(ref args) => return self.handle_request_vote(m, args),

            // Follower doesn't care about response messages.
            _ => None,
        }
    }

    fn step_candidate(&mut self, m: &Message) -> Option<Ready> {
        match m.body().variant {
            MessageRPC::AppendEntries(ref args) => {
                // Duplication with step_follower
                if m.body().term < self.current_term {
                    return Some(
                        Ready::builder()
                            .with_response(&self, m, MessageRPC::AppendEntriesResp(false))
                            .build(),
                    );
                }

                // Convert to follower.
                self.become_follower(m.body().term);
                // Handle AppendEntries same as follower.
                None
            }

            MessageRPC::RequestVote(ref args) => {
                // Handle RequestVote same as follower.
                self.handle_request_vote(&m, args)
            }

            MessageRPC::RequestVoteResp(vote_granted) => {
                // If response is not for a RequestVote RPC sent in this current election, then ignore.
                // if !self.msg_store.matches(self.current_term, &m) {
                //     return None;
                // }

                // Otherwise, handle.
                let election = self.election.get_or_insert(Election::new(&self));
                election.apply_response(m.from(), vote_granted);
                if election.has_quorum() {
                    Some(self.become_leader())
                } else {
                    None
                }
            }

            // Don't care about AppendEntriesResp
            _ => None,
        }
    }

    fn become_leader(&mut self) -> Ready {
        self.state = NodeState::LEADER;
        self.reset_heartbeat_timer();
        self.send_heartbeat()
    }

    fn send_heartbeat(&self) -> Ready {
        Ready::builder()
            .with_messages(self.broadcast(MessageRPC::AppendEntries(AppendEntriesArgs {
                leader_id: self.id,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: self.commit_index,
            })))
            .build()
    }

    fn step_leader(&mut self, m: &Message) -> Option<Ready> {
        unimplemented!()
    }
}
