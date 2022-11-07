use std::{
    collections::{HashMap, HashSet},
    vec,
};

use crate::config::*;
use crate::message::*;
use crate::storage::*;

pub type NodeId = u32;
pub type Result<T> = std::result::Result<T, RaftError>;

/// `Command` represents the input a client applies to the state machine.
pub type Command = Vec<u8>;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

/// Ready encapsulates the entries that are expected to be saved to stable storage, applied
/// to the state machine, and the messages that are expected to be sent to other peers.
/// Heavily inspired from the etcd Raft implementation.
#[derive(Clone, Default, Debug)]
pub struct Ready {
    /// Messages to be sent to peers.
    pub messages: Vec<Message>,

    /// Entries to be persisted to stable store before messages are sent.
    pub entries: Vec<Entry>,

    /// Entries to be applied to the state machine. Note that these are expected
    /// to have been previously persisted to the log.
    pub committed_entries: Vec<Entry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: u32,
    pub index: u32,
    pub noop: bool,
    pub data: Command,
}

impl Entry {
    fn new_noop(term: u32, index: u32) -> Self {
        Self {
            term,
            index,
            noop: true,
            data: vec![],
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RaftError {
    NotLeader,
    NodeStopped,
    InvalidMessage(&'static str),
}

impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::NotLeader => {
                write!(
                    f,
                    "NotLeader: node must be the leader to process this request"
                )
            }
            Self::NodeStopped => {
                write!(
                    f,
                    "NodeStopped: cannot process anything while node is stopped"
                )
            }
            Self::InvalidMessage(msg) => {
                write!(f, "InvalidMessage: {}", msg)
            }
        }
    }
}

impl std::error::Error for RaftError {}

/// Represents a single node participating in a Raft cluster.
#[derive(Debug)]
pub struct Node<S: Storage> {
    raft: Raft<S>,
    stopped: bool,
}

impl<S: Storage> Node<S> {
    pub fn new(cfg: Config<S>) -> Self {
        // TODO: validate cfg
        Node {
            raft: Raft::new(cfg),
            stopped: false,
        }
    }

    pub fn id(&self) -> NodeId {
        self.raft.id
    }

    pub fn state(&self) -> NodeState {
        self.raft.state
    }

    pub fn tick(&mut self) -> Result<Option<Ready>> {
        self.preprocess(None)?;

        let rdy = match self.raft.state {
            NodeState::FOLLOWER => self.raft.tick_follower(),
            NodeState::CANDIDATE => self.raft.tick_candidate(),
            NodeState::LEADER => self.raft.tick_leader(),
        };
        Ok(self.postprocess(rdy))
    }

    pub fn step(&mut self, m: &Message) -> Result<Option<Ready>> {
        self.preprocess(Some(m))?;

        let rdy = match self.raft.state {
            NodeState::FOLLOWER => self.raft.step_follower(m),
            NodeState::CANDIDATE => self.raft.step_candidate(m),
            NodeState::LEADER => self.raft.step_leader(m),
        };
        Ok(self.postprocess(rdy))
    }

    pub fn propose(&mut self, command: Command) -> Result<Option<Ready>> {
        if self.state() != NodeState::LEADER {
            Err(RaftError::NotLeader)
        } else {
            Ok(Some(self.raft.propose(command)))
        }
    }

    /// TODO: Should prevent sending new `Ready`'s until advance is called.
    /// This is to ensure the validity of internal state (e.g. commitIndex)
    /// in the event that the client fails to persist log entries.
    pub fn advance(&mut self) {
        unimplemented!()
    }

    /// Calling `stop` will return a `RaftError::NodeStopped` error on any
    /// calls to `tick` or `step`.
    pub fn stop(&mut self) -> () {
        self.stopped = true;
    }

    fn preprocess(&mut self, msg: Option<&Message>) -> Result<()> {
        if self.stopped {
            return Err(RaftError::NodeStopped);
        } else if let Some(msg) = msg {
            // 1. Validate message
            if msg.from() == self.id() {
                return Err(RaftError::InvalidMessage(
                    "node cannot process its own message",
                ));
            }
            // From the Raft paper: if RPC request or response contains term
            // T > currentTerm: set currentTerm = T, convert to follower.
            if msg.term() > self.raft.current_term {
                self.raft.become_follower(msg.term());
            }
        }
        Ok(())
    }

    fn postprocess(&mut self, rdy: Option<Ready>) -> Option<Ready> {
        let mut committed_entry: Option<Entry> = None;
        while self.raft.commit_index > self.raft.last_applied {
            self.raft.last_applied += 1;
            let entry = self.raft.storage.get(self.raft.last_applied);
            if !entry.noop {
                committed_entry = Some(entry);
                break;
            }
        }

        // If we have a new committed entry, will make a new Ready if the provided
        // arg is None.
        if let Some(ce) = committed_entry {
            if let Some(mut inner_rdy) = rdy {
                inner_rdy.committed_entries.push(ce);
                Some(inner_rdy)
            } else {
                self.raft.next_committed_entries.push(ce);
                Some(self.raft.return_ready())
            }
        } else {
            rdy
        }
    }
}

#[derive(Debug)]
struct Raft<S: Storage> {
    id: NodeId,
    state: NodeState,
    peers: Vec<NodeId>,
    msg_store: MsgStore,

    // State to be included in the next Ready.
    next_messages: Vec<Message>,
    next_entries: Vec<Entry>,
    next_committed_entries: Vec<Entry>,

    // Persistent state on all servers (updated on stable storage before responding to RPCs).
    current_term: u32,
    voted_for: Option<NodeId>,
    storage: S,

    // Persistent state for candidates.
    election: Option<Election>,

    // Volatile state on all servers.
    commit_index: u32,
    last_applied: u32,

    // Volatile state specific to leaders.
    next_index: HashMap<NodeId, u32>,
    match_index: HashMap<NodeId, u32>,

    // Cluster behavior determined from Config.
    default_election_timeout: u32,
    election_tick_rand: u32,
    heartbeat_timeout: u32,

    actual_election_timeout: u32,
    election_ticks_elapsed: u32,
    heartbeat_ticks_elapsed: u32,
}

impl<S: Storage> Raft<S> {
    fn new(cfg: Config<S>) -> Self {
        Self {
            id: cfg.id,
            state: NodeState::FOLLOWER,
            peers: cfg.peers,
            storage: cfg.storage,
            msg_store: MsgStore::new(),

            next_messages: vec![],
            next_entries: vec![],
            next_committed_entries: vec![],

            current_term: 0,
            voted_for: None,
            election: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),

            default_election_timeout: cfg.election_tick_timeout,
            election_tick_rand: cfg.election_tick_rand,
            heartbeat_timeout: cfg.heartbeat_tick_timeout,

            // Want the first election timeout cycle to have the same behavior as future cycles.
            // TODO: Clean up the instantiation of Raft
            actual_election_timeout: cfg.election_tick_timeout
                + Raft::<S>::rand_nonnegative(cfg.election_tick_rand),
            election_ticks_elapsed: 0,
            heartbeat_ticks_elapsed: 0,
        }
    }

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
        self.heartbeat_ticks_elapsed += 1;
        if self.heartbeat_timed_out() {
            self.reset_heartbeat_timer();
            Some(self.send_heartbeat(false))
        } else {
            None
        }
    }

    fn step_follower(&mut self, m: &Message) -> Option<Ready> {
        match m.body().variant {
            MessageRPC::AppendEntries(ref args) => {
                if m.term() < self.current_term {
                    self.make_response(m, MessageRPC::AppendEntriesResp(false));
                    return Some(self.return_ready());
                }

                // Handle the false check; namely, if the message is from a previous term
                // or we fail the log consistency check.
                if m.term() < self.current_term || !self.check_log_consistency(args) {
                    self.make_response(m, MessageRPC::AppendEntriesResp(false));
                    return Some(self.return_ready());
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
                self.next_entries = entries;
                self.make_response(m, MessageRPC::AppendEntriesResp(true));
                return Some(self.return_ready());
            }

            MessageRPC::RequestVote(ref args) => return Some(self.handle_request_vote(m, args)),

            // Follower doesn't care about response messages.
            _ => None,
        }
    }

    fn step_candidate(&mut self, m: &Message) -> Option<Ready> {
        match m.body().variant {
            MessageRPC::AppendEntries(ref args) => Some(self.handle_append_entries(&m, &args)),

            MessageRPC::RequestVote(ref args) => {
                // Handle RequestVote same as follower.
                Some(self.handle_request_vote(&m, args))
            }

            MessageRPC::RequestVoteResp(vote_granted) => {
                // If response is not for a RequestVote RPC sent in this current election, then ignore.
                if !self.msg_store.matches(&m) {
                    return None;
                }

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

    fn step_leader(&mut self, m: &Message) -> Option<Ready> {
        match m.body().variant {
            MessageRPC::AppendEntriesResp(success) => {
                let peer_id = m.from();
                let sent_msg = self.msg_store.get(peer_id, m.metadata().rpc_id)?;
                if let MessageRPC::AppendEntries(sent_args) = &sent_msg.body().variant {
                    // If follower didn't have an entry matching prevLogIndex and prevLogTerm, decrement
                    // its next_index and retry.
                    if !success {
                        let curr_next_index = *self
                            .next_index
                            .get(&peer_id)
                            .expect("Should have next_index for all peers");
                        self.next_index.insert(peer_id, curr_next_index - 1);
                        self.make_append_entries(&[peer_id], None, false);
                        Some(self.return_ready())
                    }
                    // Otherwise, update next_index and match_index, then check for quorum to
                    // update commit index.
                    //
                    // Taken from the Raft paper: If there exists an N such that N > commitIndex, a
                    // majority of matchIndex[i] >= N, and log[N].term == currentTerm: set
                    // commitIndex = N.
                    else if sent_args.entries.len() > 0 {
                        let last_index = sent_args.entries.last().unwrap().index;
                        self.next_index.insert(peer_id, last_index + 1);
                        self.match_index.insert(peer_id, last_index);
                        if self.has_quorum(last_index) {
                            self.commit_index = std::cmp::max(self.commit_index, last_index);
                        }
                        None
                    }
                    // Do nothing on heartbeat success.
                    else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn election_timed_out(&self) -> bool {
        self.election_ticks_elapsed >= self.actual_election_timeout
    }

    fn heartbeat_timed_out(&self) -> bool {
        self.heartbeat_ticks_elapsed >= self.heartbeat_timeout
    }

    /// Utility function for generating a random number in the range [0, max]
    ///
    /// Should this be in Raft, or separate utility module?
    fn rand_nonnegative(max: u32) -> u32 {
        if max == 0 {
            0
        } else {
            rand::random::<u32>() % (max - 1)
        }
    }

    fn reset_election_timer(&mut self) {
        let range = Raft::<S>::rand_nonnegative(self.election_tick_rand);
        self.actual_election_timeout = self.default_election_timeout + range;
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

    fn broadcast(&mut self, variant: MessageRPC) -> Vec<Message> {
        self.peers
            .iter()
            .map(|to| {
                let mut msg = Message::new(
                    MessageBody {
                        term: self.current_term,
                        variant: variant.clone(),
                    },
                    MessageMetadata {
                        from: self.id,
                        to: *to,
                        ..Default::default()
                    },
                );
                self.msg_store.insert(self.current_term, &mut msg);
                msg
            })
            .collect::<Vec<_>>()
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

    fn make_response(&mut self, m: &Message, variant: MessageRPC) {
        self.next_messages.push(Message::new(
            MessageBody {
                term: self.current_term,
                variant,
            },
            MessageMetadata {
                rpc_id: m.metadata().rpc_id,
                from: self.id,
                to: m.from(),
            },
        ));
    }

    /// AppendEntries RPC receiver behavior is almost entirely equivalent for followers
    /// and candidates (extra logic for a candidate becoming a follower is added). Thus,
    /// shared implementation extracted into `handle_append_entries`.
    fn handle_append_entries(&mut self, m: &Message, args: &AppendEntriesArgs) -> Ready {
        if m.term() < self.current_term {
            self.make_response(m, MessageRPC::AppendEntriesResp(false));
            return self.return_ready();
        }

        if self.state != NodeState::FOLLOWER {
            self.become_follower(m.term());
        }

        // Handle the false check; namely, if the message is from a previous term
        // or we fail the log consistency check.
        if m.term() < self.current_term || !self.check_log_consistency(args) {
            self.make_response(m, MessageRPC::AppendEntriesResp(false));
            return self.return_ready();
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
        self.next_entries = entries;
        self.make_response(m, MessageRPC::AppendEntriesResp(true));
        return self.return_ready();
    }

    /// RequestVote RPC behavior is equivalent for both followers and candidates, so shared
    /// implementation has been extracted into `handle_request_vote`.
    ///
    /// Algorithm from the Raft paper:
    /// 1. Reply false if term < currentTerm
    /// 2. If votedFor is null or candidateId, and candidate's log is at least up-to-date
    ///    as receiver's log, grant vote.
    fn handle_request_vote(&mut self, m: &Message, args: &RequestVoteArgs) -> Ready {
        if m.term() < self.current_term {
            self.make_response(&m, MessageRPC::RequestVoteResp(false));
            return self.return_ready();
        }

        let vote_granted = if self.voted_for.is_none() {
            true
        } else {
            // Check if the log is at least up-to-date as this node's log.
            // Specifically, check if candidate's lastLogTerm is >, or if
            // equal, lastLogIndex is >=
            let last_term = self.storage.last_term();
            if args.last_log_term > last_term
                || args.last_log_term == last_term
                    && args.last_log_index >= self.storage.last_index()
            {
                true
            } else {
                false
            }
        };

        self.make_response(m, MessageRPC::RequestVoteResp(vote_granted));
        self.return_ready()
    }

    fn become_leader(&mut self) -> Ready {
        self.state = NodeState::LEADER;
        self.reset_heartbeat_timer();

        // Need to initialize next_index and match_index.
        self.next_index.clear();
        self.match_index.clear();
        let next_index = self.storage.last_index() + 1;
        for peer in &self.peers {
            self.next_index.insert(*peer, next_index);
            self.match_index.insert(*peer, 0);
        }

        self.send_heartbeat(true)
    }

    fn make_append_entries(
        &mut self,
        peers: &[NodeId],
        new_entry: Option<&Entry>,
        include_noop: bool,
    ) {
        for to in peers {
            let prev_log_index = self.next_index.get(to).expect("Impossible") - 1;
            let prev_log_term = self.storage.term(prev_log_index).expect("Impossible");
            let next_index = self.next_index.get(to).expect("Impossible");
            let mut entries = self.storage.entries(*next_index, self.storage.last_index());
            if let Some(ne) = new_entry {
                entries.push(ne.clone());
            }
            if include_noop {
                entries.push(Entry::new_noop(
                    self.current_term,
                    self.storage.last_index() + 1,
                ));
            }
            let mut msg = Message::new(
                MessageBody {
                    term: self.current_term,
                    variant: MessageRPC::AppendEntries(AppendEntriesArgs {
                        leader_id: self.id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: self.commit_index,
                    }),
                },
                MessageMetadata {
                    from: self.id,
                    to: *to,
                    ..Default::default()
                },
            );
            self.msg_store.insert(self.current_term, &mut msg);
            self.next_messages.push(msg);
        }
    }

    fn send_heartbeat(&mut self, include_noop: bool) -> Ready {
        let peers = &self.peers;
        // TODO: Fix this clone.
        self.make_append_entries(&peers.clone(), None, include_noop);

        if include_noop {
            self.next_entries.push(Entry::new_noop(
                self.current_term,
                self.storage.last_index() + 1,
            ));
        }

        self.return_ready()
    }

    fn return_ready(&mut self) -> Ready {
        let mut messages = vec![];
        std::mem::swap(&mut messages, &mut self.next_messages);
        let mut entries = vec![];
        std::mem::swap(&mut entries, &mut self.next_entries);
        let mut committed_entries = vec![];
        std::mem::swap(&mut committed_entries, &mut self.next_committed_entries);
        Ready {
            messages,
            entries,
            committed_entries,
        }
    }

    fn propose(&mut self, command: Command) -> Ready {
        // 1. Append command to the log
        let entry = Entry {
            term: self.current_term,
            index: self.storage.last_index() + 1,
            noop: false,
            data: command,
        };
        self.next_entries.push(entry.clone());
        // 2. Broadcast to peers.
        self.make_append_entries(&self.peers.clone(), Some(&entry), false);
        self.return_ready()
    }

    fn has_quorum(&self, index: u32) -> bool {
        // Remember that node always votes for itself.
        let quorum = self.peers.len() / 2;
        let cnt = self
            .peers
            .iter()
            .filter(|id| self.match_index.get(id).unwrap() >= &index)
            .count();
        cnt >= quorum
    }
}

#[derive(Debug)]
struct MsgStore {
    /// Term for which this message store is valid.
    term: u32,

    /// Map of peer id's to rpc_id's.
    sent: HashMap<(NodeId, u32), Message>,
}

impl MsgStore {
    fn new() -> Self {
        Self {
            term: 0,
            sent: HashMap::new(),
        }
    }

    fn get(&self, node_id: NodeId, rpc_id: u32) -> Option<&Message> {
        self.sent.get(&(node_id, rpc_id))
    }

    fn insert(&mut self, term: u32, msg: &mut Message) -> u32 {
        if term > self.term {
            self.term = term;
            self.sent.clear();
        }
        // TODO: Use better rpc_id generation. For now, just use a random id.
        let rpc_id = rand::random();
        msg.set_rpc_id(rpc_id);
        self.sent.insert((msg.to(), rpc_id), msg.clone());
        rpc_id
    }

    fn matches(&self, msg: &Message) -> bool {
        if let Some(sent) = self.sent.get(&(msg.from(), msg.metadata().rpc_id)) {
            sent.metadata().rpc_id == msg.metadata().rpc_id
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct Election {
    // is term required?
    // term: u32,
    responded_peers: HashSet<NodeId>,
    votes_received: u32,
    quorum: u32,
}

impl Election {
    fn new<S: Storage>(raft: &Raft<S>) -> Self {
        Self {
            // term: raft.current_term,
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
