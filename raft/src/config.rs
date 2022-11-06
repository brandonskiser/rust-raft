use crate::node::*;
use crate::storage::*;

pub struct Config<S: Storage> {
    pub id: NodeId,
    pub peers: Vec<NodeId>,

    pub storage: S,

    /// The number of ticks that must pass until a node starts an election.
    pub election_tick_timeout: u32,

    /// The highest number that will be randomly added to election_tick_timeout per
    /// timeout cycle. Defaults to election_tick_timeout - 1, thus the actual election_tick_timeout
    /// will be a value in the range [election_tick_timeout, 2 * election_tick_timeout - 1]
    pub election_tick_rand: u32,

    /// The number of ticks that must pass until a leader sends a heartbeat message. It is
    /// suggested to set election_tick_timeout = 10 * heartbeat_tick_timeout to prevent
    /// unnecessary leader switching.
    pub heartbeat_tick_timeout: u32,
}

impl<S: Storage> Config<S> {
    pub fn new_with_defaults(id: NodeId, peers: Vec<NodeId>, storage: S) -> Self {
        Self {
            id,
            peers,
            storage,
            election_tick_timeout: 10,
            election_tick_rand: 9,
            heartbeat_tick_timeout: 1,
        }
    }
}
