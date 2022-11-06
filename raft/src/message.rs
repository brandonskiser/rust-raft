use crate::node::{Entry, NodeId};

#[derive(Clone, Debug)]
pub struct Message {
    body: MessageBody,
    metadata: MessageMetadata,
}

impl Message {
    pub fn new(body: MessageBody, metadata: MessageMetadata) -> Self {
        Self { body, metadata }
    }

    pub fn body(&self) -> &MessageBody {
        &self.body
    }

    pub fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    pub fn set_rpc_id(&mut self, rpc_id: u32) {
        self.metadata.rpc_id = rpc_id;
    }

    pub fn from(&self) -> NodeId {
        self.metadata.from
    }

    pub fn to(&self) -> NodeId {
        self.metadata.to
    }

    pub fn term(&self) -> u32 {
        self.body.term
    }

    pub fn is_append_entries(&self) -> bool {
        match self.body.variant {
           MessageRPC::AppendEntries(_) => true,
           _ => false
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageMetadata {
    pub rpc_id: u32,
    pub from: NodeId,
    pub to: NodeId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageBody {
    pub term: u32,
    pub variant: MessageRPC,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageRPC {
    AppendEntries(AppendEntriesArgs),
    AppendEntriesResp(bool),
    RequestVote(RequestVoteArgs),
    RequestVoteResp(bool),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendEntriesArgs {
    pub leader_id: NodeId,
    pub prev_log_index: u32,
    pub prev_log_term: u32,
    pub entries: Vec<Entry>,
    pub leader_commit: u32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RequestVoteArgs {
    pub candidate_id: NodeId,
    pub last_log_index: u32,
    pub last_log_term: u32,
}
