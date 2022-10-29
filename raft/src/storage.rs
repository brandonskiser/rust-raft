use crate::Entry;

pub trait Storage {
    /// Returns the term of the entry at the given index, returning `None` if no
    /// entry exists.
    fn term(&self, index: u32) -> Option<u32>;

    fn get(&self, index: u32) -> Entry;

    fn entries(&self, lo: u32, hi: u32) -> Vec<Entry>;

    fn last_index(&self) -> u32;
}

pub struct MemoryStorage {
    log: Vec<Entry>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            log: vec![Entry {
                term: 0,
                index: 0,
                data: vec![],
            }],
        }
    }
}

impl Storage for MemoryStorage {
    fn term(&self, index: u32) -> Option<u32> {
        if let Some(e) = self.log.get(index as usize) {
            Some(e.term)
        } else {
            None
        }
    }

    fn get(&self, index: u32) -> Entry {
        self.log[index as usize].clone()
    }

    fn entries(&self, lo: u32, hi: u32) -> Vec<Entry> {
        unimplemented!()
    }

    fn last_index(&self) -> u32 {
        self.log.len() as u32 - 1
    }
}
