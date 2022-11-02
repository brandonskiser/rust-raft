use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use crate::Entry;

pub trait Storage: Debug {
    /// Returns the term of the entry at the given index, returning `None` if no
    /// entry exists.
    fn term(&self, index: u32) -> Option<u32>;

    fn get(&self, index: u32) -> Entry;

    fn entries(&self, lo: u32, hi: u32) -> Vec<Entry>;

    fn last_index(&self) -> u32;

    fn last_term(&self) -> u32;
}

#[derive(Debug)]
pub struct MemoryStorage {
    log: RefCell<Vec<Entry>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            log: RefCell::new(vec![Entry {
                term: 0,
                index: 0,
                noop: true,
                data: vec![],
            }]),
        }
    }
}

impl MemoryStorage {
    pub fn append_entry(&self, entry: Entry) {
        self.log.borrow_mut().push(entry);
    }

    pub fn append_entries(&self, entries: &mut Vec<Entry>) {
        self.log.borrow_mut().append(entries);
    }
}

impl Storage for MemoryStorage {
    fn term(&self, index: u32) -> Option<u32> {
        if let Some(e) = self.log.borrow().get(index as usize) {
            Some(e.term)
        } else {
            None
        }
    }

    fn get(&self, index: u32) -> Entry {
        self.log.borrow()[index as usize].clone()
    }

    fn entries(&self, lo: u32, hi: u32) -> Vec<Entry> {
        unimplemented!()
    }

    fn last_index(&self) -> u32 {
        self.log.borrow().len() as u32 - 1
    }

    fn last_term(&self) -> u32 {
        self.log.borrow()[self.last_index() as usize].term
    }
}

impl<T: Storage> Storage for Rc<T> {
    fn term(&self, index: u32) -> Option<u32> {
        (**self).term(index)
    }

    fn get(&self, index: u32) -> Entry {
        (**self).get(index)
    }

    fn entries(&self, lo: u32, hi: u32) -> Vec<Entry> {
        unimplemented!()
    }

    fn last_index(&self) -> u32 {
        (**self).last_index()
    }

    fn last_term(&self) -> u32 {
        (**self).last_term()
    }
}
