use crate::heapfile::HeapFile;
use crate::page::PageIntoIter;
use common::prelude::*;
use std::sync::Arc;

#[allow(dead_code)]
/// The struct for a HeapFileIterator.
/// We use a slightly different approach for HeapFileIterator than
/// standard way of Rust's IntoIter for simplicity (avoiding lifetime issues).
/// This should store the state/metadata required to iterate through the file.
///
/// HINT: This will need an Arc<HeapFile>
pub struct HeapFileIterator {
    pub tid: TransactionId,
    heapfile: Arc<HeapFile>,
    pub pg_index: usize,
    pub pg_iter: PageIntoIter, //an iterator of a page
    pub flag: bool,
}

/// Required HeapFileIterator functions
impl HeapFileIterator {
    /// Create a new HeapFileIterator that stores the tid, and heapFile pointer.
    /// This should initialize the state required to iterate through the heap file.
    pub(crate) fn new(tid: TransactionId, hf: Arc<HeapFile>) -> Self {
        let new_hi = HeapFileIterator {
            tid: tid,
            heapfile: hf,
            pg_index: 0,
            pg_iter: PageIntoIter {blocks:Vec::new(), index:0}, //placeholder
            flag: false,
        };
        new_hi
    }
}

/// Trait implementation for heap file iterator.
/// Note this will need to iterate through the pages and their respective iterators.
impl Iterator for HeapFileIterator {
    type Item = (Vec<u8>, ValueId);
    fn next(&mut self) -> Option<Self::Item> {
        let ahf = self.heapfile.clone();
        let readable_pg_ids = ahf.page_ids.read().unwrap();

        if self.pg_index == readable_pg_ids.len() { //end of heapfile
            return None;
        }
        if self.flag {
            let next_result = self.pg_iter.next();
            if next_result == None { //end of page
                self.flag = false;
                self.pg_index += 1;
                //->go to line 66 to initiate the next page
                if self.pg_index == readable_pg_ids.len() { //end of heapfile
                    return None;
                }
            } else { //normal case, go to next value
                let Some((data, s_id)) = next_result else { todo!() };
                let v_id = ValueId::new_slot(ahf.container_id, readable_pg_ids[self.pg_index], s_id);
                return Some((data, v_id));

            }
        }
        //if !self.flag { 
        //haven't start iterating yet or finish last round, need reinitiate
        self.flag = true;
        let pg = ahf.read_page_from_file(readable_pg_ids[self.pg_index]).unwrap();
        self.pg_iter = pg.into_iter();
        let Some((data, s_id)) = self.pg_iter.next() else { return None };
        let v_id = ValueId::new_slot(ahf.container_id, readable_pg_ids[self.pg_index], s_id);
        return  Some((data, v_id));

    }
}
