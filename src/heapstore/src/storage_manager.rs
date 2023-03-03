use crate::heapfile::HeapFile;
use crate::heapfileiter::HeapFileIterator;
use crate::page::Page;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::gen_random_test_sm_dir;
use common::PAGE_SIZE;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use serde::{Serialize, Deserialize};
use serde_json;
use std::path::Path;
use std::io::BufReader;
use std::io::Write;


/// The StorageManager struct
pub struct StorageManager {
    /// Path to database metadata files.
    pub storage_path: PathBuf,
    pub arc_s_path: Arc<RwLock<PathBuf>>,
    /// Indicates if this is a temp StorageManager (for testing)
    pub container_hashmap: Arc<RwLock<HashMap<ContainerId, PathBuf>>>, //path to hf
    f_hf: Arc<RwLock<HashMap<ContainerId, Arc<HeapFile>>>>,
    is_temp: bool,
    
}

#[derive(Deserialize, Serialize)]
pub struct serde_hashmap {
    h: HashMap<u16, PathBuf>,
}

/// The required functions in HeapStore's StorageManager that are specific for HeapFiles
impl StorageManager {
    /// Get a page if exists for a given container.
    pub(crate) fn get_page(
        &self,
        container_id: ContainerId,
        page_id: PageId,
        _tid: TransactionId,
        _perm: Permissions,
        _pin: bool,
    ) -> Option<Page> {
        //recover hf
        let r_container_hm = self.container_hashmap.read().unwrap();
        let mut path_buf = r_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");
        //check
        let checkp0 = hf.read_page_from_file(page_id).unwrap();
        return Some(checkp0);

    }

    /// Write a page
    pub(crate) fn write_page(
        &self,
        container_id: ContainerId,
        page: Page,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        let r_container_hm = self.container_hashmap.read().unwrap();
        let mut path_buf = r_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");
        hf.write_page_to_file(page)
    }

    /// Get the number of pages for a container
    fn get_num_pages(&self, container_id: ContainerId) -> PageId {
        let r_container_hm = self.container_hashmap.read().unwrap();
        let mut path_buf = r_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");
        hf.num_pages()
    }


    /// Test utility function for counting reads and writes served by the heap file.
    /// Can return 0,0 for invalid container_ids
    //ignore
    #[allow(dead_code)]
    pub(crate) fn get_hf_read_write_count(&self, container_id: ContainerId) -> (u16, u16) {
        panic!("TODO milestone hs");
    }

    /// For testing
    pub fn get_page_debug(&self, container_id: ContainerId, page_id: PageId) -> String {
        match self.get_page(container_id, page_id, TransactionId::new(), Permissions::ReadOnly, false) {
            Some(p) => {
                format!("{:?}",p)
            },
            None => String::new()
        }
    }

    /// For testing
    pub fn get_page_bytes(&self, container_id: ContainerId, page_id: PageId) -> Vec<u8> {
        match self.get_page(container_id, page_id, TransactionId::new(), Permissions::ReadOnly, false) {
            Some(p) => {
                p.to_bytes()
            },
            None => Vec::new()
        }
    }


}
/////****************************************CREATE NEW STORAGE MANAGET**********************************************
/// Implementation of storage trait
impl StorageTrait for StorageManager {
    type ValIterator = HeapFileIterator;

    /// Create a new storage manager that will use storage_path as the location to persist data
    /// (if the storage manager persists records on disk; not the case for memstore)
    /// For startup/shutdown: check the storage_path for data persisted in shutdown() that you can
    /// use to populate this instance of the SM. Otherwise create a new one.
    fn new(storage_path: PathBuf) -> Self {
        let p_clone_1 = storage_path.to_path_buf();
        let p_clone_2 = storage_path.to_path_buf();

        if !Path::new(&p_clone_1).exists() || !Path::new("data.json").exists(){ //this is not a shutdown scenario
            fs::create_dir(p_clone_1);
            let new_manager = StorageManager {
                storage_path: storage_path, 
                arc_s_path: Arc::new(RwLock::new(p_clone_2)), 
                container_hashmap: Arc::new(RwLock::new(HashMap::new())),
                f_hf: Arc::new(RwLock::new(HashMap::new())),
                is_temp: false};
            return new_manager;
        } else {
            let file = File::open("data.json").unwrap();
            let reader = BufReader::new(file);

            let mut shm: serde_hashmap = serde_json::from_reader(reader).unwrap();
            // let pathbufs = shm.values.clone();
            // let ids = shm.keys.clone();
            // let hashmap: HashMap<_, _> = ids.iter().zip(pathbufs.iter()).collect();


            let new_manager = StorageManager {
                storage_path: storage_path, 
                arc_s_path: Arc::new(RwLock::new(p_clone_2)), 
                container_hashmap: Arc::new(RwLock::new(shm.h)), 
                f_hf: Arc::new(RwLock::new(HashMap::new())),
                is_temp: false};
            return new_manager;
        }
    }

    /// Create a new storage manager for testing. There is no startup/shutdown logic here: it
    /// should simply create a fresh SM and set is_temp to true
    fn new_test_sm() -> Self {
        println!("in new SM:");
        let storage_path = gen_random_test_sm_dir();
        let mut p_clone = storage_path.to_path_buf();
        println!("Making new temp storage_manager {:?}", storage_path);
        fs::create_dir(storage_path.to_path_buf());
        let new_t_manager = StorageManager {
            storage_path: storage_path, 
            arc_s_path: Arc::new(RwLock::new(p_clone)), 
            container_hashmap: Arc::new(RwLock::new(HashMap::new())),
            f_hf: Arc::new(RwLock::new(HashMap::new())),
            is_temp: true};
        new_t_manager
    }

    fn get_simple_config() -> common::ContainerConfig {
        common::ContainerConfig::simple_container()
    }

    /// Insert some bytes into a container for a particular value (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns the value id associated with the stored value.
    /// Function will need to find the first page that can hold the value.
    /// A new page may need to be created if no space on existing pages can be found.

    fn insert_value(
        &self,
        container_id: ContainerId,
        value: Vec<u8>,
        tid: TransactionId,
    ) -> ValueId {
        let mut w_container_hm = self.container_hashmap.write().unwrap();
        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        } else if w_container_hm.get(&container_id) == None { //container must have already been created
            panic!("This container doesn't exists in the storage manager");
        }

        let mut exist = false;
        //insertion
        let mut writable_hf = self.f_hf.write().unwrap();
        let mut hf:Arc<HeapFile>;
        if writable_hf.contains_key(&container_id) {
            //println!("wow key exists");
            //println!("{:?}", writable_hf);
            hf = writable_hf.get(&container_id).unwrap().clone();
            exist = true;
        } else {
            //println!("key doesn't exist");
            let path_buf = w_container_hm.get(&container_id).unwrap().to_path_buf().clone();
            hf = Arc::new(HeapFile::new(path_buf, container_id).expect("Unable to create HF when inserting bytes"));
        }
        drop(writable_hf);
        let a = hf.page_ids.read().unwrap();
        let readable_pg_ids = a.clone();
        let condition: bool = readable_pg_ids.len().clone() < 150;
        drop(a);

        let arr: &[u8] = &value; // cast into u8
        //println!("start updating");
        if condition {
            for i in 0..readable_pg_ids.len() {
                let mut p = hf.read_page_from_file(readable_pg_ids[i]).unwrap();
                //println!("does p have enough left space? {:?}", p.enough_space(arr));
                if p.enough_space(arr) {
                    let s_id = p.add_value(arr).unwrap();
                    let v_id = ValueId::new_slot(container_id, readable_pg_ids[i], s_id);
                    hf.write_page_to_file(p);
                    if !exist {
                        let mut writable_hf = self.f_hf.write().unwrap();
                        writable_hf.insert(container_id, hf);
                    }
                    return v_id;
                } else { //doesnt have enough space
                    continue;
                }
            }
        }
        //all pages full or there's no table, create a new one

        let mut new_p = Page::new(readable_pg_ids.len() as u16);
        drop(readable_pg_ids);
        let new_page_id = new_p.page_id;
        let s_id = new_p.add_value(arr).unwrap();
        hf.write_page_to_file(new_p);
        let v_id = ValueId::new_slot(container_id, new_page_id, s_id);

        if !exist {
            let mut writable_hf = self.f_hf.write().unwrap();
            writable_hf.insert(container_id, hf);
        }

        return v_id;
    }
    // fn helper(
    //     &self,
    //     container_id: ContainerId,
    //     idx: usize,
    // ) -> Result<(), CrustyError> {
    //     let path_buf = w_container_hm.get(&container_id).unwrap().to_path_buf();
    //     hf = HeapFile::new(path_buf, container_id).expect("Unable to create HF when inserting bytes");
    //     let mut writable_hf = self.f_hf.write().unwrap();
    //     writable_hf.insert(container_id, Arc::new(hf));
    // }
        

    /// Insert some bytes into a container for vector of values (e.g. record).
    /// Any validation will be assumed to happen before.
    /// Returns a vector of value ids associated with the stored values.
    fn insert_values(
        &self,
        container_id: ContainerId,
        values: Vec<Vec<u8>>,
        tid: TransactionId,
    ) -> Vec<ValueId> {
        let mut ret = Vec::new();
        for v in values {
            ret.push(self.insert_value(container_id, v, tid));
        }
        ret
    }

    /// Delete the data for a value. If the valueID is not found it returns Ok() still.
    fn delete_value(&self, id: ValueId, tid: TransactionId) -> Result<(), CrustyError> {
        let mut container_id = id.container_id.clone();
        let mut r_container_hm = self.container_hashmap.read().unwrap();
        let mut path_buf = r_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");
        if id.page_id != None && id.slot_id != None { //page and slot not specified
            let mut page = hf.read_page_from_file(id.page_id.unwrap()).unwrap();
            page.delete_value(id.slot_id.unwrap()); //delete
            hf.write_page_to_file(page).unwrap();
        }

        return Ok(());


    }

    /// Updates a value. Returns valueID on update (which may have changed). Error on failure
    /// Any process that needs to determine if a value changed will need to compare the return valueId against
    /// the sent value.
    fn update_value(
        &self,
        value: Vec<u8>,
        id: ValueId,
        _tid: TransactionId,
    ) -> Result<ValueId, CrustyError> {
        let mut container_id = id.container_id.clone();
        let mut w_container_hm = self.container_hashmap.write().unwrap();
        let mut path_buf = w_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");

        if value.len() > PAGE_SIZE {
            panic!("Cannot handle inserting a value larger than the page size");
        } else {
            let readable_pg_ids = hf.page_ids.read().unwrap();
            let arr: &[u8] = &value; // cast into u8
            for i in 0..readable_pg_ids.len() {
                let mut p = hf.read_page_from_file(readable_pg_ids[i]).unwrap();
                if p.page_id == id.page_id.unwrap() {                
                    //enough space. Delete original val, add to the end of file. x enough space, delete and create a new page
                    self.delete_value(id, _tid).unwrap();
                    return Ok(self.insert_value(id.container_id, value, _tid));
                }
                else {
                    continue;
                }
            }
        }
        return Err(CrustyError::CrustyError(format!(
            "Invalid pageID in fn update_value",
        )))
    }

    /// Create a new container to be stored.
    /// fn create_container(&self, name: String) -> ContainerId;
    /// Creates a new container object.
    /// For this milestone you will not need to utilize
    /// the container_config, name, container_type, or dependencies
    ///
    ///
    /// # Arguments
    ///
    /// * `container_id` - Id of container to add delta to.
    fn create_container(
        &self,
        container_id: ContainerId,
        _container_config: common::ContainerConfig,
        _name: Option<String>,
        _container_type: common::ids::StateType,
        _dependencies: Option<Vec<ContainerId>>,
    ) -> Result<(), CrustyError> {
        let mut writable_s_p = self.arc_s_path.write().unwrap();
        let mut storage_path = writable_s_p.clone();
        let mut strorage_path_2 = writable_s_p.clone();
        let mut hf = HeapFile::new(storage_path, container_id).expect("Unable to create HF for test");

        //add container to hashmap
        let mut w_container_hm = self.container_hashmap.write().unwrap();
        w_container_hm.insert(container_id, strorage_path_2);
        //println!("{:?}", w_container_hm);

        Ok(())
    }

    /// A wrapper function to call create container
    fn create_table(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        self.create_container(
            container_id,
            StorageManager::get_simple_config(),
            None,
            common::ids::StateType::BaseTable,
            None,
        )
    }

    /// Remove the container and all stored values in the container.
    /// If the container is persisted remove the underlying files
    fn remove_container(&self, container_id: ContainerId) -> Result<(), CrustyError> {
        let mut w_container_hm = self.container_hashmap.write().unwrap();
        let mut path_buf = w_container_hm.get(&container_id).unwrap();
        fs::remove_file(&path_buf)?;
        w_container_hm.remove(&container_id);
        Ok(())
    }
 
    /// Get an iterator that returns all valid records
    fn get_iterator(
        &self,
        container_id: ContainerId,
        tid: TransactionId,
        _perm: Permissions,
    ) -> Self::ValIterator {
        let mut r_container_hm = self.container_hashmap.read().unwrap();
        let mut path_buf = r_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");
        return HeapFileIterator::new(tid, Arc::new(hf));
    }

    /// Get the data for a particular ValueId. Error if does not exists
    fn get_value(
        &self,
        id: ValueId,
        tid: TransactionId,
        perm: Permissions,
    ) -> Result<Vec<u8>, CrustyError> {
        let mut container_id = id.container_id.clone();
        let mut r_container_hm = self.container_hashmap.read().unwrap();
        let mut path_buf = r_container_hm.get(&container_id).unwrap();
        let mut hf = HeapFile::new(path_buf.clone(), container_id).expect("Unable to create HF during fn get_page");
        if id.page_id != None && id.slot_id != None { //page and slot specified
            let mut page = hf.read_page_from_file(id.page_id.unwrap()).unwrap();

            return Ok(page.get_value(id.slot_id.unwrap()).unwrap()); //get value
        }
        return Err(CrustyError::IOError(
            "Could not find value".to_string(),
        ));
    }

    /// Notify the storage manager that the transaction is finished so that any held resources can be released.
    //ignore
    fn transaction_finished(&self, tid: TransactionId) {
    }

    /// Testing utility to reset all state associated the storage manager. Deletes all data in
    /// storage path (keeping storage path as a directory). Doesn't need to serialize any data to
    /// disk as its just meant to clear state. 
    ///
    /// Clear any data structures in the SM you add
    fn reset(&self) -> Result<(), CrustyError> {
        let mut w_container_hm = self.container_hashmap.write().unwrap();
        *w_container_hm = HashMap::new();

        fs::remove_dir_all(self.storage_path.clone())?;
        fs::create_dir_all(self.storage_path.clone()).unwrap();
        Ok(())
    }

    /// If there is a buffer pool or cache it should be cleared/reset.
    /// Otherwise do nothing.
    //ignore
    fn clear_cache(&self) {
    }

    /// Shutdown the storage manager. Should be safe to call multiple times. You can assume this
    /// function will never be called on a temp SM.
    /// This should serialize the mapping between containerID and Heapfile to disk in a way that
    /// can be read by StorageManager::new. 
    /// HINT: Heapfile won't be serializable/deserializable. You'll want to serialize information
    /// that can be used to create a HeapFile object pointing to the same data. You don't need to
    /// worry about recreating read_count or write_count.
    fn shutdown(&self) {
        let mut w_container_hm = self.container_hashmap.write().unwrap();
        // let mut paths:Vec<PathBuf> = w_container_hm.expect("REASON").values().cloned().collect(); //convert hashmap to two vec for key&val
        // let mut ks:Vec<u16> = w_container_hm.expect("REASON").keys().cloned().collect();

        // let mut object = serde_hashmap {keys: ks, values: paths};
        // let string = serde_json::to_string(&object).unwrap();
        let mut hashmap = w_container_hm.clone();

        let mut object = serde_hashmap {h:hashmap.clone()};
        let serialized = serde_json::to_string(&object).unwrap();
        let mut file = File::create("data.json").unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
        // file.write_all(string.as_bytes()).unwrap();

    }

    fn import_csv(
        &self,
        table: &Table,
        path: String,
        _tid: TransactionId,
        container_id: ContainerId,
        _timestamp: LogicalTimeStamp,
    ) -> Result<(), CrustyError> {
        // Err(CrustyError::CrustyError(String::from("TODO")))
        // Convert path into an absolute path.
        let path = fs::canonicalize(path)?;
        debug!("server::csv_utils trying to open file, path: {:?}", path);
        let file = fs::File::open(path)?;
        // Create csv reader.
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        // Iterate through csv records.
        let mut inserted_records = 0;
        for result in rdr.records() {
            #[allow(clippy::single_match)]
            match result {
                Ok(rec) => {
                    // Build tuple and infer types from schema.
                    let mut tuple = Tuple::new(Vec::new());
                    for (field, attr) in rec.iter().zip(table.schema.attributes()) {
                        // TODO: Type mismatch between attributes and record data>
                        match &attr.dtype() {
                            DataType::Int => {
                                let value: i32 = field.parse::<i32>().unwrap();
                                tuple.field_vals.push(Field::IntField(value));
                            }
                            DataType::String => {
                                let value: String = field.to_string().clone();
                                tuple.field_vals.push(Field::StringField(value));
                            }
                        }
                    }
                    //TODO: How should individual row insertion errors be handled?
                    debug!(
                        "server::csv_utils about to insert tuple into container_id: {:?}",
                        &container_id
                    );
                    self.insert_value(container_id, tuple.to_bytes(), _tid);
                    inserted_records += 1;
                }
                _ => {
                    // FIXME: get error from csv reader
                    error!("Could not read row from CSV");
                    return Err(CrustyError::IOError(
                        "Could not read row from CSV".to_string(),
                    ));
                }
            }
        }
        info!("Num records imported: {:?}", inserted_records);
        Ok(())
    }
}

/// Trait Impl for Drop
impl Drop for StorageManager {
    // if temp SM this clears the storage path entirely when it leaves scope; used for testing
    fn drop(&mut self) {
        if self.is_temp {
            debug!("Removing storage path on drop {:?}", self.storage_path);
            let remove_all = fs::remove_dir_all(self.storage_path.clone());
            if let Err(e) = remove_all {
                println!("Error on removing temp dir {}", e);
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use crate::storage_manager::StorageManager;
    use common::storage_trait::StorageTrait;
    use common::testutil::*;

    #[test]
    fn hs_sm_a_insert() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);

        let bytes = get_random_byte_vec(40);
        let tid = TransactionId::new();

        let val1 = sm.insert_value(cid, bytes.clone(), tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val1.page_id.unwrap());
        assert_eq!(0, val1.slot_id.unwrap());

        let p1 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();

        let val2 = sm.insert_value(cid, bytes, tid);
        assert_eq!(1, sm.get_num_pages(cid));
        assert_eq!(0, val2.page_id.unwrap());
        assert_eq!(1, val2.slot_id.unwrap());

        let p2 = sm
            .get_page(cid, 0, tid, Permissions::ReadOnly, false)
            .unwrap();
        assert_ne!(p1.to_bytes()[..], p2.to_bytes()[..]);
    }
    #[test]
    fn hs_sm_b_iter_small() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;
        sm.create_table(cid);
        let tid = TransactionId::new();

        //Test one page
        let mut byte_vec: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];
        for val in &byte_vec {
            sm.insert_value(cid, val.clone(), tid);
        }
        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on two pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }

        // Should be on 3 pages
        let mut byte_vec2: Vec<Vec<u8>> = vec![
            get_random_byte_vec(300),
            get_random_byte_vec(500),
            get_random_byte_vec(400),
        ];

        for val in &byte_vec2 {
            sm.insert_value(cid, val.clone(), tid);
        }
        byte_vec.append(&mut byte_vec2);

        let iter = sm.get_iterator(cid, tid, Permissions::ReadOnly);
        for (i, x) in iter.enumerate() {
            assert_eq!(byte_vec[i], x.0);
        }
    }
    
    #[test]
    fn hs_sm_b_iter_large() {
        init();
        let sm = StorageManager::new_test_sm();
        let cid = 1;

        sm.create_table(cid).unwrap();
        let tid = TransactionId::new();

        let vals = get_random_vec_of_byte_vec(1000, 40, 400);
        sm.insert_values(cid, vals, tid);
        let mut count = 0;
        for _ in sm.get_iterator(cid, tid, Permissions::ReadOnly) {
            count += 1;
        }
        assert_eq!(1000, count);
    }
}
