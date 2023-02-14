use crate::page::Page;
use common::prelude::*;
use common::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::PathBuf;
use std::path::Path;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};


use std::io::BufWriter;
use std::io::{Seek, SeekFrom};


//https://stackoverflow.com/questions/66847162/update-re-assign-struct-fields-without-taking-mut-self
/// The struct for a heap file.  
///
/// HINT: You likely will want to design for interior mutability for concurrent accesses.
/// eg Arc<RwLock<>> on some internal members
///
/// HINT: You will probably not be able to serialize HeapFile, as it needs to maintain a link to a
/// File object, which cannot be serialized/deserialized/skipped by serde. You don't need to worry
/// about persisting read_count/write_count during serialization.
///
/// Your code should persist what information is needed to recreate the heapfile.
///
pub(crate) struct HeapFile {
    //TODO milestone hs
    pub file_lock: Arc<RwLock<File>>,
    //pub left_bytes: Arc<RwLock<u64>>,
    pub page_ids: Arc<RwLock<Vec<PageId>>>,
    // Track this HeapFile's container Id
    pub container_id: ContainerId,
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
}

/// HeapFile required functions
impl HeapFile {
    /// Create a new heapfile for the given path. Return Result<Self> if able to create.
    /// Errors could arise from permissions, space, etc when trying to create the file used by HeapFile.
    pub(crate) fn new(mut file_path: PathBuf, container_id: ContainerId) -> Result<Self, CrustyError> {
        //change file name to .../container_id.hf
        //https://doc.rust-lang.org/std/path/struct.PathBuf.html
        //println!("{:?}", file_path);

        let file_path_clone = file_path.to_path_buf();
        //println!("{:?}", Path::new(file_path_clone).file_stem().unwrap());
        if file_path_clone.extension() != None { //to counter hs_hf_insert who is adding random.hf
            //println!("end with hf");
            file_path.pop();
            file_path.push(container_id.to_string());
            file_path.set_extension("hf");
        } else {
            file_path.push(container_id.to_string());
            file_path.set_extension("hf");
        }
        //println!("the file path in new heapfile is {:?}", file_path);
        //check if this file exist in the system
        //https://www.includehelp.com/rust/check-a-specified-file-exists-or-not.aspx#:~:text=The%20exists()%20method%20returns%20Boolean%20value.,%2C%20otherwise%2C%20it%20returns%20false.
        let exist:bool = (&file_path).exists();
        //println!("exist or not: {exist}");

        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
        {
            Ok(f) => f,
            Err(error) => {
                return Err(CrustyError::CrustyError(format!(
                    "Cannot open or create heap file: {} {} {:?}",
                    file_path.to_string_lossy(),
                    error.to_string(),
                    error
                )))
            }
        };
        if exist {
            let size = file_path.metadata()?.len() as usize;
            let mut offset = 0 as usize;
            let mut page_id_vec = Vec::new();
            let mut buf = vec![0; PAGE_SIZE];
            while offset < size {
                file.seek(SeekFrom::Start(offset as u64))?;
                file.read_exact(&mut buf)?;
                let new_p = Page::from_bytes(&buf);

                page_id_vec.push(new_p.page_id.clone());
                offset += PAGE_SIZE;
            }
            Ok(HeapFile {
                //TODO milestone hs
                //left_bytes: Arc::new(RwLock::new( file.metadata().unwrap().len() )),
                page_ids: Arc::new(RwLock::new(page_id_vec)),
                file_lock: Arc::new(RwLock::new(file)),
                container_id: container_id,
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            })            
        }
        //doesn't exist
        else {
            Ok(HeapFile {
                //TODO milestone hs
                //left_bytes: Arc::new(RwLock::new( file.metadata().unwrap().len() )),
                page_ids: Arc::new(RwLock::new(Vec::new())),
                file_lock: Arc::new(RwLock::new(file)),
                container_id: container_id,
                read_count: AtomicU16::new(0),
                write_count: AtomicU16::new(0),
            })
        }

    }

    /// Return the number of pages for this HeapFile.
    /// Return type is PageId (alias for another type) as we cannot have more
    /// pages than PageId can hold.
    pub fn num_pages(&self) -> PageId {
        let readable_pg_ids = self.page_ids.read().unwrap();
        readable_pg_ids.len() as u16
    }

    /// Read the page from the file.
    /// Errors could arise from the filesystem or invalid pageId
    /// Note: that std::io::{Seek, SeekFrom} require Write locks on the underlying std::fs::File
    pub(crate) fn read_page_from_file(&self, pid: PageId) -> Result<Page, CrustyError> {
        //If profiling count reads
        #[cfg(feature = "profile")]
        {
            self.read_count.fetch_add(1, Ordering::Relaxed);
        }
        let readable_pg_ids = self.page_ids.read().unwrap();
        //println!("{:?}", readable_pg_ids);
        let mut offset = 0;
        for i in 0..readable_pg_ids.len() {
            if pid == readable_pg_ids[i] {
                let mut writable_f = self.file_lock.write().unwrap();
                writable_f.seek(SeekFrom::Start(offset));
                let mut buf = vec![0; 4096];
                writable_f.read_exact(&mut buf)?; 
                return Ok(Page::from_bytes(&buf));

            } else {
                offset += 4096;
            }
        }
        return Err(CrustyError::CrustyError(format!(
            "Invalid pageID",
        )))
    }

    /// Take a page and write it to the underlying file.
    /// This could be an existing page or a new page
    pub(crate) fn write_page_to_file(&self, page: Page) -> Result<(), CrustyError> {
        // println!(
        //     "Writing page {} to file {}",
        //     page.get_page_id(),
        //     self.container_id
        // );
        //If profiling count writes
        #[cfg(feature = "profile")]
        {
            self.write_count.fetch_add(1, Ordering::Relaxed);
        }
        let bytes = page.to_bytes();

        let readable_pg_ids = self.page_ids.read().unwrap();
        //println!("{:?}", readable_pg_ids);
        //if this page exists
        let mut offset = 0;
        for i in 0..readable_pg_ids.len() {
            if page.page_id == readable_pg_ids[i] {
                let mut writable_f = self.file_lock.write().unwrap();
                writable_f.seek(SeekFrom::Start(offset))?;
                writable_f.write_all(&bytes).expect("write failed with exist pid"); //overwrite
                return Ok(());
            } else {
                offset += 4096;
            }
        }
        drop(readable_pg_ids);

        //this page doesn't exist in the file before
        //add to page id vectors
        //println!("this page doesn't exist before");
        let mut writable_pg_ids = self.page_ids.write().unwrap();
        writable_pg_ids.push(page.page_id);
        //write to file
        let mut writable_f = self.file_lock.write().unwrap();
        writable_f.seek(SeekFrom::Start(offset))?;
        writable_f.write_all(&bytes).expect("write failed with non-exist pid"); //expext is unwrap with an error message
        //println!("finish writing");

        // let mut writable_left = self.left_bytes.write().unwrap();
        // println!("{:?}", *writable_left);
        // *writable_left -= 4096;


        
        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    use super::*;
    use common::testutil::*;
    use temp_testdir::TempDir;

    #[test]
    fn hs_hf_insert() {
        init();

        //Create a temp file
        let f = gen_random_test_sm_dir();
        let tdir = TempDir::new(f, true);
        let mut f = tdir.to_path_buf();
        f.push(gen_rand_string(4));
        f.set_extension("hf");

        let mut hf = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");

        // Make a page and write
        let mut p0 = Page::new(0);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p0.add_value(&bytes);
        let p0_bytes = p0.to_bytes();

        hf.write_page_to_file(p0);
        //check the page
        assert_eq!(1, hf.num_pages());
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //Add another page
        let mut p1 = Page::new(1);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let bytes = get_random_byte_vec(100);
        p1.add_value(&bytes);
        let p1_bytes = p1.to_bytes();

        hf.write_page_to_file(p1);

        assert_eq!(2, hf.num_pages());
        //Recheck page0
        let checkp0 = hf.read_page_from_file(0).unwrap();
        assert_eq!(p0_bytes, checkp0.to_bytes());

        //check page 1
        let checkp1 = hf.read_page_from_file(1).unwrap();
        assert_eq!(p1_bytes, checkp1.to_bytes());

        //check overwrite a page in page 1



        //check recovering of page0 when hf exist
        let hf_doppelganger = HeapFile::new(f.to_path_buf(), 0).expect("Unable to create HF for test");
        let mut dg_p1 = hf_doppelganger.read_page_from_file(1).unwrap();
        assert_eq!(dg_p1.to_bytes(), checkp1.to_bytes());
        let try_bytes = get_random_byte_vec(100);
        //checkp1.add_value(&bytes);
        dg_p1.add_value(&try_bytes);

        #[cfg(feature = "profile")]
        {
            assert_eq!(*hf.read_count.get_mut(), 3);
            assert_eq!(*hf.write_count.get_mut(), 2);
        }
    }
}

