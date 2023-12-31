# Heapstore Milestone (hs)

In this milestone you will continue building a storage manager that uses heapfiles to store values/data. In CrustyDB a storage manager (**SM**) is responsible for persisting all data. A SM in Crusty is agnostic to what is being stored, as it takes a request to store a `value` as bytes (a `Vec<u8>`) in a `container`. The value is stored and the SM returns a `ValueId` that indicates how it can retrieve the value later. It is the responsibility of an other component in the system to interpret the bytes. For example, CrustyDB will create a container for each table/relation stored, and each record will get stored as a `value`.  The same database could also store an index as a container, and store each index page as a `value`.

CrustyDB comes with a 'working' storage manager, memstore, that keeps 
all containers in memory using standard data structures. Memstore persists data to files on shutdown and can re-load the files into memory on start up.  For this milestone you are writing a new SM to replace the memstore with. All the code you will need to write is in the project/crate heapstore. Note that for the hs milestone you will not be implementing any buffer pool (cache) for this SM. A later milestone will give you the option to add a buffer pool to this SM. You will additionally not be implementing anything to do with transactions and can safely ignore any functions dealing with transactions for now. 

The milestone includes a series of unit tests and integration tests for testing functionality. These tests are not exhaustive and you may want to write (and possibly contribute) additional tests. This module has a moderate amount of comments. Not all packages in CrustyDB will have the same level of comments, as this is designed to be one of the first milestones in CrustyDB. Working on a moderate sized code base with not a full set of comments or documentation is something you will encounter in your career.

This and all future milestones will be completed in a repo that pulls from the `crustydb-23` upstream. You will need to integrate your implementation of page from the first milestone into the new repo for the heapstore and all future milestones. This simply involves copying your `page.rs` file to `src/heapstore/src` in your full crustydb repo.

## Scoring and Requirements

65% of your score on this milestone is based on correctness that is demonstrated by passing all of the provided unit and integration tests in the HS package. This means when running `cargo test -p heapstore` all tests pass. 15% of your score is based on performance in the HS package. This means when running `cargo bench -p heapstore` your runtimes are within 5% of our baseline runtimes or better (i.e. runtimes at most 5% higher than the baselines). We will not be providing the baseline thresholds and we will (at least initially) be hiding your scores on the performance component of this milestone. We encourage you to discuss and share your runtimes for the benchmarks to get an idea for how your implementation performs compared to others.

 10% of your score is based on code quality (following good coding conventions, comments, well organized functions, etc). 10% is based on your write up (my-hs.txt). The write up should contain:
 -  A brief describe of your solution, in particular what design decisions you took and why. This is only needed for part of your solutions that had some significant work (e.g. just returning a counter or a pass through function has no design decision).
- How long you roughly spent on the milestone, and what would have liked/disliked on the milestone.
- If you know some part of the milestone is incomplete, write up what parts are not working, how close you think you are, and what part(s) you got stuck on.

### Logging / Logging Tests (repeated from pg)

CrustyDB uses the [env_logger](https://docs.rs/env_logger/0.8.2/env_logger/) crate for logging. Per the docs on the log crate:
```
The basic use of the log crate is through the five logging macros: error!, warn!, info!, debug! and trace! 
where error! represents the highest-priority log messages and trace! the lowest. 
The log messages are filtered by configuring the log level to exclude messages with a lower priority. 
Each of these macros accept format strings similarly to println!.
```

The logging level is set by an environmental variable, `RUST_LOG`. The easiest way to set the level is when running a cargo command you set the logging level in the same command. EG : `RUST_LOG=debug cargo run --bin server`. However, when running unit tests the logging/output is suppressed and the logger is not initialized. So if you want to use logging for a test you must:
 - Make sure the test in question calls `init()` which is defined in `common::testutils` that initializes the logger. It can safely be called multiple times.
 - Tell cargo to not capture the output. For example, setting the level to DEBUG: `RUST_LOG=debug cargo test -- --nocapture [opt_test_name]`  **note the -- before --nocapture**


### StorageManager
The storage manager (SM) is the *public* interface to this crate. The SM manages a set of `containers`, each with its own `ContainerId`, that contain data in the form of `values` (each `value` is stored as a `Vec<u8>`). All read and write requests will act on values in containers and will be handled by the SM. The SM will internally translate those requests into operations against heap files. A SM is required to implement the `StorageTrait` in the `common` crate. A SM should be created with a directory/path of where it can persist values, or should be created as a temporary SM which is used for testing. When an SM shuts down, it should persist enough information to disk so that when it is created again with the same directory/path for persisting data, it is aware of all data that was managed prior to shutdown. In other words, the SM should be able to seamlessly resume if we shut it down and bring it back up.

### HeapFile
Each container will be stored as a heap file. The heap file is really a wrapper on top of a standard filesystem File (i.e. std::fs::File). You will use the std::fs::File implementation to perform reads and writes on the file. The container's values are spread across multiple pages, so the heap file manages a sequence of fixed sized pages (`PAGE_SIZE` in `common::lib.rs`) that are stored in the File managed by the heap file object.

Values (e.g., records) are stored in a heap file in the first available location. If there is no available space in the existing pages allocated by the heap file, the heap file will allocate a new page and store it in the same underlying filesystem file. Each heap file will need to be associated with a single filesystem file, and support the ability to read and write pages to the file in a thread-safe manner (e.g., deal with multiple readers/writers).

You will need to figure out how to utilize the fact that Pages are fixed size to ensure that you are able to write pages to a File in the correct order and are subsequently able to read specific Pages from the file. 

`HeapFile::new` takes a `PathBuf` as a parameter. This parameter specifies the filename for the underlying file that the `HeapFile` will use to store pages. Remember that each `HeapFile` is mapped to a single file, so this filename uniquely identifies a `HeapFile`.

A note for startup/shutdown in the StorageManager (discussed at greater length later in this writeup): because the `HeapFile` utilizes a `std::fs::File` object to read/write to the underlying file, you will not be able to directly serialize a `HeapFile`, as you can't serialize or skip the `File` object. This means that when thinking about serializing the StorageManager, you cannot directly serialize the `HeapFile` and must instead serialize some other information that would enable you to create a new `HeapFile` object that can read from the same underlying file and access all data persisted to it.

### Path vs. PathBuf
A quick note on Path vs. PathBuf, which you'll be dealing with in this milestone in both `StorageManager` and `HeapFile`. We can think of then as analogous to `&str` vs. `String` or `&[]` vs. `Vec`. Path holds a reference to the path string data but doesn't own it (it's a pointer and a length), meaning that it is immutable. Additionally, because it doesn't own the data, Path can only reference the data as long as it is available from wherever the data is being stored. PathBuf on the other hand actually owns the underlying data and so is mutable and doesn't need to worry about availability concerns. A good rule of thumb is that if you need to store the path, you want a PathBuf as you want to own the underlying string data. Otherwise you can take a Path.

### Page (repeated from pg)
The heap file is made up of a sequence of fixed sized pages (`PAGE_SIZE` in `common::lib.rs`). Note that each page may take up slightly more memory when loaded, but it must be able to be serialized (packed) into the fixed page size.  A page is broken into a header, which holds metadata about the page and values stored, and the body, which is where the bytes for values are stored.  This means that when in memory a page may use more than ` data: [u8; PAGE_SIZE]` (member of struct Page).


Note that values can differ in size,  but CrustyDB can reject any value that is larger than `PAGE_SIZE`. When a value is stored in a page it is associated with a `slot_id` that should not change, unless the value is updated. Note that the location of the bytes in a page for a value *can* change, but again the slot_id should not. When storing values in a page, the page should insert the value in the 'first' available space in the page. We quote first as it depends on your implementation what first actually means. If a value is deleted, than that space should be reused by a later insert. A page should also provide an iterator to return all of the valid values stored in the page.

### HeapFileIterator

SMs in CrustyDB require containers to be able to iterate through all values in a container. As part of this milestone you will write an iterator that walks through all valid values stored in a container.

### ValueId (repeated from pg)
Every stored value is associated with a `ValueId`. This is defined in `common::ids`. Each ValueId must specify a ContainerId and then a set of optional Id types. For hs, we will use PageId and SlotId for each ValueId. The (data) types used for these Ids are also defined in `common::ids`. 

```
pub type ContainerId = u16;
pub type AtomicContainerId = AtomicU16;
pub type SegmentId = u8;
pub type PageId = u16;
pub type SlotId = u16; 
```
when casting to and from another type (usize) to these Id types, you should use the type (SlotId) as they could change with time.  The intention is a that a ValueId <= 64 bits. This means that we know a page cannot have more than SlotId slots (`2^16`).

## Suggested Steps
This is a rough order of steps we suggest you take to complete the hs milestone. Note this is not exhaustive of all required tests for the milestone.

### Completing and Integrating Page
You should have completed all of the page milestone. This milestone will not work without a completed and functional page.To integrate your code from the first milestone into this repo, simply copy your `src/heapstore/src/page.rs` file from your previous repo for `crusty-page` into `src/heapstore/src` into your repo for this milestone. All future milestones will be completed in this repo.

### Heap File
With a working Page, you should move onto writing a `HeapFile`.  Here we only provide one test that should test the major functionality of HeapFile. You may want to add other tests to help your development process. Note that you must leave the following variables/and counters, as we will use them later to ensure your buffer pool is properly working.
```
    // The following are for profiling/ correctness checks
    pub read_count: AtomicU16,
    pub write_count: AtomicU16,
```

If you have not worked with File I/O, start with the [simple I/O example from the Rust book](https://doc.rust-lang.org/book/ch12-02-reading-a-file.html), then look at the API/documentation for

```
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::{Seek, SeekFrom};
```
We have also provided an example in the `new` method within `heapfile.rs` for opening a file with read/write/create permissions. 

Note that the HeapFile uses interior mutability, meaning that all functions to HeapFile only pass a reference/borrow to `&self` even though you will need to modify some state. This is why there is a hint about using an `Arc<RwLock<>>` which will allow you to make changes to a file without having a mutable reference. 

Your code should pass `cargo test -p heapstore hs_hf_insert` with a working HeapFile. Next you could move onto the HeapFileIter or Storage Manager. For both steps, we are going to not give a suggested order/steps, but suggest that you look through the code and API and determine the best way to go. The tests for iterating through a heap file will be evaluated in the SM.

### Heap File Iterator
This code allows a SM to iterate through all values stored in a heap file. It will need to walk through all pages, and iterate over all values within each page.  We diverge from the standard Rust approach for constructing the iterator to avoid issues with lifetimes. You will test your `HeapFileIterator` via the SM, but feel free to write your own tests here.

### Storage Manager
Here you are mainly implementing the trait `StorageTrait` from the common package and defined functions in `impl StorageManager`. Much of SM will be translating the basic create/read/write/delete requests into using the underlying HeapFiles.

A few things to note:
 - In addition to implementing a normal constructor in `StorageManager::new` you will need to create a temp SM in `StorageManager::new_test_sm`. The temp SM does not need to worry about startup/shutdown serialization and should instead create a blank StorageManager with the `is_temp` struct value set to `true`.
 - There is a function `reset` which is used for testing. This should clear out all data associated with a storage manager and delete all files. This should also remove all metadata abouve the removed data (e.g. remove tables from a catalog).
 - SM also uses interior mutability.
 - There are many references to transactionIds, permissions, and pins. TransactionId and permissions are there for a later (optional) milestone on transactions, so you can ignore them for this milestone (and is why they are _ prefixed). Subsequently you can ignore the `transaction_finished` and `clear_cache` functions for this milestone.
 - The function `get_hf_read_write_count` is used for the BufferPool and can be ignored for now although it is very simple. If you wish to implement it now, it simply needs to return a tuple of reads and writes from the underlying heap file. If you have a variable called hf you could return this via 
 ```
    (
    hf.read_count.load(Ordering::Relaxed),
    hf.write_count.load(Ordering::Relaxed),
    )
```
 - You may need to add new functions in page/heapfile for some operations.
 - `insert_value` will likely be the trickiest function

#### Startup/Shutdown: 
A common lifecycle of a DB is to persist data onto disk, enabling the database to be stopped while not in use. Then when we restart the DB, it reads from those files to recover whatever state it was managing before.

In this milestone, you're going to implement a similar structure in the Storage Manager, so that it can reload its previous state when it's rerun after a previous shutdown. If we shutdown Crusty while the SM is managing several `Containers`/`HeapFiles`, then we want the SM to be aware of that data when we bring Crusty back up. 

For this milestone, you need to serialize the contents of the SM to a file when the SM shuts down (see `shutdown` in `StorageTrait` and `StorageManager`). The SM manages a mapping between `ContainerIds` for the `Containers` that are created and the `HeapFiles` which actually manage the data inside a `Container`. As noted earlier, you will not be able to directly serialize a `HeapFile` even if you use `#[serde(skip)]` because it will contain a `File`. To preserve this mapping you will need to instead keep track of and then serialize a mapping between `ContainerId` and some other data which can be used to recreate the `HeapFile` (hint: what uniquely identifies a `File` object and thus the `HeapFile`?). You will need to think about how you want to maintain this data in addition to the `HeapFiles` how you want to serialize it to disk in a save file on `shutdown`, and how you want to recreate the `HeapFile` objects on startup after deserializing the save file.

Additionally, `StorageManager::reset` will need to account for these save files. In addition to clearing any data being held by the struct, you will need to make sure that any data persisted to disk by previous iterations of Crusty is also cleared--either `HeapFiles` or save files previously written by the SM--so that we're not loading stale state.

There is an example of similar startup/shutdown logic in `DatabaseState` and `ServerState` that you can use as a reference. The `ServerState` manages a variety of databases (created via `CREATE DATABSE` queries), and for each one it holds a struct `DatabaseState`. Each `DatabaseState` holds a `Database`, an implementation of a `Catalog` (both are in `src/common`), which in turn holds list of tables and containers that those belong to. The `Catalog` is what is externally communicating with the `StorageManager`. The `ServerState` on shutdown serializes in JSON its mapping of id to `DatabaseState` using serde. It then in `ServerState::new` checks to see if there's a state file written in the `storage_path`, and if so it reads and loads from that save. Otherwise it generates it from scratch.

Some notes: 
1. We'd suggest identifying what data needs to be persisted, serializing it to a JSON file using serde_json, and then figuring out how to reconstruct all the state you need to from the JSON file during deserialization.

2. `StorageManager::new` and `HeapFile::new` will be called regardless of if we're loading from a previous file or creating a brand new instance. You'll need to check for the existance of a save file and then choose whether to load or not. See `ServerState::new` for an example on how this works. 

#### Tests

The tests for the SM are in two locations. 

The first are unit tests in `storage_manager`. You run these with
`cargo test -p heapstore hs_sm_`. One of these tests can be slow, so it is ignored by default. To run this ignored test run `cargo test  -p heapstore hs_sm_  -- --ignored`

The second tests are in `heapstore/tests/` and are integration tests. They are only allowed to test public functions of the SM, and these tests should pass for all SM (same tests will exist in the memstore). Run these tests with 
`cargo test  -p heapstore sm_` note this will run the unit tests also as they have sm_ in the name.

We strongly encourage you to write new tests and contribute tests that are general (eg against shared/common functions).

With this all tests in heapstore should pass: 

```
cargo test -p heapstore
```

All Buffer Pool (*bp*) tests are in `heapstore/src/bp_tests.rs` and aren't run when you run `cargo test -p heapstore` so it can be safely ignored for now.

### Criterion  / Performance Benchmarks
[Criterion](https://bheisler.github.io/criterion.rs/book/getting_started.html) is a performance benchmark to evaluate how well a piece of code runs. We have provided a few simple criterion use cases to measure a particular piece of code. The code lives in `heapstore/benches`.
To run the benchmarks: `cargo bench -p heapstore`

In order to get full points on the performance benchmark section of this milestone, the average runtime of the three benchmarks run must be below certain thresholds. The three benchmarks run are `page insert medium`, `page insert large recs`, and `sm insert 1k`. These benchmarks run the tests many times and take the average, so things like system noise shouldn't matter. When the benchmarks run they will show 3 numbers after the test name like `page insert medium` that represent the confidence interval. The left and right values are the lower and upper bound and the middle value is the actual estimate of runtime; the middle should also be bolded. In order to get full points for each benchmark, your runtime must be at most 5% higher than that of our reference solution.

*Note on our reference solution we get the message Warning: Unable to complete 100 samples in 5.0s. You may wish to increase target time to XXs, or reduce sample count to 10.*

## Replacing Memstore
For your CrustyDB to use heapstore instead of memstore you will need to change import statements. The upstream codebase should already have this import flipped. For example in `server::main` we would need to change the following code. The same thing would need to happen in `queryexe::lib`.

```
/// Re-export Storage manager here for this crate to use. This allows us to change
/// the storage manager by changing one use statement.
pub use memstore::storage_manager::StorageManager;
```


