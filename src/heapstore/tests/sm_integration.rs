#[macro_use]
extern crate log;
extern crate common;
extern crate heapstore as sm;
use std::path::PathBuf;

use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::*;
use rand::{thread_rng, Rng};
use sm::storage_manager::StorageManager;

const RO: Permissions = Permissions::ReadOnly;

#[test]
fn sm_inserts() {
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let num_vals: Vec<usize> = vec![10, 50, 75, 100, 500, 1000];
    for i in num_vals {
        let vals1 = get_random_vec_of_byte_vec(i, 50, 100);
        let cid = i as ContainerId;
        sm.create_table(cid).unwrap();
        sm.insert_values(cid, vals1.clone(), t);
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
        assert!(
            compare_unordered_byte_vecs(&vals1, check_vals),
            "Insert of size {} should be equal",
            i
        );
    }
}

#[test]
fn sm_insert_delete() {
    let mut rng = thread_rng();
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let mut vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let mut val_ids = sm.insert_values(cid, vals1.clone(), t);
    for _ in 0..10 {
        let idx_to_del = rng.gen_range(0..vals1.len());
        sm.delete_value(val_ids[idx_to_del], t).unwrap();
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
        assert!(!compare_unordered_byte_vecs(&vals1, check_vals.clone()));
        vals1.swap_remove(idx_to_del);
        val_ids.swap_remove(idx_to_del);
        assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    }
}

#[test]
fn sm_insert_updates() {
    init();
    let mut rng = thread_rng();
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let mut vals_to_ins = get_ascending_vec_of_byte_vec_02x(100, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let mut val_ids = sm.insert_values(cid, vals_to_ins.clone(), t);
    // For Debugging. idx, old vid, new vid, old bytes, new bytes
    let mut modified_items: Vec<(usize, ValueId, ValueId, Vec<u8>, Vec<u8>)> = Vec::new();
    let max_page: PageId = val_ids.iter().map(|a| a.page_id.unwrap()).max().unwrap();
    debug!(" -------- DONE INSERTING VALUES. UPDATING 10 RANDOM ENTRIES -------");
    for i in 0..10 {
        for x in 0..= max_page {
            let s = sm.get_page_debug(cid, x);
            debug!("Page {}\n{}",x, s);
        }
        let idx_to_upd = rng.gen_range(0..vals_to_ins.len());
        let new_bytes = get_random_byte_vec(15);
        let old_val_id =  val_ids[idx_to_upd].clone();
        let new_val_id = sm
            .update_value(new_bytes.clone(), val_ids[idx_to_upd], t)
            .expect("Error updating value");
        if new_val_id.page_id.unwrap() != val_ids[idx_to_upd].page_id.unwrap() {
            debug!(" -MOVED   {} new_vid:{:?} old_vid:{:?}, old bytes {:?}",idx_to_upd, new_val_id, old_val_id, &vals_to_ins[idx_to_upd][..5]);
        } else {
            debug!(" -INPLACE {} vid:{:?}, old bytes {:?}", idx_to_upd, old_val_id, &vals_to_ins[idx_to_upd][..5]);
        }
        modified_items.push((idx_to_upd, old_val_id, new_val_id, vals_to_ins[idx_to_upd].clone(), new_bytes.clone()));
        let (check_vals, _check_val_ids): (Vec<Vec<u8>>, Vec<ValueId>) =
            sm.get_iterator(cid, t, RO).map(|(a, b)| (a, b)).unzip();
        assert!(
            !compare_unordered_byte_vecs(&vals_to_ins, check_vals.clone()),
            "Data should be different. On iteration {}",
            i
        );
        vals_to_ins[idx_to_upd] = new_bytes;
        val_ids[idx_to_upd] = new_val_id;
        let same = compare_unordered_byte_vecs(&vals_to_ins, check_vals.clone());
        if !same {
            debug!("i: orig vals  |  sm_iter_vals (not the same) \n--------------------------------");
            // For debugging. Take a slice of each element to keep smaller for printing
            let vals_smaller: Vec<&[u8]> = vals_to_ins.iter().map(|f| &f[..5]).collect();
            for (i, x) in vals_smaller.iter().enumerate() {
                debug!("{} {:?}",i, x);
            }
            let cv_smaller: Vec<&[u8]> = check_vals.iter().map(|f| &f[..5]).collect();
            // for (i, (v, c)) in vals_smaller.iter().zip(cv_smaller.iter()).enumerate() {
            //     debug!("{}: {:?} | {:?}", i, v, c);
            // }
            //println!("{:?}\n{:?}\n", vals_smaller,cv_smaller);
            for (read, vid) in cv_smaller.iter().zip(_check_val_ids) {
                debug!("{:?}: {:?}", vid, read);
            }

            debug!("Changes:\n=========================================================================\n");
            for (idx, ov, nv, ob, nb) in &modified_items {
                debug!("{} {:?}->{:?} {:?}->{:?}", idx,ov,nv,ob,nb);
            }
        }
        assert!(
            compare_unordered_byte_vecs(&vals_to_ins, check_vals),
            "Data should be the same. On iteration {}.\n {:?}\n\n",
            i,
            modified_items
        );
    }
}

#[test]
#[should_panic]
fn sm_no_container() {
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    sm.insert_values(1, vals1, t);
}

#[test]
fn sm_test_shutdown() {
    let path = PathBuf::from("tmp");
    let sm = StorageManager::new(path.clone());
    let t = TransactionId::new();

    let vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let _val_ids = sm.insert_values(cid, vals1.clone(), t);
    sm.shutdown();

    let sm2 = StorageManager::new(path);
    let check_vals: Vec<Vec<u8>> = sm2.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
    assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    sm2.reset().unwrap();
}

