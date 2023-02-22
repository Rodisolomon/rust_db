use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, DataType, TableSchema, Tuple, Attribute};
use std::{collections::HashMap, hash::Hash};

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        let new_predicate = JoinPredicate { 
            op: op, 
            left_index: left_index, 
            right_index: right_index, 
        };
        return new_predicate;
    } 
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,
    open: bool,
    left_tuple: Tuple,
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        mut left_child: Box<dyn OpIterator>,
        mut right_child: Box<dyn OpIterator>,
    ) -> Self {
        left_child.open().unwrap();
        let t1 = left_child.next().unwrap().clone();
        let len1 = t1.clone().unwrap().size();
        left_child.close();
        right_child.open().unwrap();
        let t2 = right_child.next().unwrap().clone();
        let len2 = t2.clone().unwrap().size();
        right_child.close();

        let width = len1 + len2;

        let mut attrs = Vec::new();
        for _ in 0..width {
            attrs.push(Attribute::new(String::new(), DataType::Int))
        }

        let schema = TableSchema::new(attrs);
        let predicate:JoinPredicate = JoinPredicate::new(op, left_index, right_index);

        let new_join = Join {  
            left_child: left_child,
            right_child: right_child,
            schema: schema,
            predicate: predicate,
            open: false,
            left_tuple: Tuple::new(Vec::new()), //placeholder
        };
        return new_join;
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.left_child.open()?;
        self.right_child.open()?; //right child is the one keep checking and appending
        Ok(())
    }
    
    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("join has not been opened")
        }   
        let mut left_option;
        //match operation type
        if self.left_tuple.field_vals.len() == 0 { //uninitiate
            left_option = self.left_child.next().unwrap();
            self.left_tuple = left_option.clone().unwrap();
        }


        let mut right_option = self.right_child.next().unwrap();
        //println!("{:?}", right_option);

        let mut new_right_tuple;
        if right_option == None { //end of right child
            self.right_child.rewind().unwrap(); //restart right
            left_option = self.left_child.next().unwrap(); //go to next left child
            if left_option == None { //left child finish iteration, the end of output tuple
                return Ok(None);
            } 
            self.left_tuple = left_option.clone().unwrap();
            new_right_tuple = self.right_child.next().unwrap().unwrap();

        } else {
            new_right_tuple = right_option.unwrap();

        }
        // println!("{:?}", self.left_tuple.clone());
        // println!("{:?}", new_right_tuple.clone());

        let binding1 = self.left_tuple.clone();
        let binding2 = new_right_tuple.clone();
        let left_side = binding1.get_field(self.predicate.left_index.clone()).unwrap().unwrap_int_field();
        let right_side = binding2.get_field(self.predicate.right_index.clone()).unwrap().unwrap_int_field();
        //println!("relationship left {:?}, right {:?}, {:?}", left_side, right_side, (left_side < right_side));
        let join: bool = match self.predicate.op {
            SimplePredicateOp::Equals => left_side == right_side,
            SimplePredicateOp::GreaterThan => left_side > right_side,
            SimplePredicateOp::LessThan => (left_side < right_side),
            SimplePredicateOp::LessThanOrEq => left_side <= right_side,
            SimplePredicateOp::GreaterThanOrEq => left_side >= right_side,
            SimplePredicateOp::NotEq => left_side != right_side,
            SimplePredicateOp::All => true,

        };
        //println!("{:?} {:?}", self.predicate.op, join);
        if join {
            let right_return_tuple = new_right_tuple.clone();
            let ret = (self.left_tuple.clone()).merge(&right_return_tuple);
            //println!("{:?}", ret.clone());
            return Ok(Some(ret));
        } else {
            return self.next(); //recursively call the function
        }

    }
    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("join has not been opened")
        }       
        self.open = false;
        self.left_child.close()?;
        self.right_child.close()?;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("join has not been opened")
        }       
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        Ok(())
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}










/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,

    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,
    open: bool,
    schema: TableSchema,
    key_hash: HashMap<i32, Vec<Tuple>>,
    index: i32, //keep track of index of right child in key_hash
    cur_right_tuple: Tuple,
    cur_left_tuple_vec: Vec<Tuple>,
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        mut left_child: Box<dyn OpIterator>,
        mut right_child: Box<dyn OpIterator>,
    ) -> Self {
        left_child.open().unwrap();
        let t1 = left_child.next().unwrap().clone();
        let len1 = t1.clone().unwrap().size();
        left_child.rewind();
        left_child.close();
        right_child.open().unwrap();
        let t2 = right_child.next().unwrap().clone();
        let len2 = t2.clone().unwrap().size();
        right_child.rewind();
        right_child.close();
        let width = len1 + len2;
        let mut attrs = Vec::new();
        for _ in 0..width {
            attrs.push(Attribute::new(String::new(), DataType::Int))
        }
        let predicate:JoinPredicate = JoinPredicate::new(op, left_index, right_index);

        let schema = TableSchema::new(attrs);

        let new_hashjoin = HashEqJoin {
            predicate: predicate,
            left_child: left_child,
            right_child: right_child,
            open: false,
            schema: schema,
            key_hash: HashMap::new(),
            index: -1,
            cur_right_tuple: Tuple::new(Vec::new()),
            cur_left_tuple_vec: Vec::new(),
        };
        return new_hashjoin;
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.left_child.open()?;
        self.right_child.open()?; //right child is the one keep checking and appending
        //initiate the hashmap
        while let Some(t) = self.left_child.next()? {
            //println!("{:?}", t.clone());
            let key = t.clone().get_field(self.predicate.left_index).unwrap().unwrap_int_field();
            if self.key_hash.contains_key(&key.clone()) {
                let mut tuple_vec = self.key_hash.get(&key.clone()).unwrap().clone();
                tuple_vec.push(t.clone());
                self.key_hash.insert(key.clone(), tuple_vec);
            } else {
                self.key_hash.insert(key.clone(), vec!(t.clone()));
            }
        }
        //println!("hashmap {:?}", self.key_hash.clone());
        Ok(())        
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        let mut right_option;

        if self.index == -1 { //go to next right child
            right_option = self.right_child.next().unwrap();
            //println!("right option is {:?}", right_option.clone());

            if right_option == None { //end of right child, end of everything
                //println!("oh none");
                return Ok(None); 
            } else {
                self.cur_right_tuple = right_option.unwrap().clone();
                self.index = 0;
    
                //check if exist in hashmap
                let new_right_key = self.cur_right_tuple.clone().get_field(self.predicate.right_index).unwrap().clone().unwrap_int_field();
                if self.key_hash.contains_key(&new_right_key.clone()) {
                    self.cur_left_tuple_vec = self.key_hash.get(&new_right_key.clone()).unwrap().clone();
                } else {
                    self.index = -1;
                    return self.next();
                }                
            }
        }
        // println!("{:?}", self.index);
        // println!("{:?}", self.cur_left_tuple_vec.clone());

        let left_return_tuple = (self.cur_left_tuple_vec[self.index as usize]).clone();
        let ret = (left_return_tuple).merge(&self.cur_right_tuple.clone());

        //check if end of the left tuple, go to check next right child
        self.index += 1;
        if self.index == self.cur_left_tuple_vec.clone().len() as i32{ 
            self.index = -1;
            self.cur_left_tuple_vec = Vec::new();
        }
        
        //return
        //println!("{:?}", ret.clone());
        return Ok(Some(ret));
        //println!("{:?}", right_option);
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("join has not been opened")
        }       
        self.open = false;
        self.left_child.close()?;
        self.right_child.close()?;
        Ok(())    
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.open = true;
        }       
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        self.index = -1;
        if (self.key_hash.is_empty()) {
            while let Some(t) = self.left_child.next()? {
                let key = t.clone().get_field(self.predicate.left_index).unwrap().unwrap_int_field();
                if self.key_hash.contains_key(&key.clone()) {
                    let mut tuple_vec = self.key_hash.get(&key.clone()).unwrap().clone();
                    tuple_vec.push(t.clone());
                    self.key_hash.insert(key.clone(), tuple_vec);
                } else {
                    self.key_hash.insert(key.clone(), vec!(t.clone()));
                }
            }
        }
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2], 
            vec![3, 4], 
            vec![5, 6], 
            vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6],
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;

        let mut eq_join = eq_join();
        eq_join.open()?;

        let acutal = op.next()?;
        let expected = eq_join.next()?;
        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }
}
