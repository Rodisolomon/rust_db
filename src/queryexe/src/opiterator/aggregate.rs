use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use serde::ser::Error;
use serde_cbor::tags::current_cbor_tag;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};


/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with. 
    pub op: AggOp,
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    ori_datatype: Vec<i32>, //keep track of the datatype of the field been aggregated, initiate as -1, 0 means i32, 1 means string; as same length of agg_field
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    groups: HashMap<String, Vec<Field>>,
    groups_count: HashMap<String, usize>,
    groups_lst: Vec<String>,//keep track of the order of groups
    num: usize, //keep track of the tot number of tuple added
    avg_lst: Vec<usize>, //keep track of the index in schema that is avg

}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        let mut avg_lst = Vec::new();
        let mut i = groupby_fields.len();
        for ag in agg_fields.clone() {
            let sub_result = match ag.op {
                AggOp::Avg => 1, //calculate as sum first, avg in the end
                AggOp::Count => 0,
                AggOp::Max => 0,
                AggOp::Min => 0,
                AggOp::Sum => 0,
            };
            if sub_result == 1 {
                avg_lst.push(i);
            }
            i += 1;
        }

        let length = groupby_fields.clone().len() + agg_fields.clone().len();

        let new_aggregator = Aggregator {
            agg_fields: agg_fields,
            ori_datatype: vec![-1; length],
            groupby_fields: groupby_fields,
            schema: schema.clone(),
            groups: HashMap::new(),
            groups_count: HashMap::new(),
            groups_lst: Vec::new(),
            num: 0,
            avg_lst: avg_lst,
        };
        return new_aggregator;
    }

    /// Handles the creation of groups for aggregation. handle group by
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    /// 

    fn Op_treatment(&mut self, tuple: &Tuple, exist: bool, cur_key: String) { //helper to update groups hashmap
        self.num += 1;
        //let length = self.groupby_fields.len().clone() + self.agg_fields.len().clone(); //lenght of the tuple
        // println!("{:?}", exist);
        // println!("{:?}", self.ori_datatype.clone());
        if exist { //key exist, update
            let mut new_vec = self.groups.get(&cur_key.clone()).unwrap().clone();
            let mut i = self.groupby_fields.len().clone(); //length of the groupby part
            let mut new_count = self.groups_count.get(&cur_key).unwrap().clone() + 1;
            self.groups_count.insert(cur_key.clone(), new_count); //update the number of object for this group. 

            for ag in self.agg_fields.clone() {
                //self.ori_datatype[i] must be initialized if this group exist
                let mut original_vec = self.groups.get(&cur_key.clone()).unwrap().clone();
                let mut original_val = original_vec[i].clone();

                if self.ori_datatype[i] == 1 { //string, only count, min or max
                    let val_string =  match original_val.clone() {
                        Field::IntField(x) => 0, // count
                        Field::StringField(s) => 1, //min or max
                    };
                    if val_string == 1 {
                        let mut str1 = original_val.unwrap_string_field().clone();
                        let mut new_val = tuple.get_field(ag.field).unwrap().unwrap_string_field().clone();
                        let maximum = str1.max(new_val).to_string();
                        let minimum = str1.min(new_val).to_string();
                        let sub_result: Field = match ag.op {
                            AggOp::Max => Field::StringField(maximum),
                            AggOp::Min => Field::StringField(minimum),
                            AggOp::Avg => todo!(),
                            AggOp::Count => todo!(),
                            AggOp::Sum => todo!(),
                        };
                        new_vec[i] = sub_result;
                    } else {
                        new_vec[i] = Field::IntField(new_count as i32); //count
                    }
                    i += 1;
                    continue;
                }
                let mut int_original_val = original_val.clone().unwrap_int_field();
                let mut new_val = tuple.get_field(ag.field).unwrap().clone().unwrap_int_field();
                let maximum;
                let minimum;
                if int_original_val.clone() > new_val.clone() { //determine max and min
                    maximum = int_original_val.clone();
                    minimum = new_val.clone();
 
                } else {
                    maximum = new_val.clone();
                    minimum = int_original_val.clone();
                }
                // let new_avg = (int_original_val*(new_count as i32 - 1) + new_val)/(new_count as i32);
                // println!("{:?}", (int_original_val*(new_count as i32 - 1) + new_val));
                let sub_result = match ag.op {
                    AggOp::Avg => int_original_val + new_val, //calculate as sum first, avg in the end
                    AggOp::Count => new_count as i32,
                    AggOp::Max => maximum,
                    AggOp::Min => minimum,
                    AggOp::Sum => int_original_val + new_val,
                };
                //println!("{:?} with original val {:?}, new val {:?}, and new count {} new result is {:?}", ag.op.clone(), int_original_val.clone(), new_val.clone(), new_count.clone(), sub_result.clone());
                new_vec[i] = Field::IntField(sub_result);
                i += 1;
            }
            self.groups.insert(cur_key, new_vec);
        } else { //doesn't exist before
            let mut new_vec = Vec::new();
            let mut i = 0;
            for gb in self.groupby_fields.clone() { //add groupby firstly
                let string =  match tuple.get_field(gb).unwrap() {
                    Field::IntField(x) => 0,
                    Field::StringField(s) => 1,
                };
                if string == 1 {
                    new_vec.push(Field::StringField(tuple.get_field(gb).unwrap().unwrap_string_field().to_string()));

                } else {
                    new_vec.push(Field::IntField(tuple.get_field(gb).unwrap().unwrap_int_field()));

                }
                i += 1;
            }

            //add aggregate field secondly
            for ag in self.agg_fields.clone() { 
                if self.ori_datatype[i] == -1 {//uninitiate
                    let string =  match tuple.get_field(ag.field).unwrap() {
                        Field::IntField(x) => 0,
                        Field::StringField(s) => 1,
                    };
                    self.ori_datatype[i] = string;
                }
                let value;
                if self.ori_datatype[i] == 1{  //string can only be aggregated about count, min, max
                    let sub_result = match ag.op {
                        AggOp::Avg => -1,
                        AggOp::Count => 1 as i32,
                        AggOp::Max => 2,
                        AggOp::Min => 3,
                        AggOp::Sum => -1,
                    };     
                    if sub_result == 1 { //count
                        new_vec.push(Field::IntField(1 as i32));

                    } else if sub_result == 2 || sub_result == 3 { //max or min
                        new_vec.push(tuple.get_field(ag.field).unwrap().clone());
                    }

                    i += 1;
                    continue;
                } else { //i32
                    value = tuple.get_field(ag.field).unwrap().unwrap_int_field();
                    self.ori_datatype[i] = 0;
                }
                i += 1;

                let sub_result = match ag.op {
                    AggOp::Avg => value,
                    AggOp::Count => 1 as i32,
                    AggOp::Max => value,
                    AggOp::Min => value,
                    AggOp::Sum => value,
                };         
                new_vec.push(Field::IntField(sub_result as i32));
                //println!("{:?}", new_vec.clone());
            }
            self.groups.insert(cur_key.clone(), new_vec);
            //println!("{:?}", self.groups.clone());
            self.groups_count.insert(cur_key.clone(), 1); //update the number of object for this group. 
            self.groups_lst.push(cur_key.clone());
            //println!("{:?}", self.groups_lst.clone());



        }

    }

    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        let mut cur_key = String::new();
        if self.groupby_fields.len() != 0 {
            for i in self.groupby_fields.clone() { //generate key from groupby fields eg. 0-1-2
                cur_key.push_str(&tuple.get_field(i).unwrap().to_string());
                if i < self.groupby_fields.len() - 1 {cur_key.push_str("-")}
            }
        } else { //there's no group
            cur_key.push_str("placeholder"); //in this case, there's only one key val pair in groups

        }
        //println!("{:?}", cur_key.clone());

        if self.groups.contains_key(&cur_key) {
            let exist = true;
            self.Op_treatment(tuple, exist, cur_key);
        } else {
            let exist = false;
            self.Op_treatment(tuple, exist, cur_key);
        }

    }


    // Treatment
    // pub enum AggOp {
    //     Avg, -> save as sum, compute in the end
    //     Count, -> plus 1
    //     Max, -> check
    //     Min, -> check
    //     Sum, -> sum
    // }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        let mut tuples = Vec::new();
        for key in self.groups_lst.clone() {
            let mut vec = self.groups.get(&key.clone()).unwrap().clone();
            //println!("avg lst {:?}", self.avg_lst.clone());
            for i in self.avg_lst.clone() { //do avg treatment
                let mut new_avg = vec[i].unwrap_int_field().clone() / (self.num.clone() as i32);
                //println!("{:?}/{:?} = {:?}", vec[i].unwrap_int_field().clone(), (self.num.clone() as i32), new_avg);
                vec[i] = Field::IntField(new_avg);
            }
            //println!("{:?}", vec.clone());
            let new_tuple = Tuple::new(vec);
            tuples.push(new_tuple.clone());
        }
        let new_iterator = TupleIterator::new(tuples, self.schema.clone());
        return new_iterator;
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: TupleIterator,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
    aggregator: Aggregator,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        let mut agg_fields = Vec::new();
        for i in 0..agg_indices.len() {
            agg_fields.push(AggregateField {field: agg_indices[i].clone(), op: ops[i].clone()});

        }
        let mut attribute_vec = Vec::new();
        for group in groupby_names {
            attribute_vec.push(Attribute::new(group.to_string(), DataType::Int));
        }
        for name in agg_names {
            // name,
            // dtype,
            // constraint: Constraint::None,
            attribute_vec.push(Attribute::new(name.to_string(), DataType::Int));
        }
        let schema = TableSchema::new(attribute_vec.clone());

        // agg_fields: Vec<AggregateField>,
        // groupby_fields: Vec<usize>,
        // schema: &TableSchema,
        let aggregator = Aggregator::new(agg_fields.clone(), groupby_indices.clone(), &schema);

        let new_aggregate = Self {
            groupby_fields: groupby_indices,
            /// Aggregation fields and corresponding aggregation functions.
            agg_fields: agg_fields,
            /// Aggregation iterators for results.
            agg_iter: TupleIterator::new(Vec::new(), TableSchema::new(Vec::new())), //placeholder
            /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
            schema: schema.clone(),
            /// Boolean if the iterator is open.
            open: false,
            /// Child operator to get the data from.
            child: child,    
            aggregator: aggregator,
        };
        return new_aggregate;
    }

}


impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.child.open();
        while let Some(t) = self.child.next()? {
            //println!("{:?}", t.clone());
            self.aggregator.merge_tuple_into_group(&t);
        }
        self.agg_iter = self.aggregator.iterator();
        self.agg_iter.open();
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        return self.agg_iter.next();
        
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }   
        self.open = false;
        self.child.close()?;
        self.agg_iter.close()?;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        self.close()?;
        self.open();
        self.agg_iter.rewind()?;
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

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);
            //println!("start running!");

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }
            //println!("{:?}", agg.groups.clone());

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }
            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
