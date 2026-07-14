// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::StringDictionaryBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, RecordBatch, record_batch};
use arrow_schema::{ArrowError, DataType, Field, Schema};

use crate::reader::{ExternalSchemaFileReader, FileReader};
use crate::writer::ExternalSchemaFileWriter;

#[test]
fn external_schema_file_round_trip_random_access() -> Result<(), ArrowError> {
    let batch1 = record_batch!(("a", Int32, [1, 2, 3]), ("b", Utf8, ["x", "y", "z"]))?;
    let batch2 = record_batch!(("a", Int32, [4, 5, 6]), ("b", Utf8, ["i", "j", "k"]))?;
    let schema = batch1.schema();

    let mut bytes = Vec::new();
    {
        let mut writer = ExternalSchemaFileWriter::try_new(&mut bytes, schema.clone())?;
        writer.write_arrays(batch1.columns().to_vec())?;
        writer.write_arrays(batch2.columns().to_vec())?;
        writer.finish()?;
    }

    assert!(
        std::panic::catch_unwind(|| FileReader::try_new(Cursor::new(bytes.clone()), None)).is_err()
    );

    let mut reader =
        ExternalSchemaFileReader::try_new(Cursor::new(bytes.clone()), schema.clone(), None)?;
    assert_eq!(reader.num_batches(), 2);

    reader.set_index(1)?;
    let arrays = reader.next().transpose()?.unwrap();
    let back = RecordBatch::try_new(schema.clone(), arrays)?;
    assert_eq!(back, batch2);

    reader.set_index(0)?;
    let arrays = reader.next().transpose()?.unwrap();
    let back = RecordBatch::try_new(schema.clone(), arrays)?;
    assert_eq!(back, batch1);

    let projected = ExternalSchemaFileReader::try_new(Cursor::new(bytes), schema, Some(vec![1]))?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].len(), 1);
    assert_eq!(projected[0][0].data_type(), &DataType::Utf8);

    Ok(())
}

#[test]
fn external_schema_file_round_trip_dictionary_batch() -> Result<(), ArrowError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));

    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    builder.append("a").unwrap();
    builder.append("b").unwrap();
    builder.append("a").unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish()) as ArrayRef])?;

    let mut bytes = Vec::new();
    {
        let mut writer = ExternalSchemaFileWriter::try_new(&mut bytes, schema.clone())?;
        writer.write_arrays(batch.columns().to_vec())?;
        writer.finish()?;
    }

    assert!(
        std::panic::catch_unwind(|| FileReader::try_new(Cursor::new(bytes.clone()), None)).is_err()
    );

    let reader = ExternalSchemaFileReader::try_new(Cursor::new(bytes), schema, None)?;
    let batches = reader
        .map(|arrays| RecordBatch::try_new(batch.schema(), arrays?))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(batches, vec![batch]);

    Ok(())
}
