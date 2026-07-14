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

use crate::reader::{ExternalSchemaStreamReader, StreamReader};
use crate::writer::ExternalSchemaStreamWriter;

#[test]
fn external_schema_stream_round_trip() -> Result<(), ArrowError> {
    let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Utf8, ["x", "y", "z"]))?;
    let schema = batch.schema();
    let mut bytes = Vec::new();
    {
        let mut writer = ExternalSchemaStreamWriter::try_new(&mut bytes, schema.clone())?;
        writer.write_arrays(batch.columns().to_vec())?;
        writer.finish()?;
    }

    assert!(StreamReader::try_new(Cursor::new(bytes.clone()), None).is_err());

    let reader =
        ExternalSchemaStreamReader::try_new(Cursor::new(bytes.clone()), schema.clone(), None)?;
    let batches = reader
        .map(|arrays| RecordBatch::try_new(schema.clone(), arrays?))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(batches, vec![batch]);

    let reader = ExternalSchemaStreamReader::try_new(Cursor::new(bytes), schema, Some(vec![1]))?;
    let projected = reader.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(projected.len(), 1);
    assert_eq!(projected[0].len(), 1);
    assert_eq!(projected[0][0].data_type(), &DataType::Utf8);

    Ok(())
}

#[test]
fn external_schema_stream_round_trip_dictionary_batch() -> Result<(), ArrowError> {
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
        let mut writer = ExternalSchemaStreamWriter::try_new(&mut bytes, schema.clone())?;
        writer.write_arrays(batch.columns().to_vec())?;
        writer.finish()?;
    }

    assert!(StreamReader::try_new(Cursor::new(bytes.clone()), None).is_err());

    let reader = ExternalSchemaStreamReader::try_new(Cursor::new(bytes), schema, None)?;
    let batches = reader
        .map(|arrays| RecordBatch::try_new(batch.schema(), arrays?))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(batches, vec![batch]);

    Ok(())
}
