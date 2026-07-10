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

use arrow_array::record_batch;
use arrow_schema::{ArrowError, DataType, Field, Schema};

use crate::reader::{StreamReader, read_stream_schema};
use crate::writer::{ExternalSchemaStreamWriter, write_stream_schema};

#[test]
fn stream_schema_round_trip() -> Result<(), ArrowError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]);

    let mut bytes = Vec::new();
    write_stream_schema(&mut bytes, &schema)?;

    let read_schema = read_stream_schema(Cursor::new(bytes.clone()))?;
    assert_eq!(read_schema.as_ref(), &schema);

    let mut stream_reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    assert_eq!(stream_reader.schema().as_ref(), &schema);
    assert!(stream_reader.next().is_none());

    Ok(())
}

#[test]
fn stream_schema_reader_rejects_record_batch_message() -> Result<(), ArrowError> {
    let batch = record_batch!(("a", Int32, [1, 2, 3]))?;
    let mut bytes = Vec::new();
    {
        let mut writer = ExternalSchemaStreamWriter::try_new(&mut bytes, batch.schema().as_ref())?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    assert!(read_stream_schema(Cursor::new(bytes)).is_err());

    Ok(())
}
