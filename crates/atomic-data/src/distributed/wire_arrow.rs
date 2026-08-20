use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

use crate::error::DataResult;

pub fn encode_batch(batch: &RecordBatch) -> DataResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

pub fn decode_batch(bytes: &[u8]) -> DataResult<RecordBatch> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batches: Result<Vec<_>, _> = reader.collect();
    let mut batches = batches?;
    match batches.len() {
        1 => Ok(batches.remove(0)),
        0 => Err(crate::error::DataError::Other(
            "arrow IPC stream contained no batches".into(),
        )),
        n => Err(crate::error::DataError::Other(format!(
            "arrow IPC stream contained {n} batches, expected 1"
        ))),
    }
}

pub fn encode_batches(batches: &[RecordBatch]) -> DataResult<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batches[0].schema())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(buf)
}

pub fn decode_batches(bytes: &[u8]) -> DataResult<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    Ok(reader.collect::<Result<Vec<_>, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[test]
    fn round_trip_single() {
        let batch = sample_batch();
        let bytes = encode_batch(&batch).unwrap();
        let decoded = decode_batch(&bytes).unwrap();
        assert_eq!(batch, decoded);
    }

    #[test]
    fn round_trip_vec() {
        let batches = vec![sample_batch(), sample_batch()];
        let bytes = encode_batches(&batches).unwrap();
        let decoded = decode_batches(&bytes).unwrap();
        assert_eq!(batches, decoded);
    }

    #[test]
    fn round_trip_empty_vec() {
        let bytes = encode_batches(&[]).unwrap();
        let decoded = decode_batches(&bytes).unwrap();
        assert!(decoded.is_empty());
    }
}
