//! file:	io.rs
//! author:	Jacob Xie
//! date:	2023/01/15 00:14:54 Sunday
//! brief:	I/O

use std::io::{Read, Seek, Write};
use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::avro::avro_schema;
use arrow2::io::avro::read as avro_read;
use arrow2::io::avro::write as avro_write;
use arrow2::io::parquet::read as parquet_read;
use arrow2::io::parquet::write as parquet_write;

use crate::{Datagrid, FxResult};

impl Datagrid {
    pub fn write_avro<W: Write>(
        &self,
        writer: &mut W,
        schema: &Schema,
        compression: Option<avro_schema::file::Compression>,
    ) -> FxResult<()> {
        let record = avro_write::to_record(schema)?;
        let arrays = self.0.arrays();

        let mut serializers = arrays
            .iter()
            .zip(record.fields.iter())
            .map(|(array, field)| avro_write::new_serializer(array.as_ref(), &field.schema))
            .collect::<Vec<_>>();
        let mut block = avro_schema::file::Block::new(arrays[0].as_ref().len(), vec![]);

        avro_write::serialize(&mut serializers, &mut block);

        let mut compressed_block = avro_schema::file::CompressedBlock::default();

        let _was_compressed =
            avro_schema::write::compress(&mut block, &mut compressed_block, compression)?;

        avro_schema::write::write_metadata(writer, record, compression)?;

        avro_schema::write::write_block(writer, &compressed_block)?;

        Ok(())
    }

    pub fn read_avro<R: Read>(&mut self, reader: &mut R) -> FxResult<()> {
        let metadata = avro_schema::read::read_metadata(reader)?;

        let schema = avro_read::infer_schema(&metadata.record)?;

        let mut blocks = avro_read::Reader::new(reader, metadata, schema.fields, None);

        if let Some(Ok(c)) = blocks.next() {
            let vec_arc_dyn_arr = c
                .into_arrays()
                .into_iter()
                .map(|e| Arc::<dyn Array>::from(e))
                .collect::<Vec<_>>();

            self.0 = Chunk::new(vec_arc_dyn_arr);
        }

        Ok(())
    }

    pub fn write_parquet<W: Write>(
        &self,
        writer: &mut W,
        schema: &Schema,
        compression: parquet_write::CompressionOptions,
    ) -> FxResult<()> {
        let options = parquet_write::WriteOptions {
            write_statistics: true,
            compression,
            version: parquet_write::Version::V2,
            data_pagesize_limit: None,
        };

        let iter = vec![Ok(self.0.clone())];

        let encodings = schema
            .fields
            .iter()
            .map(|f| parquet_write::transverse(f.data_type(), |_| parquet_write::Encoding::Plain))
            .collect();

        let row_groups =
            parquet_write::RowGroupIterator::try_new(iter.into_iter(), schema, options, encodings)?;

        let mut fw = parquet_write::FileWriter::try_new(writer, schema.clone(), options)?;

        for group in row_groups {
            fw.write(group?)?;
        }

        let _size = fw.end(None)?;

        Ok(())
    }

    pub fn read_parquet<R: Read + Seek>(&mut self, reader: &mut R) -> FxResult<()> {
        let metadata = parquet_read::read_metadata(reader)?;

        let schema = parquet_read::infer_schema(&metadata)?;

        for field in &schema.fields {
            let _statistics = parquet_read::statistics::deserialize(field, &metadata.row_groups)?;
        }

        let row_groups = metadata.row_groups;

        let chunks = parquet_read::FileReader::new(
            reader,
            row_groups,
            schema,
            Some(1024 * 8 * 8),
            None,
            None,
        );

        for maybe_chunk in chunks {
            let vec_arc_dyn_arr = maybe_chunk?
                .into_arrays()
                .into_iter()
                .map(|e| Arc::<dyn Array>::from(e))
                .collect::<Vec<_>>();

            self.0 = Chunk::new(vec_arc_dyn_arr);
        }

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_io {

    use super::*;

    const FILE_AVRO: &str = "./cache/test.avro";
    const FILE_PARQUET: &str = "./cache/test.parquet";

    #[test]
    fn avro_write_success() {
        let a = Int32Array::from([Some(1), None, Some(3)]).arced();
        let b = Float32Array::from([Some(2.1), None, Some(6.2)]).arced();
        let c = Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).arced();

        let datagrid = Datagrid::new(vec![a, b, c]);
        let schema = datagrid.gen_schema(&["c1", "c2", "c3"]).unwrap();

        let mut file = std::fs::File::create(FILE_AVRO).unwrap();

        datagrid
            .write_avro(&mut file, &schema, None)
            .expect("write success")
    }

    #[test]
    fn avro_read_success() {
        let mut datagrid = Datagrid::empty();

        let mut file = std::fs::File::open(FILE_AVRO).unwrap();

        datagrid.read_avro(&mut file).unwrap();

        let data_types = datagrid
            .0
            .arrays()
            .iter()
            .map(|a| a.data_type())
            .collect::<Vec<_>>();
        println!("{data_types:?}");
    }

    #[test]
    fn parquet_write_success() {
        let a = Int32Array::from([Some(1), None, Some(3)]).arced();
        let b = Float32Array::from([Some(2.1), None, Some(6.2)]).arced();
        let c = Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).arced();

        let datagrid = Datagrid::new(vec![a, b, c]);
        let schema = datagrid.gen_schema(&["c1", "c2", "c3"]).unwrap();

        let mut file = std::fs::File::create(FILE_PARQUET).unwrap();

        datagrid
            .write_parquet(
                &mut file,
                &schema,
                parquet_write::CompressionOptions::Uncompressed,
            )
            .expect("write success");
    }

    #[test]
    fn parquet_read_success() {
        let mut datagrid = Datagrid::empty();

        let mut file = std::fs::File::open(FILE_PARQUET).unwrap();

        datagrid.read_parquet(&mut file).unwrap();

        let data_types = datagrid
            .0
            .arrays()
            .iter()
            .map(|a| a.data_type())
            .collect::<Vec<_>>();
        println!("{data_types:?}");
    }
}
