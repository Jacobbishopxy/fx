//! file: arvo.rs
//! author: Jacob Xie
//! date: 2023/01/18 20:03:41 Wednesday
//! brief: Arvo I/O

use std::io::{Read, Write};
use std::sync::Arc;

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::avro::avro_schema;
use arrow2::io::avro::read as avro_read;
use arrow2::io::avro::write as avro_write;

use crate::cont::ab::Chunking;
use crate::{FxGrid, FxIO, FxResult};

// TODO generic FxGrid

impl FxIO {
    pub fn write_avro<W: Write>(
        data: &FxGrid,
        writer: &mut W,
        schema: &Schema,
        compression: Option<avro_schema::file::Compression>,
    ) -> FxResult<()> {
        let record = avro_write::to_record(schema)?;
        let arrays = data.arrays();

        let mut serializers = arrays
            .iter()
            .zip(record.fields.iter())
            .map(|(array, field)| avro_write::new_serializer(array.as_ref(), &field.schema))
            .collect::<Vec<_>>();
        let mut block = avro_schema::file::Block::new(arrays[0].len(), vec![]);

        avro_write::serialize(&mut serializers, &mut block);

        let mut compressed_block = avro_schema::file::CompressedBlock::default();

        let _was_compressed =
            avro_schema::write::compress(&mut block, &mut compressed_block, compression)?;

        avro_schema::write::write_metadata(writer, record, compression)?;

        avro_schema::write::write_block(writer, &compressed_block)?;

        Ok(())
    }

    pub fn read_avro<R: Read>(data: &mut FxGrid, reader: &mut R) -> FxResult<()> {
        let metadata = avro_schema::read::read_metadata(reader)?;

        let schema = avro_read::infer_schema(&metadata.record)?;

        let mut blocks = avro_read::Reader::new(reader, metadata, schema.fields, None);

        if let Some(Ok(c)) = blocks.next() {
            let vec_arc_dyn_arr = c
                .into_arrays()
                .into_iter()
                .map(Arc::<dyn Array>::from)
                .collect::<Vec<_>>();

            data.0 = Chunk::new(vec_arc_dyn_arr);
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_arvo {

    use super::*;

    const FILE_AVRO: &str = "./cache/test.avro";

    #[test]
    fn avro_write_success() {
        let a = Int32Array::from([Some(1), None, Some(3)]).arced();
        let b = Float32Array::from([Some(2.1), None, Some(6.2)]).arced();
        let c = Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).arced();

        let grid = FxGrid::new(vec![a, b, c]);
        let schema = grid.gen_schema(&["c1", "c2", "c3"]).unwrap();

        let mut file = std::fs::File::create(FILE_AVRO).unwrap();

        FxIO::write_avro(&grid, &mut file, &schema, None).expect("write success")
    }

    #[test]
    fn avro_read_success() {
        let mut grid = FxGrid::empty();

        let mut file = std::fs::File::open(FILE_AVRO).unwrap();

        FxIO::read_avro(&mut grid, &mut file).unwrap();

        let data_types = grid
            .0
            .arrays()
            .iter()
            .map(|a| a.data_type())
            .collect::<Vec<_>>();
        println!("{data_types:?}");
    }
}
