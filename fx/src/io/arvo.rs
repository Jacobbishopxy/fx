//! file: arvo.rs
//! author: Jacob Xie
//! date: 2023/01/18 20:03:41 Wednesday
//! brief: Arvo I/O

use std::io::Write;

use arrow2::io::avro::avro_schema;
use arrow2::io::avro::read as avro_read;
use arrow2::io::avro::write as avro_write;

use super::{ec::ReadSeek, FxIO, SimpleIO};
use crate::ab::{Congruent, Eclectic, FxSeq, Purport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// Arvo
// ================================================================================================

impl FxIO {
    pub fn write_avro<D: Eclectic + Purport, W: Write>(
        data: D,
        mut writer: W,
        options: Option<avro_schema::file::Compression>,
    ) -> FxResult<()> {
        let record = avro_write::to_record(data.schema())?;
        let arrays = data.take_shortest_to_chunk()?;

        let mut serializers = arrays
            .iter()
            .zip(record.fields.iter())
            .map(|(array, field)| avro_write::new_serializer(array.as_ref(), &field.schema))
            .collect::<Vec<_>>();
        let mut block = avro_schema::file::Block::new(arrays[0].len(), vec![]);

        avro_write::serialize(&mut serializers, &mut block);

        let mut compressed_block = avro_schema::file::CompressedBlock::default();

        let _was_compressed =
            avro_schema::write::compress(&mut block, &mut compressed_block, options)?;

        avro_schema::write::write_metadata(&mut writer, record, options)?;

        avro_schema::write::write_block(&mut writer, &compressed_block)?;

        Ok(())
    }

    pub fn read_avro<D: Eclectic + Purport, R: ReadSeek>(reader: &mut R) -> FxResult<D> {
        let metadata = avro_schema::read::read_metadata(reader)?;

        let schema = avro_read::infer_schema(&metadata.record)?;

        let mut blocks = avro_read::Reader::new(reader, metadata, schema.fields, None);

        let mut res = None;
        if let Some(Ok(c)) = blocks.next() {
            let data = c
                .into_arrays()
                .into_iter()
                .map(FxSeq::from_box_arr)
                .collect::<Vec<_>>();

            res = Some(D::from_vec_seq(data));
        }

        res.unwrap()
    }
}

// ================================================================================================
// SimpleIO
// ================================================================================================

impl<T: Eclectic + Purport> SimpleIO<T> {
    pub fn write_arvo(&mut self, options: Option<avro_schema::file::Compression>) -> FxResult<()> {
        if self.data.is_none() || self.writer.is_none() {
            return Err(FxError::EmptyContent);
        }

        let writer = self.writer.take().unwrap();
        let data = self.take_data().unwrap();

        FxIO::write_avro(data, writer, options)
    }

    pub fn read_arvo(&mut self) -> FxResult<()> {
        if self.reader.is_none() {
            return Err(FxError::EmptyContent);
        }

        let mut reader = self.reader.take().unwrap();

        self.data = Some(FxIO::read_avro::<T, _>(&mut reader)?);

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_arvo {

    use crate::ab::FromSlice;
    use crate::arc_arr;
    use crate::cont::FxBatch;

    use super::*;

    const FILE_AVRO: &str = "./cache/test.avro";

    #[test]
    fn avro_write_success() {
        let a = arc_arr!([Some(1), None, Some(3)]);
        let b = arc_arr!([Some(2.1), None, Some(6.2)]);
        let c = arc_arr!([Some("a"), Some("b"), Some("c")]);

        let data = FxBatch::new_with_names(vec![a, b, c], ["c1", "c2", "c3"]);
        println!("{:?}", data.schema());

        let mut file = std::fs::File::create(FILE_AVRO).unwrap();

        FxIO::write_avro(data, &mut file, None).expect("write success")
    }

    #[test]
    fn avro_read_success() {
        let mut file = std::fs::File::open(FILE_AVRO).unwrap();

        let res = FxIO::read_avro::<FxBatch, _>(&mut file);
        assert!(res.is_ok());

        println!("{:?}", res.unwrap());
    }

    #[test]
    fn simple_write() {
        let arrays = vec![
            arc_arr!(["a", "c", "x"]),
            arc_arr!([Some("x"), None, Some("y")]),
            arc_arr!([Some(2.1), None, Some(6.2)]),
            arc_arr!([true, false, false]),
        ];
        let batch = FxBatch::new(arrays);

        let mut simple = SimpleIO::new_with_data(batch);

        let fw = simple.set_file_writer(FILE_AVRO);
        assert!(fw.is_ok());

        let w = simple.write_arvo(None);
        assert!(w.is_ok());
    }

    #[test]
    fn simple_read() {
        let mut simple = SimpleIO::<FxBatch>::new();

        let fr = simple.set_file_reader(FILE_AVRO);
        assert!(fr.is_ok());

        let r = simple.read_arvo();
        assert!(r.is_ok());

        let ref_data = simple.data();
        assert!(ref_data.is_some());

        println!("{:?}", ref_data.unwrap());
    }
}
