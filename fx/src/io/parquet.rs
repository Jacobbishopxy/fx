//! file: parquet.rs
//! author: Jacob Xie
//! date: 2023/01/18 20:12:29 Wednesday
//! brief: Parquet I/O

use std::io::Write;

use arrow2::io::parquet::read as parquet_read;
use arrow2::io::parquet::write as parquet_write;

use super::{ec::ReadSeek, FxIO, SimpleIO};
use crate::ab::{Congruent, Eclectic, FxSeq, Purport};
use crate::error::FxError;
use crate::error::FxResult;

// ================================================================================================
// Parquet
// ================================================================================================

impl FxIO {
    // single threaded
    pub fn write_parquet<D: Eclectic + Purport, W: Write>(
        data: D,
        mut writer: W,
        options: Option<parquet_write::WriteOptions>,
    ) -> FxResult<()> {
        let default_options = parquet_write::WriteOptions {
            write_statistics: true,
            compression: parquet_write::CompressionOptions::Uncompressed,
            version: parquet_write::Version::V2,
            data_pagesize_limit: None,
        };
        let options = options.unwrap_or(default_options);

        let schema = data.schema().clone();

        let encodings = data
            .schema()
            .fields
            .iter()
            .map(|f| parquet_write::transverse(f.data_type(), |_| parquet_write::Encoding::Plain))
            .collect::<Vec<_>>();

        let iter = vec![Ok(data.take_shortest_to_chunk()?)];

        let row_groups = parquet_write::RowGroupIterator::try_new(
            iter.into_iter(),
            &schema,
            options,
            encodings,
        )?;

        let mut fw = parquet_write::FileWriter::try_new(&mut writer, schema.clone(), options)?;

        for group in row_groups {
            fw.write(group?)?;
        }

        let _size = fw.end(None)?;

        Ok(())
    }

    pub fn read_parquet<D: Eclectic + Purport, R: ReadSeek>(reader: &mut R) -> FxResult<D> {
        let metadata = parquet_read::read_metadata(reader)?;

        let schema = parquet_read::infer_schema(&metadata)?;

        for field in &schema.fields {
            let _statistics = parquet_read::statistics::deserialize(field, &metadata.row_groups)?;
        }

        let row_groups = metadata.row_groups;

        let chunks = parquet_read::FileReader::new(reader, row_groups, schema, None, None, None);

        let mut res = None;
        // only loop once, since parquet's file reader has no size limit
        for maybe_chunk in chunks {
            let data = maybe_chunk?
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
    pub fn write_parquet(&mut self, options: Option<parquet_write::WriteOptions>) -> FxResult<()> {
        if self.data.is_none() || self.writer.is_none() {
            return Err(FxError::EmptyContent);
        }

        let writer = self.writer.take().unwrap();
        let data = self.take_data().unwrap();

        FxIO::write_parquet(data, writer, options)
    }

    pub fn read_parquet(&mut self) -> FxResult<()> {
        if self.reader.is_none() {
            return Err(FxError::EmptyContent);
        }

        let mut reader = self.reader.take().unwrap();

        self.data = Some(FxIO::read_parquet::<T, _>(&mut reader)?);

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_parquet {

    use crate::ab::FromSlice;
    use crate::arc_arr;
    use crate::cont::FxBatch;

    use super::*;

    const FILE_PARQUET: &str = "./cache/test.parquet";

    #[test]
    fn parquet_write_success() {
        let a = arc_arr!([Some(1), None, Some(3)]);
        let b = arc_arr!([Some(2.1), None, Some(6.2)]);
        let c = arc_arr!([Some("a"), Some("b"), Some("c")]);

        let data = FxBatch::new_with_names(vec![a, b, c], ["c1", "c2", "c3"]);
        println!("{:?}", data.schema());

        let mut file = std::fs::File::create(FILE_PARQUET).unwrap();

        FxIO::write_parquet(data, &mut file, None).expect("write success");
    }

    #[test]
    fn parquet_read_success() {
        let mut file = std::fs::File::open(FILE_PARQUET).unwrap();

        let res = FxIO::read_parquet::<FxBatch, _>(&mut file);
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

        let fw = simple.set_file_writer(FILE_PARQUET);
        assert!(fw.is_ok());

        let w = simple.write_parquet(None);
        assert!(w.is_ok());
    }

    #[test]
    fn simple_read() {
        let mut simple = SimpleIO::<FxBatch>::new();

        let fr = simple.set_file_reader(FILE_PARQUET);
        assert!(fr.is_ok());

        let r = simple.read_parquet();
        assert!(r.is_ok());

        let ref_data = simple.data();
        assert!(ref_data.is_some());

        println!("{:?}", ref_data.unwrap());
    }
}
