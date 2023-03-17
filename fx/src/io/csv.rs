//! file: csv.rs
//! author: Jacob Xie
//! date: 2023/03/13 22:53:42 Monday
//! brief: CSV I/O

use std::io::Write;
use std::ops::Deref;

use arrow2::io::csv::read as csv_read;
use arrow2::io::csv::write as csv_write;

use super::{ec::ReadSeek, FxIO, SimpleIO};
use crate::ab::{Congruent, Eclectic, FxSeq, Purport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// CSV
// ================================================================================================

impl FxIO {
    pub fn write_csv<D: Eclectic + Purport, W: Write>(
        data: D,
        writer: &mut W,
        options: Option<&csv_write::SerializeOptions>,
    ) -> FxResult<()> {
        let names = data.names();
        let default_opt = csv_write::SerializeOptions::default();
        let opt = options.unwrap_or(&default_opt);

        csv_write::write_header(writer, &names, opt)?;

        let columns = data.take_shortest_to_chunk()?;
        csv_write::write_chunk(writer, &columns, opt)?;

        Ok(())
    }

    pub fn read_csv<D: Eclectic + Purport, R: ReadSeek>(
        reader: R,
        projection: Option<&[usize]>,
    ) -> FxResult<D> {
        let mut cr = csv_read::ReaderBuilder::new().from_reader(reader);
        let (fields, s) = csv_read::infer_schema(&mut cr, None, true, &csv_read::infer)?;

        let mut rows = vec![csv_read::ByteRecord::default(); s];

        let rows_read = csv_read::read_rows(&mut cr, 0, &mut rows)?;
        let rows = &rows[..rows_read];

        let res = csv_read::deserialize_batch(
            rows,
            &fields,
            projection,
            0,
            csv_read::deserialize_column,
        )?
        .arrays()
        .iter()
        .map(|a| FxSeq::from_ref(a.deref()))
        .collect::<Vec<_>>();

        D::from_vec_seq(res)
    }
}

// ================================================================================================
// SimpleIO
// ================================================================================================

impl<T: Eclectic + Purport> SimpleIO<T> {
    // notice after writing complete, data & writer both turn to None
    pub fn write_csv(&mut self, options: Option<&csv_write::SerializeOptions>) -> FxResult<()> {
        if self.data.is_none() || self.writer.is_none() {
            return Err(FxError::EmptyContent);
        }

        let mut writer = self.writer.take().unwrap();
        let data = self.take_data().unwrap();

        FxIO::write_csv(data, &mut writer, options)
    }

    pub fn read_csv(&mut self, projection: Option<&[usize]>) -> FxResult<()> {
        if self.reader.is_none() {
            return Err(FxError::EmptyContent);
        }

        let reader = self.reader.take().unwrap();

        self.data = Some(FxIO::read_csv::<T, _>(reader, projection)?);

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_csv {

    use crate::ab::FromSlice;
    use crate::cont::{ArcArr, FxBatch};

    use super::*;

    const FILE_CSV: &str = "./cache/test.csv";

    #[test]
    fn csv_write_success() {
        let a = ArcArr::from_slice(&[Some(1), None, Some(3)]);
        let b = ArcArr::from_slice(&[Some(2.1), None, Some(6.2)]);
        let c = ArcArr::from_slice(&[Some("a"), Some("b"), Some("c")]);

        let data = FxBatch::new_with_names(vec![a, b, c], ["c1", "c2", "c3"]);
        println!("{:?}", data.schema());

        let mut file = std::fs::File::create(FILE_CSV).unwrap();

        FxIO::write_csv(data, &mut file, None).expect("write success");
    }

    #[test]
    fn csv_read_success() {
        let mut file = std::fs::File::open(FILE_CSV).unwrap();

        let res = FxIO::read_csv::<FxBatch, _>(&mut file, None);
        assert!(res.is_ok());

        println!("{:?}", res.unwrap());
    }

    #[test]
    fn simple_write() {
        let arrays = vec![
            ArcArr::from_slice(&["a", "c", "x"]),
            ArcArr::from_slice(&[Some("x"), None, Some("y")]),
            ArcArr::from_slice(&[Some(2.1), None, Some(6.2)]),
            ArcArr::from_slice(&[true, false, false]),
        ];
        let batch = FxBatch::new(arrays);

        let mut simple = SimpleIO::new_with_data(batch);

        let fw = simple.set_file_writer(FILE_CSV);
        assert!(fw.is_ok());

        let w = simple.write_csv(None);
        assert!(w.is_ok());
    }

    #[test]
    fn simple_read() {
        let mut simple = SimpleIO::<FxBatch>::new();

        let fr = simple.set_file_reader(FILE_CSV);
        assert!(fr.is_ok());

        let r = simple.read_csv(None);
        assert!(r.is_ok());

        let ref_data = simple.data();
        assert!(ref_data.is_some());

        println!("{:?}", ref_data.unwrap());
    }
}
