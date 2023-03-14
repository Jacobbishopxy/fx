//! file: csv.rs
//! author: Jacob Xie
//! date: 2023/03/13 22:53:42 Monday
//! brief: CSV I/O

use std::io::{Read, Seek, Write};
use std::sync::Arc;

use arrow2::io::csv::read as csv_read;
use arrow2::io::csv::write as csv_write;

use super::FxIO;
use crate::ab::{Congruent, Eclectic, Purport};
use crate::error::FxResult;
use crate::row_builder::EclecticMutChunk;

// ================================================================================================
// CSV
// ================================================================================================

impl FxIO {
    pub fn write_csv<W: Write, D: Eclectic + Purport + Congruent>(
        data: D,
        writer: &mut W,
        options: csv_write::SerializeOptions,
    ) -> FxResult<()> {
        let names = data.names();

        csv_write::write_header(writer, &names, &options)?;

        let columns = data.take_shortest_to_chunk()?;
        csv_write::write_chunk(writer, &columns, &options)?;

        Ok(())
    }

    pub fn read_csv<R: Read + Seek, D: Eclectic + Purport + EclecticMutChunk>(
        data: &mut D,
        reader: R,
        projection: Option<&[usize]>,
    ) -> FxResult<()> {
        let mut cr = csv_read::ReaderBuilder::new().from_reader(reader);
        let (fields, _) = csv_read::infer_schema(&mut cr, None, true, &csv_read::infer)?;

        let mut rows = vec![csv_read::ByteRecord::default(); 100];

        let rows_read = csv_read::read_rows(&mut cr, 0, &mut rows)?;
        let rows = &rows[..rows_read];

        let res = csv_read::deserialize_batch(
            rows,
            &fields,
            projection,
            0,
            csv_read::deserialize_column,
        )?;
        let chunk = res
            .into_arrays()
            .into_iter()
            .map(Arc::from)
            .collect::<Vec<_>>();

        data.try_extent(&chunk)?;

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_csv {
    use arrow2::datatypes::{DataType, Field, Schema};

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

        FxIO::write_csv(data, &mut file, csv_write::SerializeOptions::default())
            .expect("write success");
    }

    #[test]
    fn csv_read_success() {
        let schema = Schema::from(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c1", DataType::Utf8, false),
        ]);
        let mut batch = FxBatch::empty_with_schema(schema);

        let mut file = std::fs::File::open(FILE_CSV).unwrap();

        let res = FxIO::read_csv(&mut batch, &mut file, None);
        assert!(res.is_ok());

        println!("{batch:?}");
    }
}
