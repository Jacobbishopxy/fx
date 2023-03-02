//! file: parquet.rs
//! author: Jacob Xie
//! date: 2023/01/18 20:12:29 Wednesday
//! brief: Parquet I/O

use std::io::{Read, Seek, Write};
use std::sync::Arc;

use arrow2::io::parquet::read as parquet_read;
use arrow2::io::parquet::write as parquet_write;

use super::FxIO;
use crate::ab::{Congruent, Eclectic, EclecticMutChunk, Purport};
use crate::error::FxResult;

// ================================================================================================
// Parquet
// ================================================================================================

impl FxIO {
    // single threaded
    pub fn write_parquet<W: Write, D: Eclectic + Purport + Congruent>(
        data: D,
        writer: &mut W,
        compression: parquet_write::CompressionOptions,
    ) -> FxResult<()> {
        let options = parquet_write::WriteOptions {
            write_statistics: true,
            compression,
            version: parquet_write::Version::V2,
            data_pagesize_limit: None,
        };

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

        let mut fw = parquet_write::FileWriter::try_new(writer, schema.clone(), options)?;

        for group in row_groups {
            fw.write(group?)?;
        }

        let _size = fw.end(None)?;

        Ok(())
    }

    pub fn read_parquet<R: Read + Seek, D: Eclectic + Purport + EclecticMutChunk>(
        data: &mut D,
        reader: &mut R,
    ) -> FxResult<()> {
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

        // TODO: simplify
        for maybe_chunk in chunks {
            let chunk = maybe_chunk?
                .into_arrays()
                .into_iter()
                .map(Arc::from)
                .collect::<Vec<_>>();

            data.try_extent(&chunk)?;
        }

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_parquet {

    use crate::ab::FromSlice;
    use crate::cont::{ArcArr, FxBatch};

    use super::*;

    const FILE_PARQUET: &str = "./cache/test.parquet";

    #[test]
    fn parquet_write_success() {
        let a = ArcArr::from_slice(&[Some(1), None, Some(3)]);
        let b = ArcArr::from_slice(&[Some(2.1), None, Some(6.2)]);
        let c = ArcArr::from_slice(&[Some("a"), Some("b"), Some("c")]);

        let data = FxBatch::new_with_names(vec![a, b, c], ["c1", "c2", "c3"]);

        let mut file = std::fs::File::create(FILE_PARQUET).unwrap();

        FxIO::write_parquet(
            data,
            &mut file,
            parquet_write::CompressionOptions::Uncompressed,
        )
        .expect("write success");
    }

    // #[test]
    // fn parquet_read_success() {
    //     // let schema =
    //     let mut grid = FxBatch::empty_with_schema();

    //     let mut file = std::fs::File::open(FILE_PARQUET).unwrap();

    //     FxIO::read_parquet(&mut grid, &mut file).unwrap();

    //     let data_types = grid
    //         .0
    //         .arrays()
    //         .iter()
    //         .map(|a| a.data_type())
    //         .collect::<Vec<_>>();
    //     println!("{data_types:?}");
    // }
}
