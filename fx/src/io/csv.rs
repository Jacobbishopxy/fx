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
use crate::error::{FxError, FxResult};
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
        path: &str,
        projection: Option<&[usize]>,
    ) -> FxResult<()> {
        let mut reader = csv_read::ReaderBuilder::new()
            .from_path(path)
            .map_err(|_| FxError::InvalidArgument("path not found".to_owned()))?;
        let (fields, _) = csv_read::infer_schema(&mut reader, None, true, &csv_read::infer)?;

        let mut rows = vec![csv_read::ByteRecord::default(); 100];

        let rows_read = csv_read::read_rows(&mut reader, 0, &mut rows)?;
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
