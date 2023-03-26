//! file: ipc.rs
//! author: Jacob Xie
//! date: 2023/03/26 08:06:20 Sunday
//! brief:

use std::io::Write;

use arrow2::chunk::Chunk;
use arrow2::io::ipc::read as ipc_read;
use arrow2::io::ipc::write as ipc_write;

use super::{ec::ReadSeek, FxIO, SimpleIO};
use crate::ab::{Congruent, Eclectic, FxSeq, Purport};
use crate::error::{FxError, FxResult};

// ================================================================================================
// Arrow
// ================================================================================================

impl FxIO {
    pub fn write_ipc<D: Eclectic + Purport, W: Write>(
        data: D,
        writer: W,
        options: Option<ipc_write::WriteOptions>,
    ) -> FxResult<()> {
        let schema = data.schema().clone();
        let default_options = ipc_write::WriteOptions { compression: None };
        let opt = options.unwrap_or(default_options);

        let mut writer = ipc_write::FileWriter::new(writer, schema, None, opt);

        let chunk = data
            .take_shortest_to_chunk()?
            .into_arrays()
            .into_iter()
            .map(|a| a.to_box_array())
            .collect::<FxResult<Vec<_>>>()?;
        let chunk = Chunk::new(chunk);

        writer.start()?;
        writer.write(&chunk, None)?;
        writer.finish()?;

        Ok(())
    }

    pub fn read_ipc<D: Eclectic + Purport, R: ReadSeek>(mut reader: R) -> FxResult<D> {
        let metadata = ipc_read::read_file_metadata(&mut reader)?;

        let reader = ipc_read::FileReader::new(&mut reader, metadata, None, None);

        let mut res = None;
        // only loop once, since ipc's file write only write one batch
        for maybe_chunk in reader {
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
    pub fn write_ipc(&mut self, options: Option<ipc_write::WriteOptions>) -> FxResult<()> {
        if self.data.is_none() || self.writer.is_none() {
            return Err(FxError::EmptyContent);
        }

        let writer = self.writer.take().unwrap();
        let data = self.take_data().unwrap();

        FxIO::write_ipc(data, writer, options)
    }

    pub fn read_ipc(&mut self) -> FxResult<()> {
        if self.reader.is_none() {
            return Err(FxError::EmptyContent);
        }

        let mut reader = self.reader.take().unwrap();

        self.data = Some(FxIO::read_ipc::<T, _>(&mut reader)?);

        Ok(())
    }
}

// ================================================================================================
// Test
// ================================================================================================

#[cfg(test)]
mod test_ipc {
    use crate::ab::FromSlice;
    use crate::arc_arr;
    use crate::cont::FxBatch;

    use super::*;

    const FILE_IPC: &str = "./cache/test.ipc";

    #[test]
    fn ipc_write_success() {
        let a = arc_arr!([Some(1), None, Some(3)]);
        let b = arc_arr!([Some(2.1), None, Some(6.2)]);
        let c = arc_arr!([Some("a"), Some("b"), Some("c")]);

        let data = FxBatch::new_with_names(vec![a, b, c], ["c1", "c2", "c3"]);
        println!("{:?}", data.schema());

        let mut file = std::fs::File::create(FILE_IPC).unwrap();

        FxIO::write_ipc(data, &mut file, None).expect("write success");
    }

    #[test]
    fn ipc_read_success() {
        let mut file = std::fs::File::open(FILE_IPC).unwrap();

        let res = FxIO::read_ipc::<FxBatch, _>(&mut file);
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

        let fw = simple.set_file_writer(FILE_IPC);
        assert!(fw.is_ok());

        let w = simple.write_ipc(None);
        assert!(w.is_ok());
    }

    #[test]
    fn simple_read() {
        let mut simple = SimpleIO::<FxBatch>::new();

        let fr = simple.set_file_reader(FILE_IPC);
        assert!(fr.is_ok());

        let r = simple.read_ipc();
        assert!(r.is_ok());

        let ref_data = simple.data();
        assert!(ref_data.is_some());

        println!("{:?}", ref_data.unwrap());
    }
}
