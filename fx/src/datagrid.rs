//! Datagrid

use std::io::{Read, Seek, Write};

use arrow2::array::*;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{Field, Schema};
use arrow2::io::avro::avro_schema;
use arrow2::io::avro::read as avro_read;
use arrow2::io::avro::write as avro_write;
use arrow2::io::parquet::read as parquet_read;
use arrow2::io::parquet::write as parquet_write;

use crate::{FxArray, FxError, FxResult};

// ================================================================================================
// Datagrid
// ================================================================================================

#[derive(Debug)]
pub struct Datagrid(Chunk<Box<dyn Array>>);

impl Datagrid {
    pub fn empty() -> Self {
        Datagrid(Chunk::new(vec![]))
    }

    // WARNING: arrays with different length will cause runtime panic!!!
    pub fn new(arrays: Vec<Box<dyn Array>>) -> Self {
        Datagrid(Chunk::new(arrays))
    }

    pub fn try_new(arrays: Vec<Box<dyn Array>>) -> FxResult<Self> {
        let chunk = Chunk::try_new(arrays)?;
        Ok(Datagrid(chunk))
    }

    pub fn gen_schema(&self, names: &[&str]) -> FxResult<Schema> {
        let arrays = self.0.arrays();
        let al = arrays.len();
        let nl = names.len();
        if al != nl {
            return Err(FxError::InvalidArgument(format!(
                "length does not match: names.len ${nl} & arrays.len ${al}"
            )));
        }

        let fld = names
            .iter()
            .zip(arrays)
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), a.null_count() > 0))
            .collect::<Vec<_>>();

        Ok(Schema::from(fld))
    }

    pub fn into_arrays(self) -> Vec<Box<dyn Array>> {
        self.0.into_arrays()
    }

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
            self.0 = c;
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
            let chunk = maybe_chunk?;
            self.0 = chunk;
        }

        Ok(())
    }
}

// ================================================================================================
// DatagridColWiseBuilder
// ================================================================================================

#[derive(Debug)]
pub struct DatagridColWiseBuilder<const S: usize> {
    buffer: [Option<FxArray>; S],
}

impl<const S: usize> Default for DatagridColWiseBuilder<S> {
    fn default() -> Self {
        Self {
            buffer: [(); S].map(|_| None),
        }
    }
}

impl<const S: usize> DatagridColWiseBuilder<S> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stack<T: Into<FxArray>>(&mut self, arr: T) -> &mut Self {
        for e in self.buffer.iter_mut() {
            if e.is_none() {
                *e = Some(arr.into());
                break;
            }
        }

        self
    }

    pub fn build(self) -> FxResult<Datagrid> {
        let vec = self.buffer.into_iter().flatten().collect::<Vec<_>>();
        Datagrid::try_from(vec)
    }
}

// ================================================================================================
// FxDatagrid & FxDatagridRowBuilderCst & FxDatagridRowBuilder
// ================================================================================================

pub trait FxDatagrid {
    fn gen_row_builder() -> Box<dyn FxDatagridRowBuilder<Self>>;
}

pub trait FxDatagridRowBuilderCst {
    fn new() -> Self;
}

pub trait FxDatagridRowBuilder<T>: Send {
    fn stack(&mut self, row: T);

    fn build(self: Box<Self>) -> FxResult<Datagrid>;
}

#[cfg(test)]
mod test_datagrid {

    use super::*;

    const FILE_AVRO: &str = "./cache/test.avro";
    const FILE_PARQUET: &str = "./cache/test.parquet";

    #[test]
    fn avro_write_success() {
        let a = Int32Array::from([Some(1), None, Some(3)]).boxed();
        let b = Float32Array::from([Some(2.1), None, Some(6.2)]).boxed();
        let c = Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).boxed();

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
        let a = Int32Array::from([Some(1), None, Some(3)]).boxed();
        let b = Float32Array::from([Some(2.1), None, Some(6.2)]).boxed();
        let c = Utf8Array::<i32>::from([Some("a"), Some("b"), Some("c")]).boxed();

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

    #[test]
    fn datagrid_builder_col_wise_success() {
        let mut builder = DatagridColWiseBuilder::<3>::new();

        builder.stack(vec!["a", "b", "c"]);
        builder.stack(vec![1, 2, 3]);
        builder.stack(vec![Some(1.2), None, Some(2.1)]);

        let d = builder.build().unwrap();

        println!("{d:?}");
    }

    #[test]
    fn datagrid_builder_row_wise_success() {
        #[allow(dead_code)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        #[derive(Default)]
        struct UsersBuild {
            id: Vec<i32>,
            name: Vec<String>,
            check: Vec<Option<bool>>,
        }

        impl FxDatagridRowBuilderCst for UsersBuild {
            fn new() -> Self {
                Self::default()
            }
        }

        impl FxDatagridRowBuilder<Users> for UsersBuild {
            fn stack(&mut self, row: Users) {
                self.id.push(row.id);
                self.name.push(row.name);
                self.check.push(row.check);
            }

            fn build(self: Box<Self>) -> FxResult<Datagrid> {
                Datagrid::try_from(vec![
                    FxArray::from(self.id),
                    FxArray::from(self.name),
                    FxArray::from(self.check),
                ])
            }
        }

        impl FxDatagrid for Users {
            fn gen_row_builder() -> Box<dyn FxDatagridRowBuilder<Self>> {
                Box::new(UsersBuild::new())
            }
        }

        let r1 = Users {
            id: 1,
            name: "Jacob".to_string(),
            check: Some(true),
        };

        let r2 = Users {
            id: 2,
            name: "Mia".to_string(),
            check: None,
        };

        // 3. generate `Datagrid` from builder
        let mut bd = Users::gen_row_builder();

        bd.stack(r1);
        bd.stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }

    #[test]
    fn datagrid_builder_row_wise_proc_macro_success() {
        use crate::FX;

        #[allow(dead_code)]
        #[derive(FX)]
        struct Users {
            id: i32,
            name: String,
            check: Option<bool>,
        }

        let r1 = Users {
            id: 1,
            name: "Jacob".to_string(),
            check: Some(true),
        };

        let r2 = Users {
            id: 2,
            name: "Mia".to_string(),
            check: None,
        };

        // 3. generate `Datagrid` from builder
        let mut bd = Users::gen_row_builder();

        bd.stack(r1);
        bd.stack(r2);

        let d = bd.build();

        println!("{d:?}");
    }
}
