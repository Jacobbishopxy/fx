# FX

Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix) without using [Polars](https://github.com/pola-rs/polars)' `Series` and `DataFrame`. `Fx` currently tries to explore different type of data containers by some general auto impl traits (see [chunking.rs](./fx/src/containers/ab/chunking.rs) and [container.rs](./fx/src/containers/ab/container.rs)).

## Structure

```txt
    .
    ├── fx
    │   ├── src
    │   │   ├── cont
    │   │   │   ├── ab
    │   │   │   │   ├── builder.rs
    │   │   │   │   ├── congruent.rs
    │   │   │   │   ├── eclectic.rs
    │   │   │   │   ├── mod.rs
    │   │   │   │   ├── private.rs
    │   │   │   │   ├── purport.rs
    │   │   │   │   └── seq.rs
    │   │   │   ├── batch.rs
    │   │   │   ├── batches.rs
    │   │   │   ├── cvt.rs
    │   │   │   ├── ext.rs
    │   │   │   ├── macros.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── nullopt.rs
    │   │   │   ├── table.rs
    │   │   │   └── tables.rs
    │   │   ├── io
    │   │   │   ├── ab
    │   │   │   │   └── mod.rs
    │   │   │   ├── arvo.rs
    │   │   │   ├── csv.rs
    │   │   │   ├── ipc.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── parquet.rs
    │   │   │   └── sql.rs
    │   │   ├── ctor.rs
    │   │   ├── error.rs
    │   │   ├── lib.rs
    │   │   ├── macros.rs
    │   │   ├── types.rs
    │   │   └── value.rs
    │   └── tests
    │       ├── arrow_compute_test.rs
    │       ├── fx_builder_test.rs
    │       └── fx_macros_test.rs
    ├── fx-macros
    │   └── src
    │       ├── dr.rs
    │       ├── helper.rs
    │       └── lib.rs
    ├── LICENSE
    └── README.md
```

### Traits

- `FxSeq`

- `Eclectic`

- `EclecticCollection`

- `Congruent`

- `Purport`

### Types

Traits implementation for Rust and Arrow types:

- `ArcArr`: `Arc<dyn Array>` with `FxSeq` trait implemented

- `BoxArr`: `Box<dyn Array>` with `FxSeq` trait implemented

- `ArcVec`: `Arc<dyn MutableArray>` with `FxSeq` trait implemented

- `BoxVec`: `Box<dyn MutableArray>` with `FxSeq` trait implemented

- `Vec<S>`: `Vec<S> where S: FxSeq` with `Eclectic` trait implemented

- `ChunkArr`: `Chunk<Arc<dyn Array>>` with `Eclectic` trait implemented

- `Vec<[S; W]>`: `Vec<[S; W]> where S: FxSeq, W: usize` with `EclecticCollection` trait implemented

- `VecChunk`: `Vec<Chunk<Arc<dyn Array>>>` with `EclecticCollection` trait implemented

- `HashMap<I, ChunkArr>`: `HashMap<I, Chunk<Arc<dyn Array>>> where I: Hash + Eq` with `EclecticCollection` trait implemented

### Structs and Enums

Structs and enums provided by Fx crate:

- `NullableOptions`: indicates fields' nullable status

- `Batch`: chunked data consists of arrow's `Array`, with a schema field

- `Batches`: vector of all who implemented `Eclectic` trait, with a schema field

- `Table`: array of `FxSeq`, with a schema field

- `Tables`: vector of `FxSeq` arrays, with a schema field

## Dependencies

- fx

  - `arrow2`
  - `futures`
  - `sqlx`
  - `thiserror`
  - `tokio`
  - `ref-cast`
  - `inherent`

- fx-macros

  - `proc-macro2`
  - `quote`
  - `syn`

## Misc

- To get a tree view of this project, run `cargo make --makefile fx.toml tree`

## Todo

- Better conversion among ArcArr/BoxArr/ArcVec/BoxVec

- Iterator for ArcArr/BoxArr/ArcVec/BoxVec

- Let I/O satisfies all containers
