# FX

Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix) without using [Polars](https://github.com/pola-rs/polars)' `Series` and `DataFrame`. `Fx` currently tries to explore different types of data container by using some general auto impl traits (see [eclectic.rs](./fx/src/cont/ab/eclectic.rs) and [receptacle.rs](./fx/src/cont/ab/eclectic.rs)).

## Structure

```txt
    .
    ├── fx
    │   ├── src
    │   │   ├── cont
    │   │   │   ├── ab
    │   │   │   │   ├── builder.rs
    │   │   │   │   ├── confined.rs
    │   │   │   │   ├── congruent.rs
    │   │   │   │   ├── dqs.rs
    │   │   │   │   ├── eclectic.rs
    │   │   │   │   ├── mod.rs
    │   │   │   │   ├── private.rs
    │   │   │   │   ├── purport.rs
    │   │   │   │   ├── receptacle.rs
    │   │   │   │   └── seq.rs
    │   │   │   ├── batch.rs
    │   │   │   ├── batches.rs
    │   │   │   ├── bundle.rs
    │   │   │   ├── bundles.rs
    │   │   │   ├── deque.rs
    │   │   │   ├── ext.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── nullopt.rs
    │   │   │   ├── private.rs
    │   │   │   ├── table.rs
    │   │   │   └── tabular.rs
    │   │   ├── io
    │   │   │   ├── ab
    │   │   │   │   └── mod.rs
    │   │   │   ├── ec
    │   │   │   │   ├── mod.rs
    │   │   │   │   ├── parallel.rs
    │   │   │   │   └── simple.rs
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
    │       ├── fx_derive_test.rs
    │       ├── fx_iter_test.rs
    │       ├── fx_seq_test.rs
    │       ├── fx_sizing_dqs_test.rs
    │       └── fx_table_tabular_test.rs
    ├── fx-derive
    │   └── src
    │       ├── constant.rs
    │       ├── dr.rs
    │       ├── eclectic_builder.rs
    │       ├── helper.rs
    │       ├── lib.rs
    │       ├── receptacle_builder.rs
    │       └── sql_impl.rs
    ├── LICENSE
    └── README.md
```

### Traits

- `FxSeq`: common behavior of various sequences type: `Array` and `MutableArray`

- `Purport`: schema related

- `Confined`: typed and fixed length

- `Eclectic`: common behavior of a collection of `FxSeq`

- `Receptacle`: common behavior of the `Confined` types

- `Congruent`: `Chunk` related

- `Dqs`: an `inherent` trait for `FxTable` and `FxTabular`

### Types

Traits implementation for Rust and Arrow types:

- `ArcArr`: `Arc<dyn Array>` with `FxSeq` trait implemented

- `BoxArr`: `Box<dyn Array>` with `FxSeq` trait implemented

- `ArcVec`: `Arc<dyn MutableArray>` with `FxSeq` trait implemented

- `BoxVec`: `Box<dyn MutableArray>` with `FxSeq` trait implemented

- `[S; W]`: `[S; W] where S: FxSeq, W: usize` with `Eclectic` trait implemented

- `Vec<S>`: `Vec<S> where S: FxSeq` with `Eclectic` trait implemented

- `ChunkArr`: `Chunk<Arc<dyn Array>>` with `Eclectic` trait implemented

- `Vec<E>`: `Vec<E> where E: Eclectic` with `Receptacle` trait implemented

- `HashMap<I, E>`: `HashMap<I, E> where I: Hash + Eq, E: Eclectic` with `Receptacle` trait implemented

### Structs and Enums

Structs and enums provided by Fx crate:

- `NullableOptions`: indicates fields' nullable status

- `Batch`: chunked data consists of arrow's `Array`, with `Eclectic` impled and carrying a schema field

- `Batches`: vector of all who implemented `Eclectic` trait, with a schema field

- `Bundle`: array of `FxSeq`, with `Eclectic` impled and carrying a schema field

- `Bundles`: vector of `FxSeq` arrays, with a schema field

- `Deque`: VecDeque of `Array`

- `Table`: array of `Deque`

- `Tabular`: vector of `Deque`

### IO

- `arvo`

- `csv`

- `ipc`

- `parquet`

- `sql`

### Macros

- `arc_arr`

- `arc_vec`

- `box_arr`

- `box_vec`

## Test

- [fx_builder_test](./fx/tests/fx_builder_test.rs): Builder mode. Manually impl builder traits which placed in [builder.rs](./fx/src/cont/ab/builder.rs).

- [fx_derive_test](./fx/tests/fx_derive_test.rs): Powerful builder mode by proc-macro. Given a struct who has been placed the derived macro `#[derive(FX)]`, automatically generate wanted containers. Check [fx-derive](./fx-derive/src/lib.rs) for more details.

- [fx_iter_test](./fx/tests/fx_iter_test.rs): Iterator of `ArcArr`, `BoxArr`, `ArcVec` & `BoxVec`.

- [fx_seq_test](./fx/tests/fx_seq_test.rs): Functionality of `FxSeq` trait (WIP).

- [fx_table_tabular_test](./fx/tests/fx_table_tabular_test.rs): Functionality of `FxTable` and `FxTabular` (WIP).

## Dependencies

- fx

  - `arrow2`
  - `futures`
  - `sqlx`
  - `thiserror`
  - `tokio`
  - `ref-cast`
  - `inherent`

- fx-derive

  - `proc-macro2`
  - `quote`
  - `syn`

## Misc

- To get a tree view of this project, run `cargo make --makefile fx.toml tree`

## Todo

- Reader/Writer for `Eclectic` (single thread) & `Receptacle` (parallel)

- Streaming I/O

- Type support: `ListArray`/`MutableListArray` & `FixedSizeListArray`/`MutableFixedSizeListArray`

- Type support: `StructArray`/`MutableStructArray`

- Type support: `BinaryArray`/`MutableBinaryArray`
