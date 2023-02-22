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
    │   │   │   ├── bundle.rs
    │   │   │   ├── cvt.rs
    │   │   │   ├── ext.rs
    │   │   │   ├── macros.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── nullopt.rs
    │   │   │   ├── parcel.rs
    │   │   │   └── table.rs
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

- `Congruent`

- `Purport`

### Types

- `ArcArr`: `Arc<dyn Array>` implement `FxSeq` trait

- `ArcVec`: `Arc<dyn MutableArray>` implement `FxSeq` trait

### Structs and Enums

- `Grid`/`Batch`: chunked data consists of arrow's `Array`, the latter one has a schema field

- `Bundle`: vector of `Grid`, with a schema field

- `Parcel`: WIP

- `Table`: WIP

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

- enhance `fx-macros`: use derived attributes to choose which `Eclectic` & `EclecticContainer`

- test `inherent` crate in `fx-macros` (for builders)

- add new type `Box<dyn Array>` implementation

- declarative macros for each container

- Let I/O satisfies all containers
