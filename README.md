# FX

Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix) without using [Polars](https://github.com/pola-rs/polars)' `Series` and `DataFrame`. Based on `sqlx` and `arrow2`, `fx` only works for transferring a small amount of data.

## Structure

```txt
    .
    ├── examples
    ├── fx
    │   ├── src
    │   │   ├── containers
    │   │   │   ├── ab
    │   │   │   │   ├── builder.rs
    │   │   │   │   ├── chunking.rs
    │   │   │   │   ├── container.rs
    │   │   │   │   ├── mod.rs
    │   │   │   │   ├── nullopt.rs
    │   │   │   │   └── private.rs
    │   │   │   ├── array.rs
    │   │   │   ├── batch.rs
    │   │   │   ├── batches.rs
    │   │   │   ├── cvt.rs
    │   │   │   ├── grid.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── table.rs
    │   │   │   └── vector.rs
    │   │   ├── io
    │   │   │   ├── arvo.rs
    │   │   │   ├── csv.rs
    │   │   │   ├── ipc.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── parquet.rs
    │   │   │   └── sql.rs
    │   │   ├── error.rs
    │   │   ├── lib.rs
    │   │   ├── macros.rs
    │   │   ├── types.rs
    │   │   └── value.rs
    │   └── tests
    │       └── arrow_compute_test.rs
    ├── fx-macros
    │   └── src
    │       ├── dr.rs
    │       └── lib.rs
    ├── LICENSE
    └── README.md
```

- `Array`/`Vector`: immutable array and mutable vector

- `Grid`/`Batch`: chunked data consists of `Array`, the letter one has a schema field

- `Batches`: vector of `Grid`, with a schema field

- `Table`: WIP

## Dependencies

- fx

  - `arrow2`
  - `futures`
  - `sqlx`
  - `thiserror`
  - `tokio`
  - `ref-cast`

- fx-macros

  - `proc-macro2`
  - `quote`
  - `syn`

## Misc

- To get a tree view of this project, run `cargo make --makefile fx.toml tree`

## Todo

- ColWiseBuilder & RowWiseBuilder in a generic way, and make `fx-macros` follows it.
