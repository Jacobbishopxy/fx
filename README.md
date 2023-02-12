# FX

Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix) without using [Polars](https://github.com/pola-rs/polars)' `Series` and `DataFrame`. `Fx` currently tries to explore different type of data containers by some general auto impl traits (see [chunking.rs](./fx/src/containers/ab/chunking.rs) and [container.rs](./fx/src/containers/ab/container.rs)).

## Structure

```txt
    .
    ├── examples
    ├── fx
    │   ├── src
    │   │   ├── cont
    │   │   │   ├── ab
    │   │   │   │   ├── builder.rs
    │   │   │   │   ├── chunking.rs
    │   │   │   │   ├── container.rs
    │   │   │   │   ├── mod.rs
    │   │   │   │   ├── private.rs
    │   │   │   │   └── seq.rs
    │   │   │   ├── array.rs
    │   │   │   ├── batch.rs
    │   │   │   ├── bundle.rs
    │   │   │   ├── cvt.rs
    │   │   │   ├── grid.rs
    │   │   │   ├── mod.rs
    │   │   │   ├── nullopt.rs
    │   │   │   ├── parcel.rs
    │   │   │   ├── table.rs
    │   │   │   └── vector.rs
    │   │   ├── io
    │   │   │   ├── ab
    │   │   │   │   └── mod.rs
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
    │       ├── helper.rs
    │       └── lib.rs
    ├── LICENSE
    └── README.md
```

- `Array`/`Vector`: immutable array (wrapping arrow's `Array`) and mutable vector (wrapping arrow's `MutableArray`)

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

- fx-macros

  - `proc-macro2`
  - `quote`
  - `syn`

## Misc

- To get a tree view of this project, run `cargo make --makefile fx.toml tree`

## Todo

- Use a new structure which can hold either `Array` or `Vector`

- Let I/O satisfies all containers
