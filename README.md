# FX

Yet another [Fabrix](https://github.com/Jacobbishopxy/fabrix) without using [Polars]' `Series` and `DataFrame`. Based on `sqlx` and `arrow2`, `fx` only works for transferring a small amount of data.

## Structure

```txt
    .
    ├── fx
    │   └── src
    │       ├── array.rs
    │       ├── connector.rs
    │       ├── datagrid.rs
    │       ├── error.rs
    │       ├── lib.rs
    │       ├── row.rs
    │       └── value.rs
    ├── fx-macros
    │   └── src
    │       ├── dr.rs
    │       └── lib.rs
    ├── LICENSE
    └── README.md
```

## Dependencies

- fx

  - `arrow2`
  - `futures`
  - `sqlx`
  - `thiserror`
  - `tokio`

- fx-macros

  - `proc-macro2`
  - `quote`
  - `syn`
