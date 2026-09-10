# GX 1.20.0 uniqueness regression

**Filed upstream:** [fivetran/great_expectations#12179](https://github.com/fivetran/great_expectations/issues/12179)
(opened 2026-09-10, still open at time of writing)

This is the write-up behind that report: the reproduction, the root-cause
diff, and why the pin in `requirements.txt` cannot be lifted yet.

**Re-check before unpinning.** The bug is live in 1.20.0, 1.21.0 and 1.22.0 —
`column_values_unique.py` is byte-identical across all three. If the issue is
closed, diff that file against 1.19.1 before changing the pin.

---

## Report

### Summary

Since 1.20.0, `expect_column_values_to_be_unique` raises `KeyError: '<column>'`
on a SQLAlchemy backend when the column name is mixed case and the result
format requests unexpected indices. The expectation is reported as failed with
an exception rather than evaluated.

The same suite passes on the Pandas backend against the same data, so a
cross-backend pipeline starts disagreeing with itself.

### Versions

| | |
|---|---|
| Great Expectations | 1.20.0, 1.21.0, 1.22.0 (all affected); 1.19.1 unaffected |
| Backend | PostgreSQL 16 (via `postgresql+psycopg2`) |
| Python | 3.12 |
| OS | Ubuntu 24.04 (GitHub Actions runner) |

### Traceback

```
great_expectations/expectations/metrics/column_map_metrics/column_values_unique.py:284
    in _sqlalchemy_unique_unexpected_index_query
        duplicates = _build_duplicate_rows_source(
great_expectations/expectations/metrics/column_map_metrics/column_values_unique.py:149
    in _build_duplicate_rows_source
        source.c[column_name] == dup_keys.c[column_name],
sqlalchemy/sql/base.py:1715 in __getitem__
    return self._index[key][1]
KeyError: 'Rndrng_NPI'
```

### Cause

`_build_duplicate_rows_source` was introduced in 1.20.0 and does not exist in
1.19.1, which places the regression precisely at that release.

In the branch taken by dialects outside `_SINGLE_REFERENCE_DIALECTS`
(currently `{MYSQL, SINGLESTOREDB}`, so PostgreSQL takes it), the source
subquery is built with:

```python
source = (
    sa.select(*[sa.column(c) for c in table_columns])
    .select_from(selectable)
    .subquery(_SOURCE_SUBQUERY_ALIAS)
)
```

and is then indexed with `source.c[column_name]`. When `column_name` is mixed
case, the key used to build the projection and the key used to read it back
do not agree, and `.c` lookup fails.

`table_columns` comes from `metrics["table.columns"]`.

### Reproduction

```python
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from sqlalchemy import create_engine

CONN = "postgresql+psycopg2://user:password@localhost:5432/db"

pd.DataFrame(
    {
        "Rndrng_NPI": ["1000000001", "1000000002", "1000000002"],
        "Tot_Srvcs": [10.0, 20.0, 30.0],
    }
).to_sql("mup_provider", create_engine(CONN), index=False, if_exists="replace")

context = gx.get_context(mode="ephemeral")
ds = context.data_sources.add_postgres(name="pg", connection_string=CONN)
asset = ds.add_table_asset(name="t", table_name="mup_provider")
bd = asset.add_batch_definition_whole_table("whole_table")

suite = context.suites.add(gx.ExpectationSuite(name="s"))
suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="Rndrng_NPI"))

vd = context.validation_definitions.add(
    gx.ValidationDefinition(name="vd", data=bd, suite=suite)
)
checkpoint = context.checkpoints.add(
    gx.Checkpoint(
        name="cp",
        validation_definitions=[vd],
        result_format={"result_format": "COMPLETE"},
    )
)
checkpoint.run()
```

**Expected:** the expectation evaluates and fails, reporting 2 unexpected values.
**Actual:** the expectation is reported failed with `KeyError: 'Rndrng_NPI'`.

### Notes

- A lower-cased column name is unaffected, which is why this is easy to miss.
- SQLite does **not** reproduce it, despite taking the same code branch — so a
  SQLite-backed test would not have caught it. PostgreSQL is where we see it.
- `result_format: "SUMMARY"` avoids the unexpected-index path and so avoids
  the error; `"COMPLETE"` triggers it.
- Mixed-case columns are not exotic here: the source is a public CMS extract
  whose columns ship as `Rndrng_NPI`, `Tot_Srvcs` and so on. Normalising them
  to lower case is not an option either, because GX emits quoted SQL and
  PostgreSQL is case-sensitive once an identifier is quoted — lower-casing
  makes every other column lookup fail instead.

### Impact

This is a cross-backend data-contract pipeline: one expectation suite run
against Pandas, PostgreSQL and Spark. The Pandas leg passes and the PostgreSQL
leg errors on the same rule and the same data, which is the specific failure
mode a portable contract exists to prevent.

Pinned to 1.19.1 for now.
