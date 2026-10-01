---
title: Generator Functions
rank: 4
---

# Built-in Spark Generator Functions

<!--@include: ./_common/notice.md-->

## Compatibility aliases

`unnest` and `unnest_outer` are compatibility aliases for `explode` and
`explode_outer`, for queries written against systems that spell the generator
`unnest` (Trino, DuckDB, PostgreSQL). They are recognized in a bare `FROM`
clause:

```sql
SELECT * FROM unnest(ARRAY(1, 2, 3));
SELECT * FROM unnest_outer(ARRAY(1, 2, 3));
```

Because `unnest` is not Spark syntax, it is not a keyword in Sail's parser and
is matched by name in the analyzer. Only that exact, unqualified spelling gets
the bare-`FROM` alias treatment. For any other form, use the fully general
`LATERAL` clause, which accepts any generator name:

```sql
SELECT * FROM LATERAL unnest(ARRAY(1, 2, 3));
SELECT * FROM t, LATERAL unnest(t.items);
SELECT * FROM t LEFT JOIN LATERAL unnest(t.items) ON true;
```

`explode` and `explode_outer` remain the primary, fully supported spellings and
are unaffected.

<FunctionSupportTable :data="data.generator" />

<script setup>
import FunctionSupportTable from "@theme/components/FunctionSupportTable.vue";
import { data } from "./support.data.ts";
</script>
