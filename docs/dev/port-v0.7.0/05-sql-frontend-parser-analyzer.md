# Porting feat/0.7.0 → feat/0.7.1 — Doc 05: SQL Frontend — Parser, AST, Spec & Analyzer

> Part of the `docs/dev/port-v0.7.0/` inventory. Documents the new/changed SQL surface on
> `feat/0.7.0` vs base `f0b137d6` that feeds the catalog/DDL (doc 06), row-level ops
> (doc 07), LOAD (doc 08) and procedures/metadata tables (doc 09) clusters: new `CALL`,
> `TRUNCATE TABLE`, `SHOW TBLPROPERTIES`, `DESCRIBE VIEW`, and a substantially expanded
> `ALTER TABLE` column-operation vocabulary.
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`. Note: this inventory covers the
> `sail-sql-parser` → `sail-sql-analyzer` → `sail-common::spec` **specification delta**
> only; the iceberg physical implementations of these statements are in the sibling docs.

---

## 1. Scope

| File | Change |
|---|---|
| `sail-common/src/spec/expression.rs` | `Identifier` and `ObjectName` gain `PartialOrd, Ord` |
| `sail-common/src/spec/plan.rs` | `CommandNode::ShowTblProperties`, `CommandNode::CallProcedure`, new `AlterTableOperation` variants + `ColumnDefinition`, `ColumnAlterationOption`, `ColumnPosition` types |
| `sail-sql-parser/data/keywords.txt` | adds `CALL` |
| `sail-sql-parser/src/ast/statement.rs` | `Statement::ShowTblProperties`, `TruncateTable`, `Call`; `DescribeItem::View`; keyword imports (`Call`, `Truncate`) |
| `sail-sql-parser/tests/gold_data/syntax.json` | parser gold data for the new grammar |
| `sail-sql-analyzer/src/statement.rs` | conversions for all the above (+~595 LOC incl. a large test module) |
| `sail-sql-analyzer/src/parser.rs` | parse tests for `TRUNCATE TABLE`, `CALL` positional/named |
| `sail-spark-connect/tests/gold_data/plan/ddl_alter_table.json` | ALTER TABLE golden plan snapshots updated from `"unknown"` to concrete ops |

`CALL` and `TRUNCATE` keywords were already recognized on `feat/0.7`/0.7.1's parser? — on the
0.7.0 base only `TRUNCATE` was newly keyworded; `CALL` was added to `keywords.txt` and the AST
keyword import set. (`Truncate` appears in the existing keyword import list only after this
delta; the net diff shows both `Call` and `Truncate` added to the import list.)

---

## 2. Spec additions (`sail-common::spec`)

### 2.1 `plan.rs`

`CommandNode` gains:

```rust
ShowTblProperties { table: ObjectName, property_key: Option<String> },   // near ShowFunctions
CallProcedure { name: ObjectName, arguments: Vec<(Option<Identifier>, Expr)> }, // near overwrite/partition commands
```

`AlterTableOperation` (camelCase serde) replaces the bare `Unknown`/TODO surface with real ops
(the `Unknown` variant and pre-existing `SetTableProperties`, `SetColumnComment`/… remain):

```rust
RenameTable { new_name: ObjectName },
AlterColumnComment  { name: ObjectName, comment: Option<String> },
AlterColumnNullability { name: ObjectName, nullable: bool },
AlterColumnPosition { name: ObjectName, position: ColumnPosition },
AddColumns  { items: Vec<ColumnDefinition> },
DropColumns { names: Vec<ObjectName>, if_exists: bool },
// pre-existing: Unknown, SetTableProperties, …, SetColumnComment {name, default?} …
```

New helper types:

```rust
pub struct ColumnDefinition {
    pub name: ObjectName,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,   // raw SQL text of the default expression
    pub comment: Option<String>,
}

pub enum ColumnAlterationOption { NotNull, Default(Box<Expr>), Comment(String), Position(ColumnPosition) }

pub enum ColumnPosition { First, After(ObjectName) }   // Ord/Hash derives for set usage
```

### 2.2 `expression.rs`

`Identifier` and `ObjectName` derive `PartialOrd, Ord` (in addition to Hash/Eq) — enables
sorted/b-tree-keyed use (e.g. catalog property ordering / dedup sets downstream).

---

## 3. Parser AST additions (`sail-sql-parser/src/ast/statement.rs`)

New `Statement` variants (the enum is the grammar definition via the parser derive macro;
each variant's field list is the rule, with `#[parser(function = ...)]` customizing how a
wrapped tuple is lifted to an `Option`):

```rust
ShowTblProperties {
    show: Show, tblproperties: Tblproperties,
    table: ObjectName,
    property_key: Option<(LeftParenthesis, StringLiteral, RightParenthesis)>, // parser fn compose
},

TruncateTable { truncate: Truncate, table: Table, name: ObjectName },

Call {
    call: Call,
    name: ObjectName,
    #[parser(function = |(_, _, e, _), o| compose(e, o))]
    arguments: ast::expression::FunctionArgumentList,
},
```

And `DescribeItem::View { view: View, extended: Option<Extended>, name: ObjectName }`
(grammar `[Keyword(View), Option(Keyword(Extended)), ObjectName]`, tried after the other
DESCRIBE items).

`FunctionArgumentList` already existed for function-call syntax and now doubles as the
`CALL` argument list — it supports named (`name => value`), unnamed, and sequence/duplicate
treatments; the analyzer only consumes `Named`/`Unnamed`.

Keyword data: `CALL` added to `keywords.txt`; `Call` and `Truncate` added to the generated
keyword import block in the AST.

### 3.1 `syntax.json` gold data

Regenerated grammar trees now include: `Keyword(Call)`/`Keyword(Truncate)` terminals, the
`DescribeItem::View` sequence, the `ShowTblProperties` sequence and its
`Option(Tuple(LeftParenthesis, StringLiteral, RightParenthesis))` sub-rule, and the
`TruncateTable`/`Call` statement sequences (`Keyword(Call) ObjectName FunctionArgumentList`).

---

## 4. Analyzer conversions (`sail-sql-analyzer/src/statement.rs`)

### 4.1 New statement mappings in `from_ast_statement`

- `Statement::ShowTblProperties` → `CommandNode::ShowTblProperties { table,
  property_key: property_key.map(|(_, key, _)| from_ast_string(key)).transpose()? }`.
- `Statement::TruncateTable { name, .. }` → **`CommandNode::Delete { table, table_alias: None,
  condition: None }`** — TRUNCATE is DELETE without a WHERE; the delete path then produces an
  empty snapshot (see `plan_delete` with `condition: None`, doc 07).
- `Statement::Call` → `CommandNode::CallProcedure`: flattens `FunctionArgumentList` sequences;
  `FunctionArgument::Named(name, _, value)` → `(Some(name), expr)`,
  `Unnamed(value)` → `(None, expr)`; name → `from_ast_object_name`.
- `DescribeItem::View { extended, name, .. }` → `CommandNode::DescribeTable { table,
  extended: extended.is_some(), partition: Default::default(), column: None }`
  (previously this arm produced an error / was unhandled).

### 4.2 ALTER TABLE operations — `from_ast_alter_table_operation`

Now a full mapping (was mostly `Unknown`):

| AST (`AlterTableOperation`) | spec op |
|---|---|
| `RenameTable { name }` | `RenameTable { new_name }` |
| `RenamePartition / RenameColumn / AddPartitions / DropPartition / SetFileFormat / SetLocation / RecoverPartitions` | `Unknown` (unchanged) |
| `AlterColumn { name, operation: Comment(_, s), .. }` | `AlterColumnComment { name, comment: Some(from_ast_string(s)) }` |
| `AlterColumn { operation: SetNotNull(..), .. }` | `AlterColumnNullability { nullable: false }` |
| `AlterColumn { operation: DropNotNull(..), .. }` | `AlterColumnNullability { nullable: true }` |
| `AlterColumn { operation: Position(pos), .. }` | `AlterColumnPosition`; `ColumnPosition::First(_)→First`, `After(_, name)→After(name)` |
| `AddColumns { items, .. }` | `AddColumns { items: from_ast_column_alteration_list(items) }` |
| `ReplaceColumns { items, .. }` | validate via the same list fn, then `Unknown` (still untranslated) |
| `DropColumns { names, if_exists }` | `DropColumns { names (delimited or not), if_exists: if_exists.is_some() }` |

`from_ast_column_alteration_list` (was `-> SqlResult<()>`, only validating) now returns
`SqlResult<Vec<spec::ColumnDefinition>>`, mapping each `ColumnAlteration { name, data_type,
options }` to a `ColumnDefinition`:

```rust
name:     from_ast_object_name(name)?,
data_type: from_ast_data_type(data_type)?,
nullable: !options.not_null,
default:  options.default.as_ref().map(|expr| expr.text().trim().to_string()),
comment:  options.comment.map(from_ast_string).transpose()?,
```

`ColumnAlterationOptions` (`TryFrom<Vec<ColumnAlterationOption>>`) is unchanged except it now
feeds real translation (it collects `not_null`, `default`, `comment`, and validates duplicate
NOT NULL/DEFAULT/COMMENT/POSITION clauses).

### 4.3 New tests

- `parser.rs`: `test_parse_truncate_table`, `test_parse_call_positional`,
  `test_parse_call_named`.
- `statement.rs` test module (uses `parse_alter_op` helper):
  - Rename: `ALTER TABLE old_name RENAME TO new_name`, multipart `db.new_name`.
  - Drop columns: single, multiple `(a, b.c)`, `IF EXISTS`.
  - Alter column comment: set `COMMENT 'hello world'`, empty `COMMENT ''`.
  - Nullability: `SET NOT NULL` / `DROP NOT NULL`.
  - Position: `FIRST`, `AFTER d`, multipart names `a.b.c`, `AFTER a.b`.
  - Add columns: basic `x int`, NOT NULL, DEFAULT (int/string/boolean/float), COMMENT
    (single & double-quoted, empty), FIRST, all-options, multiple `(x int NOT NULL, y string
    COMMENT 'name')`, multipart `a.b.c bigint`, and type mapping — DECIMAL(10,2), DATE,
    TIMESTAMP (→ Microsecond), BOOLEAN, DOUBLE, FLOAT, SMALLINT, TINYINT, BINARY,
    ARRAY<INT> (+NOT NULL), MAP<STRING,INT>, STRUCT<...>, AFTER y, mixed combo.
  - Statement mapping: `TRUNCATE TABLE landing.customers` → `Delete{condition: None}`;
    `CALL test.system.rollback_to_snapshot('landing.customers', 42)` → positional
    `CallProcedure`; `CALL ... expire_snapshots(table => ..., older_than => TIMESTAMP '...')`
    → named `CallProcedure`.

### 4.4 Spark-Connect ALTER TABLE plan gold data

`crates/sail-spark-connect/tests/gold_data/plan/ddl_alter_table.json`: all previously
`"operation": "unknown"` entries are replaced with concrete operations, e.g.
`"addColumns"` with `items[{name, data_type (incl. configuredUtf8/int32), nullable,
default, comment}]`, `"alterColumnNullability"`, `"alterColumnComment"`,
`"alterColumnPosition": {"position": "First"}`. These cover ADD COLUMN, ADD COLUMNS,
ALTER COLUMN (comment/nullability/position) examples. (The JSON body of this gold file was
also touched by the scalar-subquery/DDL commit `9e455322`; the plan-level operation shapes
above are the meaningful spec changes.)

---

## 5. Port notes / risks

1. `Statement::TruncateTable` at this layer collapses into `Delete` — but a dedicated AST
   keyword is added so the parser stays Spark-compatible; the DELETE-empty-snapshot semantics
   live in the row-level-ops layer (doc 07 §empty-table DELETE) and the catalog layer
   (48dca759 added support for catalog-managed empty-table DELETE, doc 06).
2. `AlterTableOperation::RenameTable` now also flows through `sail-catalog`/`sail-plan`
   resolvers (doc 06). `DropColumns`/`AddColumns`/`AlterColumn*` are consumed by the iceberg
   `update.rs`/metadata-commit path (doc 07) and by catalog-managed iceberg DDL.
3. The `spec::ColumnPosition`/`ColumnDefinition`/`ColumnAlterationOption` types are shared by
   analyzer and resolver — port as a unit with `plan.rs` so serde field names
   (camelCase) match the gold data.
4. `Identifier`/`ObjectName` `Ord` derives ripple to any downstream code relying on spec
   ordering; benign but part of the compile-time surface.
5. The parser derive macros consume the AST field layout verbatim — copy the enum variants
   exactly, including `#[parser(function = ...)]` attributes, or the generated grammar/syntax
   gold tests will mismatch.
