use std::borrow::Cow;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::ScalarFunctionArgs;
use datafusion_expr::registry::FunctionRegistry;
use moka::sync::Cache;
use regex::RegexBuilder;
use rhai::{
    AST, Array as RhaiArray, Dynamic, Engine, FLOAT, INT, ImmutableString, Map, OptimizationLevel,
    Scope,
};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;

mod rewrite;
mod toposort;
mod variables;

pub use rewrite::maybe_rewrite_integers;
pub use toposort::{CycleError, sort_rules};
pub use variables::extract_variables;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RhaiEval {
    signature: Signature,
}

impl Default for RhaiEval {
    fn default() -> Self {
        Self::new()
    }
}

impl RhaiEval {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RhaiEval {
    fn name(&self) -> &str {
        "rhai_eval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let [expr_arg, vars_arg] = args.as_slice() else {
            return exec_err!(
                "`rhai_eval` expects exactly 2 arguments: expression string and vars struct"
            );
        };

        let row_count = match (expr_arg, vars_arg) {
            (ColumnarValue::Array(array), _) => array.len(),
            (_, ColumnarValue::Array(array)) => array.len(),
            _ => number_rows.max(1),
        };

        let engine = shared_engine();
        let mut values = Vec::with_capacity(row_count);

        // A scalar expression is identical for every row: rewrite, promote and
        // compile it once instead of once per row.
        let scalar_expr = match expr_arg {
            ColumnarValue::Scalar(_) => match extract_expression(expr_arg, 0)? {
                // Every row is NULL: no evaluation needed at all.
                None => {
                    values.resize(row_count, None);
                    return Ok(ColumnarValue::Array(
                        Arc::new(StringArray::from(values)) as ArrayRef
                    ));
                }
                Some(expr) => Some(compile_expression(engine, &expr)?),
            },
            ColumnarValue::Array(_) => None,
        };
        // The vars shape is fixed for the batch. Scalar vars build their scope
        // once and rewind it between rows; array vars need per-row values but
        // can still reuse the pre-collected field names.
        let field_names: Option<Vec<ImmutableString>> = match vars_arg {
            ColumnarValue::Array(array) => struct_field_names_for(array)?,
            ColumnarValue::Scalar(value) => match value {
                ScalarValue::Struct(array) => Some(struct_field_names(array)),
                _ => None,
            },
        };
        let mut scalar_scope = match (&scalar_expr, vars_arg) {
            (Some(compiled), ColumnarValue::Scalar(_)) => Some(build_scope(
                vars_arg,
                0,
                field_names.as_deref().unwrap_or_default(),
                compiled.promote_integers,
            )?),
            _ => None,
        };
        let base_scope_len = scalar_scope.as_ref().map_or(0, Scope::len);
        // Array expressions usually repeat down a column (rule tables), so a
        // one-entry cache keyed on the raw text keeps the rewrite scan and
        // compilation out of the common case.
        let mut compiled: Option<(String, CompiledExpression)> = None;
        for row in 0..row_count {
            let compiled = match &scalar_expr {
                Some(compiled) => {
                    if let Some(scope) = scalar_scope.as_mut() {
                        // `base_scope_len` counts the constants pushed when the
                        // scope was built, so the loop stops before
                        // `Scope::pop` can panic on an empty scope.
                        while scope.len() > base_scope_len {
                            scope.pop();
                        }
                    }
                    compiled
                }
                None => {
                    let Some(expr) = extract_expression(expr_arg, row)? else {
                        values.push(None);
                        continue;
                    };
                    let trimmed = expr.trim();
                    if compiled
                        .as_ref()
                        .is_none_or(|(cached, _)| cached != trimmed)
                    {
                        let entry = compile_expression(engine, trimmed)?;
                        compiled = Some((trimmed.to_string(), entry));
                    }
                    let Some((_, entry)) = compiled.as_ref() else {
                        return exec_err!("`rhai_eval` failed to compile expression");
                    };
                    entry
                }
            };

            let value = match scalar_scope.as_mut() {
                Some(scope) => engine.eval_ast_with_scope::<Dynamic>(scope, &compiled.ast),
                None => {
                    let mut scope = build_scope(
                        vars_arg,
                        row,
                        field_names.as_deref().unwrap_or_default(),
                        compiled.promote_integers,
                    )?;
                    engine.eval_ast_with_scope::<Dynamic>(&mut scope, &compiled.ast)
                }
            }
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "`rhai_eval` failed to evaluate expression `{}`: {e}",
                    compiled.source,
                ))
            })?;
            values.push(stringify_rhai_result(value)?);
        }

        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(values)) as ArrayRef
        ))
    }
}

/// A rewritten, compiled expression plus the bindings it needs.
struct CompiledExpression {
    /// The expression text after [`maybe_rewrite_integers`] (used in errors).
    source: String,
    ast: AST,
    /// Whether integer inputs must bind as floats (the rule performs division).
    promote_integers: bool,
}

/// Apply the Spark-division rewrite and compile the result.
///
/// When the rule contains `/`, bare integer literals evaluate as floats
/// (`7 / 2` → `3.5`) and integer inputs bind as floats. Without `/` the text
/// is used unchanged.
fn compile_expression(engine: &Engine, expr: &str) -> Result<CompiledExpression> {
    let trimmed = expr.trim();
    let source: Cow<'_, str> = maybe_rewrite_integers(trimmed);
    let ast = engine.compile(source.as_ref()).map_err(|e| {
        DataFusionError::Execution(format!(
            "`rhai_eval` failed to evaluate expression `{trimmed}`: {e}",
        ))
    })?;

    Ok(CompiledExpression {
        source: source.into_owned(),
        ast,
        promote_integers: trimmed.contains('/'),
    })
}

pub fn register_rhai_functions(context: &SessionContext) -> Result<()> {
    let rhai_eval = rhai_eval_udf();
    context
        .state_ref()
        .write()
        .register_udf(Arc::new(rhai_eval.clone()))?;

    let catalog_manager = context.extension::<CatalogManager>()?;
    catalog_manager
        .register_function(rhai_eval)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(())
}

pub fn rhai_eval_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(RhaiEval::new())
}

/// Builds the Rhai engine for `rhai_eval`.
///
/// Matches the DQ-rule evaluation contract: full optimization, DoS bounds
/// (1M operations, 100KB strings, 10K arrays — all surface as evaluation
/// errors), and `eval`/`import` disabled so rules cannot pull in code.
///
/// SQL `NULL` binds as Rhai unit (`()`) — see `scalar_value_to_dynamic` — so
/// null checks spell as `YEAR != ()` or `type_of(YEAR) == "()"`. The `is_null`
/// / `is_not_null` helpers below offer the same check under a Spark-like name.
/// `regex_match(text, pattern)` compiles `pattern` per distinct value and
/// returns `false` for invalid patterns instead of erroring.
fn rhai_engine() -> Engine {
    let mut engine = Engine::new();

    engine.set_optimization_level(OptimizationLevel::Full);
    engine.set_max_operations(1_000_000);
    engine.set_max_string_size(100_000);
    engine.set_max_array_size(10_000);
    engine.disable_symbol("eval");
    engine.disable_symbol("import");
    engine.register_fn("is_null", |value: Dynamic| value.is_unit());
    engine.register_fn("is_not_null", |value: Dynamic| !value.is_unit());
    engine.register_fn("regex_match", |text: &str, pattern: &str| {
        regex_match(text, pattern)
    });
    engine
}

/// Upper bound for distinct cached regex patterns (invalid patterns included).
/// The cache is an implementation detail; it only avoids recompiling the
/// same pattern on every row.
const REGEX_CACHE_CAP: u64 = 256;

/// Upper bound for an individual regex pattern's compiled size (1 MB).
const REGEX_SIZE_LIMIT: usize = 1 << 20;

static REGEX_CACHE: OnceLock<Cache<String, Option<Arc<regex::Regex>>>> = OnceLock::new();

fn regex_cache() -> &'static Cache<String, Option<Arc<regex::Regex>>> {
    REGEX_CACHE.get_or_init(|| Cache::builder().max_capacity(REGEX_CACHE_CAP).build())
}

fn regex_match(text: &str, pattern: &str) -> bool {
    let cache = regex_cache();
    if let Some(entry) = cache.get(pattern) {
        return entry.as_ref().is_some_and(|re| re.is_match(text));
    }
    let compiled = RegexBuilder::new(pattern)
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .ok()
        .map(Arc::new);
    let matched = compiled.as_ref().is_some_and(|re| re.is_match(text));
    cache.insert(pattern.to_string(), compiled);
    matched
}

/// Shared engine for batch evaluation.
///
/// `Engine` is `Send + Sync` under the `sync` feature and evaluation only
/// borrows it, so one instance serves all batches. `rhai_engine()` remains
/// the constructor (used by tests and initialization here).
static ENGINE: OnceLock<Engine> = OnceLock::new();

fn shared_engine() -> &'static Engine {
    ENGINE.get_or_init(rhai_engine)
}

fn extract_expression(arg: &ColumnarValue, row: usize) -> Result<Option<String>> {
    match scalar_at(arg, row)? {
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            Ok(value)
        }
        ScalarValue::Null => Ok(None),
        other => exec_err!("`rhai_eval` expects a STRING expression, got {other:?}"),
    }
}

/// Field names of a vars `StructArray`, or `None` when the array is not a struct.
fn struct_field_names_for(array: &ArrayRef) -> Result<Option<Vec<ImmutableString>>> {
    let Some(array) = array.as_any().downcast_ref::<StructArray>() else {
        return Ok(None);
    };
    Ok(Some(struct_field_names(array)))
}

/// Identifiers of every field of the vars struct, collected once per batch so
/// the per-row scope rebuild does not clone a name per column per row.
fn struct_field_names(array: &StructArray) -> Vec<ImmutableString> {
    array
        .fields()
        .iter()
        .map(|field| ImmutableString::from(field.name().as_str()))
        .collect()
}

fn build_scope(
    arg: &ColumnarValue,
    row: usize,
    names: &[ImmutableString],
    promote_integers: bool,
) -> Result<Scope<'static>> {
    let scalar = scalar_at(arg, row)?;
    match scalar {
        ScalarValue::Struct(array) => struct_scalar_to_scope(&array, names, promote_integers),
        ScalarValue::Null => Ok(Scope::new()),
        other => exec_err!("`rhai_eval` expects vars to be a STRUCT value, got {other:?}"),
    }
}

fn scalar_at(arg: &ColumnarValue, row: usize) -> Result<ScalarValue> {
    match arg {
        ColumnarValue::Scalar(value) => Ok(value.clone()),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(array.as_ref(), row),
    }
}

fn struct_scalar_to_scope(
    array: &StructArray,
    names: &[ImmutableString],
    promote_integers: bool,
) -> Result<Scope<'static>> {
    let mut scope = Scope::new();
    if array.null_count() == array.len() {
        return Ok(scope);
    }
    for (name, column) in names.iter().zip(array.columns()) {
        let value = ScalarValue::try_from_array(column.as_ref(), 0)?;
        scope.push_constant(
            name.clone(),
            scalar_value_to_dynamic(&value, promote_integers)?,
        );
    }
    Ok(scope)
}

fn scalar_value_to_dynamic(value: &ScalarValue, promote_integers: bool) -> Result<Dynamic> {
    // Integer inputs bind as floats when the rule performs division, so
    // `EMP_ID / 2` on `7` yields `3.5` instead of truncated `3`.
    // Note: `as f64` loses precision past 2^53; this matches the contract's
    // `as_f64()` coercion row.
    let integer = |value: Option<i64>| {
        if promote_integers {
            float_dynamic(value.map(|v| v as f64))
        } else {
            integer_dynamic(value)
        }
    };
    let unsigned = |value: Option<u64>| {
        if promote_integers {
            float_dynamic(value.map(|v| v as f64))
        } else {
            unsigned_dynamic(value)
        }
    };
    Ok(match value {
        ScalarValue::Null => Dynamic::UNIT,
        ScalarValue::Boolean(value) => value.map(Dynamic::from).unwrap_or(Dynamic::UNIT),
        ScalarValue::Int8(value) => integer(value.map(i64::from))?,
        ScalarValue::Int16(value) => integer(value.map(i64::from))?,
        ScalarValue::Int32(value) => integer(value.map(i64::from))?,
        ScalarValue::Int64(value) => integer(*value)?,
        ScalarValue::UInt8(value) => unsigned(value.map(u64::from))?,
        ScalarValue::UInt16(value) => unsigned(value.map(u64::from))?,
        ScalarValue::UInt32(value) => unsigned(value.map(u64::from))?,
        ScalarValue::UInt64(value) => unsigned(*value)?,
        ScalarValue::Float32(value) => float_dynamic(value.map(f64::from))?,
        ScalarValue::Float64(value) => float_dynamic(*value)?,
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            value
                .as_ref()
                .map(|v| Dynamic::from(ImmutableString::from(v.as_str())))
                .unwrap_or(Dynamic::UNIT)
        }
        ScalarValue::Struct(array) => {
            Dynamic::from_map(struct_scalar_to_map(array, promote_integers)?)
        }
        other => {
            return exec_err!(
                "`rhai_eval` does not support context value type {other:?}; use primitive or struct fields"
            );
        }
    })
}

fn struct_scalar_to_map(array: &StructArray, promote_integers: bool) -> Result<Map> {
    let mut map = Map::new();
    if array.null_count() == array.len() {
        return Ok(map);
    }
    for (field, column) in array.fields().iter().zip(array.columns()) {
        let value = ScalarValue::try_from_array(column.as_ref(), 0)?;
        map.insert(
            field.name().into(),
            scalar_value_to_dynamic(&value, promote_integers)?,
        );
    }
    Ok(map)
}

fn integer_dynamic(value: Option<i64>) -> Result<Dynamic> {
    match value {
        Some(value) => Ok(Dynamic::from(i64_to_rhai_int(value)?)),
        None => Ok(Dynamic::UNIT),
    }
}

fn unsigned_dynamic(value: Option<u64>) -> Result<Dynamic> {
    match value {
        Some(value) => Ok(Dynamic::from(u64_to_rhai_int(value)?)),
        None => Ok(Dynamic::UNIT),
    }
}

fn float_dynamic(value: Option<f64>) -> Result<Dynamic> {
    match value {
        Some(value) if value.is_finite() => Ok(Dynamic::from(value as FLOAT)),
        Some(_) => exec_err!("`rhai_eval` cannot encode NaN or infinity"),
        None => Ok(Dynamic::UNIT),
    }
}

fn i64_to_rhai_int(value: i64) -> Result<INT> {
    INT::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!(
            "`rhai_eval` cannot encode integer {value} as a Rhai INT"
        ))
    })
}

fn u64_to_rhai_int(value: u64) -> Result<INT> {
    INT::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!(
            "`rhai_eval` cannot encode unsigned integer {value} as a Rhai INT"
        ))
    })
}

fn stringify_rhai_result(value: Dynamic) -> Result<Option<String>> {
    if value.is_unit() {
        return Ok(None);
    }
    // `read_lock` probes the type without cloning; only arrays and maps
    // (below) pay for a clone, and only when the value actually is one.
    if let Some(value) = value.read_lock::<bool>() {
        return Ok(Some(value.to_string()));
    }
    if let Some(value) = value.read_lock::<INT>() {
        return Ok(Some(value.to_string()));
    }
    if let Some(value) = value.read_lock::<FLOAT>() {
        // Rhai `Dynamic` rendering: shortest round-trip (`3.5`, `7.0`, `inf`).
        return Ok(Some(Dynamic::from(*value).to_string()));
    }
    if let Some(value) = value.read_lock::<ImmutableString>() {
        return Ok(Some(value.to_string()));
    }
    if let Some(value) = value.read_lock::<RhaiArray>() {
        return Ok(Some(stringify_rhai_array((*value).clone())?));
    }
    if let Some(value) = value.read_lock::<Map>() {
        return Ok(Some(stringify_rhai_map((*value).clone())?));
    }
    exec_err!("`rhai_eval` returned unsupported value type")
}

fn stringify_rhai_array(value: RhaiArray) -> Result<String> {
    let items = value
        .into_iter()
        .map(stringify_rhai_json_like)
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("[{}]", items.join(",")))
}

fn stringify_rhai_map(value: Map) -> Result<String> {
    let items = value
        .into_iter()
        .map(|(key, value)| {
            Ok(format!(
                "{}:{}",
                quote_json_string(key.as_str()),
                stringify_rhai_json_like(value)?
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("{{{}}}", items.join(",")))
}

fn stringify_rhai_json_like(value: Dynamic) -> Result<String> {
    if value.is_unit() {
        return Ok("null".to_string());
    }
    if let Some(value) = value.read_lock::<bool>() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.read_lock::<INT>() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.read_lock::<FLOAT>() {
        return Ok(Dynamic::from(*value).to_string());
    }
    if let Some(value) = value.read_lock::<ImmutableString>() {
        return Ok(quote_json_string(value.as_str()));
    }
    if let Some(value) = value.read_lock::<RhaiArray>() {
        return stringify_rhai_array((*value).clone());
    }
    if let Some(value) = value.read_lock::<Map>() {
        return stringify_rhai_map((*value).clone());
    }
    exec_err!("`rhai_eval` returned unsupported nested value type")
}

fn quote_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use datafusion::common::{DataFusionError, Result};
    use datafusion::logical_expr::ColumnarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{ScalarValue, exec_err};
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;

    #[test]
    fn test_rhai_eval_if_expression() -> Result<()> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("col1", DataType::Int64, true)),
            Arc::new(Field::new("col2", DataType::Int64, true)),
            Arc::new(Field::new("col3", DataType::Int64, true)),
            Arc::new(Field::new("col4", DataType::Int64, true)),
            Arc::new(Field::new("col5", DataType::Int64, true)),
        ]);
        let vars = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(5), Some(1), Some(0)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(3), Some(2), Some(2)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(10), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(99), Some(88), Some(77)])) as ArrayRef,
            ],
            None,
        )?;

        let result = RhaiEval::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "if col1 > col2 { col3 + col4 } else { col5 }".to_string(),
                ))),
                ColumnarValue::Array(Arc::new(vars) as ArrayRef),
            ],
            arg_fields: vec![
                Arc::new(Field::new("expr", DataType::Utf8, false)),
                Arc::new(Field::new("vars", DataType::Struct(fields), true)),
            ],
            number_rows: 3,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;

        let ColumnarValue::Array(array) = result else {
            return exec_err!("expected array result from `rhai_eval`");
        };
        let values = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("expected Utf8 output".to_string()))?
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(values, vec![Some("11"), Some("88"), Some("77")]);
        Ok(())
    }

    fn year_vars() -> Result<(Fields, ArrayRef)> {
        let fields = Fields::from(vec![
            Arc::new(Field::new("SCENARIO", DataType::Utf8, true)),
            Arc::new(Field::new("YEAR", DataType::Utf8, true)),
        ]);
        let vars = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("C")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("2024"), None, Some("")])) as ArrayRef,
            ],
            None,
        )?;
        Ok((fields, Arc::new(vars) as ArrayRef))
    }

    fn invoke_rhai(
        expr: ColumnarValue,
        vars: ArrayRef,
        fields: Fields,
        number_rows: usize,
    ) -> Result<Vec<Option<String>>> {
        let result = RhaiEval::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![expr, ColumnarValue::Array(vars)],
            arg_fields: vec![
                Arc::new(Field::new("expr", DataType::Utf8, false)),
                Arc::new(Field::new("vars", DataType::Struct(fields), true)),
            ],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let ColumnarValue::Array(array) = result else {
            return exec_err!("expected array result from `rhai_eval`");
        };
        let values = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("expected Utf8 output".to_string()))?
            .iter()
            .map(|v| v.map(str::to_string))
            .collect::<Vec<_>>();
        Ok(values)
    }

    fn utf8_expr(value: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string())))
    }

    /// Builds one `invoke_with_args` call with a scalar expression and either
    /// scalar or array vars, so the `Scalar`/`Array` paths can be compared.
    fn invoke_rhai_with(
        expr: ColumnarValue,
        vars: ColumnarValue,
        fields: &Fields,
        number_rows: usize,
    ) -> Result<Vec<Option<String>>> {
        let result = RhaiEval::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![expr, vars],
            arg_fields: vec![
                Arc::new(Field::new("expr", DataType::Utf8, false)),
                Arc::new(Field::new("vars", DataType::Struct(fields.clone()), true)),
            ],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let ColumnarValue::Array(array) = result else {
            return exec_err!("expected array result from `rhai_eval`");
        };
        Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("expected Utf8 output".to_string()))?
            .iter()
            .map(|v| v.map(str::to_string))
            .collect())
    }

    /// The hoisted scalar-expression/scalar-vars path must produce exactly what
    /// the per-row rebuild path produces, including rules that divide.
    #[test]
    fn test_rhai_eval_scalar_and_array_paths_agree() -> Result<()> {
        let (fields, vars) = year_vars()?;
        let rows = 3;
        // Three identical rows: as a scalar they contribute row 0 to every row,
        // so the array-vars run exercises the per-row rebuild with the same
        // values the reused scope holds.
        let repeated = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("A"), Some("A"), Some("A")])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("2024"),
                    Some("2024"),
                    Some("2024"),
                ])) as ArrayRef,
            ],
            None,
        )?;
        let one_row = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("A")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("2024")])) as ArrayRef,
            ],
            None,
        )?;
        for expr in ["YEAR != ()", "is_null(YEAR)", "YEAR ?? \"N/A\"", "7 / 2"] {
            let scalar_vars = invoke_rhai_with(
                utf8_expr(expr),
                ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(one_row.clone()))),
                &fields,
                rows,
            )?;
            let array_vars = invoke_rhai_with(
                utf8_expr(expr),
                ColumnarValue::Array(Arc::new(repeated.clone()) as ArrayRef),
                &fields,
                rows,
            )?;
            assert_eq!(
                scalar_vars, array_vars,
                "scalar/array paths disagree: {expr}"
            );
            assert_eq!(scalar_vars.len(), rows);
        }
        // A scalar NULL expression yields all-NULL output without evaluating.
        let nulls = invoke_rhai_with(
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(one_row))),
            &fields,
            rows,
        )?;
        assert_eq!(nulls, vec![None, None, None]);
        // Sanity: the array-vars path still sees genuinely per-row values.
        let per_row = invoke_rhai_with(
            utf8_expr("YEAR ?? \"N/A\""),
            ColumnarValue::Array(vars),
            &fields,
            rows,
        )?;
        assert_eq!(
            per_row,
            vec![
                Some("2024".to_string()),
                Some("N/A".to_string()),
                Some(String::new())
            ]
        );
        Ok(())
    }

    #[test]
    fn test_rhai_eval_null_is_unit() -> Result<()> {
        let (fields, vars) = year_vars()?;
        let values = invoke_rhai(utf8_expr("YEAR != ()"), vars.clone(), fields.clone(), 3)?;
        assert_eq!(
            values,
            vec![
                Some("true".to_string()),
                Some("false".to_string()),
                Some("true".to_string())
            ]
        );
        let values = invoke_rhai(utf8_expr("YEAR == ()"), vars.clone(), fields.clone(), 3)?;
        assert_eq!(
            values,
            vec![
                Some("false".to_string()),
                Some("true".to_string()),
                Some("false".to_string())
            ]
        );
        let values = invoke_rhai(utf8_expr("type_of(YEAR) == \"()\""), vars, fields, 3)?;
        assert_eq!(
            values,
            vec![
                Some("false".to_string()),
                Some("true".to_string()),
                Some("false".to_string())
            ]
        );
        Ok(())
    }

    #[test]
    fn test_rhai_eval_is_null_helpers() -> Result<()> {
        let (fields, vars) = year_vars()?;
        let values = invoke_rhai(utf8_expr("is_null(YEAR)"), vars.clone(), fields.clone(), 3)?;
        assert_eq!(
            values,
            vec![
                Some("false".to_string()),
                Some("true".to_string()),
                Some("false".to_string())
            ]
        );
        let values = invoke_rhai(
            utf8_expr("is_not_null(YEAR)"),
            vars.clone(),
            fields.clone(),
            3,
        )?;
        assert_eq!(
            values,
            vec![
                Some("true".to_string()),
                Some("false".to_string()),
                Some("true".to_string())
            ]
        );
        let values = invoke_rhai(utf8_expr("YEAR ?? \"N/A\""), vars, fields, 3)?;
        assert_eq!(
            values,
            vec![
                Some("2024".to_string()),
                Some("N/A".to_string()),
                Some(String::new())
            ]
        );
        Ok(())
    }

    #[test]
    fn test_rhai_eval_varying_expressions() -> Result<()> {
        let (fields, vars) = year_vars()?;
        let exprs = StringArray::from(vec![Some("YEAR != ()"), Some("is_null(YEAR)")]);
        let values = invoke_rhai(
            ColumnarValue::Array(Arc::new(exprs) as ArrayRef),
            vars,
            fields,
            2,
        )?;
        assert_eq!(
            values,
            vec![Some("true".to_string()), Some("true".to_string())]
        );
        Ok(())
    }

    fn eval_direct(expr: &str) -> std::result::Result<Dynamic, Box<rhai::EvalAltResult>> {
        rhai_engine().eval::<Dynamic>(expr)
    }

    #[test]
    fn test_engine_limits_and_disabled_symbols() -> Result<()> {
        // 1M-operation budget: an infinite loop surfaces an error, not a hang.
        assert!(eval_direct("while true {}").is_err());
        // 100KB string bound: repeated concatenation past the bound fails.
        assert!(eval_direct("let s = \"\"; for i in 0..200000 { s += \"x\"; }").is_err());
        // 10K array bound.
        assert!(eval_direct("let a = []; for i in 0..20000 { a.push(i); }").is_err());
        // `eval` and `import` are disabled.
        assert!(eval_direct("eval(\"40 + 2\")").is_err());
        assert!(eval_direct("import \"foo\" as bar;").is_err());
        // Small workloads stay well inside the bounds.
        assert_eq!(
            eval_direct("let s = \"\"; for i in 0..8 { s += \"x\"; } s")
                .unwrap_or(Dynamic::UNIT)
                .to_string(),
            "xxxxxxxx"
        );
        Ok(())
    }

    fn single_string_field(name: &str, values: Vec<Option<&str>>) -> Result<(Fields, ArrayRef)> {
        let fields = Fields::from(vec![Arc::new(Field::new(name, DataType::Utf8, true))]);
        let vars = StructArray::try_new(
            fields.clone(),
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
            None,
        )?;
        Ok((fields, Arc::new(vars) as ArrayRef))
    }

    fn single_int_field(name: &str, values: Vec<Option<i64>>) -> Result<(Fields, ArrayRef)> {
        use datafusion::arrow::array::Int64Array;
        let fields = Fields::from(vec![Arc::new(Field::new(name, DataType::Int64, true))]);
        let vars = StructArray::try_new(
            fields.clone(),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
            None,
        )?;
        Ok((fields, Arc::new(vars) as ArrayRef))
    }

    #[test]
    fn test_regex_match_contract() -> Result<()> {
        let (fields, vars) = single_string_field("IV1", vec![Some("ABC12"), Some("abcd")])?;
        let values = invoke_rhai(
            utf8_expr("regex_match(IV1, \"^[A-Z]{3}[0-9]{2}$\")"),
            vars,
            fields,
            2,
        )?;
        assert_eq!(
            values,
            vec![Some("true".to_string()), Some("false".to_string())]
        );
        // Invalid patterns evaluate to `false`, never an error.
        let (fields, vars) = single_string_field("IV1", vec![Some("ABC12")])?;
        let values = invoke_rhai(utf8_expr("regex_match(IV1, \"(\")"), vars, fields, 1)?;
        assert_eq!(values, vec![Some("false".to_string())]);
        Ok(())
    }

    #[test]
    fn test_integer_division_contract() -> Result<()> {
        // Scope promotion: int input binds as float when the rule has `/`.
        let (fields, vars) = single_int_field("EMP_ID", vec![Some(7)])?;
        let values = invoke_rhai(utf8_expr("EMP_ID / 2"), vars, fields, 1)?;
        assert_eq!(values, vec![Some("3.5".to_string())]);
        // Literal rewriting: bare int literals gain `.0`.
        let (fields, vars) = single_int_field("EMP_ID", vec![Some(7)])?;
        let values = invoke_rhai(utf8_expr("7 / 2"), vars, fields, 1)?;
        assert_eq!(values, vec![Some("3.5".to_string())]);
        // No `/` in the rule: integers stay integers.
        let (fields, vars) = single_int_field("EMP_ID", vec![Some(7)])?;
        let values = invoke_rhai(utf8_expr("EMP_ID + 1"), vars, fields, 1)?;
        assert_eq!(values, vec![Some("8".to_string())]);
        // Identifier digits are never rewritten (`IV1`, not `IV1.0`).
        let (fields, vars) = single_int_field("IV1", vec![Some(5)])?;
        let values = invoke_rhai(utf8_expr("IV1 + 1"), vars, fields, 1)?;
        assert_eq!(values, vec![Some("6".to_string())]);
        Ok(())
    }

    #[test]
    fn test_null_comparisons_for_int_and_string() -> Result<()> {
        let (fields, vars) = single_int_field("X", vec![None, Some(3)])?;
        let values = invoke_rhai(utf8_expr("X == ()"), vars.clone(), fields.clone(), 2)?;
        assert_eq!(
            values,
            vec![Some("true".to_string()), Some("false".to_string())]
        );
        let values = invoke_rhai(utf8_expr("X != ()"), vars, fields, 2)?;
        assert_eq!(
            values,
            vec![Some("false".to_string()), Some("true".to_string())]
        );
        Ok(())
    }

    #[test]
    fn test_result_rendering_contract() -> Result<()> {
        let (fields, vars) = year_vars()?;
        // Floats render shortest-roundtrip, keeping `.0`.
        for (expr, want) in [
            ("3.5", "3.5"),
            ("7.0", "7.0"),
            ("5.0", "5.0"),
            ("true", "true"),
            ("42", "42"),
        ] {
            let values = invoke_rhai(utf8_expr(expr), vars.clone(), fields.clone(), 3)?;
            assert_eq!(
                values,
                vec![
                    Some(want.to_string()),
                    Some(want.to_string()),
                    Some(want.to_string())
                ],
                "expr: {expr}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_integer_overflow_is_an_error() -> Result<()> {
        // Probe for §7 CONFIRM: default checked arithmetic, `fast_operators` off.
        let (fields, vars) = year_vars()?;
        let result = invoke_rhai(utf8_expr("9223372036854775807 + 1"), vars, fields, 3);
        assert!(result.is_err(), "integer overflow must fail");
        if let Err(e) = result {
            assert!(
                e.to_string().contains("failed to evaluate expression"),
                "unexpected error: {e}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_float_division_by_zero_is_inf() -> Result<()> {
        // Probe for §7 CONFIRM: IEEE semantics, no error.
        let (fields, vars) = year_vars()?;
        let values = invoke_rhai(utf8_expr("1.0 / 0.0"), vars, fields, 3)?;
        assert_eq!(
            values,
            vec![
                Some("inf".to_string()),
                Some("inf".to_string()),
                Some("inf".to_string())
            ]
        );
        Ok(())
    }

    #[test]
    fn test_malformed_expression_error_shape() -> Result<()> {
        let (fields, vars) = year_vars()?;
        let result = invoke_rhai(utf8_expr("YEAR != ("), vars, fields, 3);
        assert!(result.is_err(), "malformed rule must fail");
        if let Err(e) = result {
            assert!(
                e.to_string()
                    .contains("`rhai_eval` failed to evaluate expression `YEAR != (`: "),
                "unexpected error: {e}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_assigning_to_inputs_fails() -> Result<()> {
        // Bindings are immutable constants; results never leak rule-to-rule.
        let (fields, vars) = single_int_field("EMP_ID", vec![Some(7)])?;
        assert!(invoke_rhai(utf8_expr("EMP_ID = 5"), vars, fields, 1).is_err());
        Ok(())
    }

    mod int_arithmetic_oracle {
        use proptest::prelude::*;
        use rhai::Dynamic;

        use crate::rhai_engine;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(128))]
            #[test]
            fn int_arithmetic_matches_rust(a in -1000i64..1000i64, b in -1000i64..1000i64, op in 0..3u8) {
                let engine = rhai_engine();
                let (sym, expected) = match op {
                    0 => ("+", a.checked_add(b)),
                    1 => ("-", a.checked_sub(b)),
                    _ => ("*", a.checked_mul(b)),
                };
                let expr = format!("{a} {sym} {b}");
                match (engine.eval::<Dynamic>(expr.as_str()), expected) {
                    (Ok(got), Some(want)) => prop_assert_eq!(got.to_string(), want.to_string()),
                    (Err(_), None) => {}
                    (Ok(got), None) => prop_assert!(false, "expected overflow error for {expr}, got {got}"),
                    (Err(e), Some(want)) => prop_assert!(false, "expected {want} for {expr}, got error {e}"),
                }
            }
        }
    }
}
