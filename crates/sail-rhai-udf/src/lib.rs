use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::ScalarFunctionArgs;
use datafusion_expr::registry::FunctionRegistry;
use rhai::{AST, Array as RhaiArray, Dynamic, Engine, FLOAT, INT, ImmutableString, Map, Scope};
use sail_catalog::manager::CatalogManager;
use sail_common_datafusion::extension::SessionExtensionAccessor;

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
        let engine = rhai_engine();
        // Expressions are usually constant across rows, so compile once and
        // recompile only when the expression text changes.
        let mut compiled: Option<(String, AST)> = None;
        let mut values = Vec::with_capacity(row_count);

        for row in 0..row_count {
            let Some(expr) = extract_expression(expr_arg, row)? else {
                values.push(None);
                continue;
            };
            let expr = expr.trim().to_string();
            if compiled.as_ref().is_none_or(|(cached, _)| *cached != expr) {
                let ast = engine.compile(expr.as_str()).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "`rhai_eval` failed to evaluate expression `{expr}`: {e}",
                    ))
                })?;
                compiled = Some((expr.clone(), ast));
            }
            let Some((_, ast)) = compiled.as_ref() else {
                return exec_err!("`rhai_eval` failed to compile expression");
            };
            let mut scope = build_scope(vars_arg, row)?;
            let value = engine
                .eval_ast_with_scope::<Dynamic>(&mut scope, ast)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "`rhai_eval` failed to evaluate expression `{expr}`: {e}",
                    ))
                })?;
            values.push(stringify_rhai_result(value)?);
        }

        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(values)) as ArrayRef
        ))
    }
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
/// SQL `NULL` binds as Rhai unit (`()`) — see `scalar_value_to_dynamic` — so
/// null checks spell as `YEAR != ()` or `type_of(YEAR) == "()"`. The `is_null`
/// / `is_not_null` helpers below offer the same check under a Spark-like name.
fn rhai_engine() -> Engine {
    let mut engine = Engine::new();
    engine.register_fn("is_null", |value: Dynamic| value.is_unit());
    engine.register_fn("is_not_null", |value: Dynamic| !value.is_unit());
    engine
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

fn build_scope(arg: &ColumnarValue, row: usize) -> Result<Scope<'static>> {
    let scalar = scalar_at(arg, row)?;
    match scalar {
        ScalarValue::Struct(array) => struct_scalar_to_scope(&array),
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

fn struct_scalar_to_scope(array: &StructArray) -> Result<Scope<'static>> {
    let mut scope = Scope::new();
    if array.null_count() == array.len() {
        return Ok(scope);
    }
    for (field, column) in array.fields().iter().zip(array.columns()) {
        let value = ScalarValue::try_from_array(column.as_ref(), 0)?;
        scope.push_dynamic(field.name().clone(), scalar_value_to_dynamic(&value)?);
    }
    Ok(scope)
}

fn scalar_value_to_dynamic(value: &ScalarValue) -> Result<Dynamic> {
    Ok(match value {
        ScalarValue::Null => Dynamic::UNIT,
        ScalarValue::Boolean(value) => value.map(Dynamic::from).unwrap_or(Dynamic::UNIT),
        ScalarValue::Int8(value) => integer_dynamic(value.map(i64::from))?,
        ScalarValue::Int16(value) => integer_dynamic(value.map(i64::from))?,
        ScalarValue::Int32(value) => integer_dynamic(value.map(i64::from))?,
        ScalarValue::Int64(value) => integer_dynamic(*value)?,
        ScalarValue::UInt8(value) => unsigned_dynamic(value.map(u64::from))?,
        ScalarValue::UInt16(value) => unsigned_dynamic(value.map(u64::from))?,
        ScalarValue::UInt32(value) => unsigned_dynamic(value.map(u64::from))?,
        ScalarValue::UInt64(value) => unsigned_dynamic(*value)?,
        ScalarValue::Float32(value) => float_dynamic(value.map(f64::from))?,
        ScalarValue::Float64(value) => float_dynamic(*value)?,
        ScalarValue::Utf8(value) | ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
            value
                .as_ref()
                .map(|v| Dynamic::from(ImmutableString::from(v.as_str())))
                .unwrap_or(Dynamic::UNIT)
        }
        ScalarValue::Struct(array) => Dynamic::from_map(struct_scalar_to_map(array)?),
        other => {
            return exec_err!(
                "`rhai_eval` does not support context value type {other:?}; use primitive or struct fields"
            );
        }
    })
}

fn struct_scalar_to_map(array: &StructArray) -> Result<Map> {
    let mut map = Map::new();
    if array.null_count() == array.len() {
        return Ok(map);
    }
    for (field, column) in array.fields().iter().zip(array.columns()) {
        let value = ScalarValue::try_from_array(column.as_ref(), 0)?;
        map.insert(field.name().into(), scalar_value_to_dynamic(&value)?);
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
    if let Some(value) = value.clone().try_cast::<bool>() {
        return Ok(Some(value.to_string()));
    }
    if let Some(value) = value.clone().try_cast::<INT>() {
        return Ok(Some(value.to_string()));
    }
    if let Some(value) = value.clone().try_cast::<FLOAT>() {
        return Ok(Some(stringify_float(value)));
    }
    if let Some(value) = value.clone().try_cast::<ImmutableString>() {
        return Ok(Some(value.to_string()));
    }
    if let Some(value) = value.clone().try_cast::<RhaiArray>() {
        return Ok(Some(stringify_rhai_array(value)?));
    }
    if let Some(value) = value.try_cast::<Map>() {
        return Ok(Some(stringify_rhai_map(value)?));
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
    if let Some(value) = value.clone().try_cast::<bool>() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.clone().try_cast::<INT>() {
        return Ok(value.to_string());
    }
    if let Some(value) = value.clone().try_cast::<FLOAT>() {
        return Ok(stringify_float(value));
    }
    if let Some(value) = value.clone().try_cast::<ImmutableString>() {
        return Ok(quote_json_string(value.as_str()));
    }
    if let Some(value) = value.clone().try_cast::<RhaiArray>() {
        return stringify_rhai_array(value);
    }
    if let Some(value) = value.try_cast::<Map>() {
        return stringify_rhai_map(value);
    }
    exec_err!("`rhai_eval` returned unsupported nested value type")
}

fn stringify_float(value: FLOAT) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
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
}
