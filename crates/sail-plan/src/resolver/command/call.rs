// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{Extension, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::lakesource::LakeSourceProcedureOperation;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_logical_plan::procedure::{ProcedureNode, ProcedureOptions};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    /// Resolves `CALL <catalog>.system.<procedure>(...)` into a
    /// [`CatalogCommand::CallProcedure`] (see `sail-catalog`).
    ///
    /// Only the `system` procedures are supported: `rollback_to_snapshot`,
    /// `set_current_snapshot`, and `expire_snapshots`.
    pub(super) async fn resolve_command_call_procedure(
        &self,
        name: spec::ObjectName,
        arguments: Vec<(Option<spec::Identifier>, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let procedure_parts: Vec<String> = name.clone().into();
        // `catalog.system.<procedure>` (e.g. `test.system.rollback_to_snapshot`).
        let [_, system, procedure_name] = procedure_parts.as_slice() else {
            return Err(PlanError::unsupported(format!(
                "CALL requires a fully qualified <catalog>.system.<procedure> name, got '{}'",
                Vec::<String>::from(name.clone()).join(".")
            )));
        };
        if !system.eq_ignore_ascii_case("system") {
            return Err(PlanError::unsupported(format!(
                "CALL only supports the 'system' namespace, got '{system}'"
            )));
        }

        // Reject unknown named arguments up front, mirroring Spark's procedure
        // argument binding (an unrecognized parameter is an error, never a
        // silent no-op).
        let accepted: &[&str] = match procedure_name.to_ascii_lowercase().as_str() {
            "rollback_to_snapshot" => &["table", "snapshot_id"],
            "set_current_snapshot" => &["table", "snapshot_id", "ref"],
            "expire_snapshots" => &["table", "older_than", "retain_last", "snapshot_ids"],
            other => {
                return Err(PlanError::unsupported(format!(
                    "unsupported system procedure: {other}"
                )));
            }
        };
        validate_named_arguments(&arguments, accepted)?;

        let (table, procedure_name, operation) = match procedure_name.to_ascii_lowercase().as_str()
        {
            "rollback_to_snapshot" => {
                let (table, snapshot_id) = self
                    .resolve_table_and_snapshot_id(&arguments, state)
                    .await?;
                (
                    table,
                    "rollback_to_snapshot".to_string(),
                    LakeSourceProcedureOperation::RollbackToSnapshot { snapshot_id },
                )
            }
            "set_current_snapshot" => {
                let (table, snapshot_id, r#ref) = self
                    .resolve_table_and_snapshot_target(&arguments, state)
                    .await?;
                (
                    table,
                    "set_current_snapshot".to_string(),
                    LakeSourceProcedureOperation::SetCurrentSnapshot { snapshot_id, r#ref },
                )
            }
            "expire_snapshots" => {
                let (table, older_than_ms, retain_last, snapshot_ids) = self
                    .resolve_table_and_expire_args(&arguments, state)
                    .await?;
                (
                    table,
                    "expire_snapshots".to_string(),
                    LakeSourceProcedureOperation::ExpireSnapshots {
                        older_than_ms,
                        retain_last,
                        snapshot_ids,
                    },
                )
            }
            other => {
                return Err(PlanError::unsupported(format!(
                    "unsupported system procedure: {other}"
                )));
            }
        };

        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let status = catalog_manager
            .get_table_or_view(&table)
            .await
            .map_err(PlanError::from)?;
        let (format, target_path, properties) = match status.kind {
            TableKind::Table {
                location,
                format,
                properties,
                ..
            } => {
                let normalized_format = format.to_ascii_lowercase();
                let location = location.ok_or_else(|| {
                    PlanError::unsupported("CALL procedures require a table with a location")
                })?;
                (normalized_format, location, properties)
            }
            _ => {
                return Err(PlanError::unsupported(
                    "CALL procedures are only supported on tables, not views",
                ));
            }
        };
        if format != "iceberg" {
            return Err(PlanError::unsupported(format!(
                "CALL procedures are only supported for Iceberg tables, got '{format}'"
            )));
        }

        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                &table,
                LakehouseOperation::Maintenance,
                Some(&format),
                vec![],
            )
            .await?;
        let target_options = vec![OptionLayer::TablePropertyList { items: properties }];

        let node = ProcedureNode::new(ProcedureOptions {
            format,
            procedure_name: vec!["system".to_string(), procedure_name],
            operation,
            target_table: Some(table),
            target_path: Some(target_path),
            target_options,
            target_lakehouse_table: Some(lakehouse_table),
        });
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Resolves the `<table>`, `snapshot_id` arguments for `rollback_to_snapshot`.
    async fn resolve_table_and_snapshot_id(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, i64)> {
        let table = self.resolve_named_arg(arguments, "table", 0, state).await?;
        let table = scalar_to_table_name_parts(&table)?;
        let snapshot_id = self
            .resolve_named_arg(arguments, "snapshot_id", 1, state)
            .await?;
        let snapshot_id = scalar_to_snapshot_id(&snapshot_id)?;
        Ok((table, snapshot_id))
    }

    /// Resolves the `<table>`, `snapshot_id | ref` arguments for `set_current_snapshot`.
    ///
    /// Exactly one of `snapshot_id` (positional 1 or named) and `ref` (named only) must
    /// be provided, mirroring the Iceberg procedure contract.
    async fn resolve_table_and_snapshot_target(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Option<i64>, Option<String>)> {
        let table = self.resolve_named_arg(arguments, "table", 0, state).await?;
        let table = scalar_to_table_name_parts(&table)?;
        let snapshot_id = self
            .resolve_optional_named_arg(arguments, "snapshot_id", 1, state)
            .await?
            .map(|scalar| scalar_to_snapshot_id(&scalar))
            .transpose()?;
        let r#ref = self
            .resolve_optional_named_arg(arguments, "ref", 1, state)
            .await?
            .map(|scalar| scalar_to_table_name(&scalar))
            .transpose()?;
        match (snapshot_id, r#ref.as_ref()) {
            (Some(_), None) | (None, Some(_)) => {}
            (Some(_), Some(_)) => {
                return Err(PlanError::invalid(
                    "Either snapshot_id or ref must be provided, not both",
                ));
            }
            (None, None) => {
                return Err(PlanError::invalid(
                    "Either snapshot_id or ref must be provided for set_current_snapshot",
                ));
            }
        }
        Ok((table, snapshot_id, r#ref))
    }

    /// Resolves the `<table>`, `[older_than]`, `[retain_last]` arguments for
    /// `expire_snapshots`. Both optional arguments resolve to `None` when absent.
    async fn resolve_table_and_expire_args(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        state: &mut PlanResolverState,
    ) -> PlanResult<(Vec<String>, Option<i64>, Option<i32>, Vec<i64>)> {
        let table = self.resolve_named_arg(arguments, "table", 0, state).await?;
        let table = scalar_to_table_name_parts(&table)?;
        let older_than_ms = self
            .resolve_optional_named_arg(arguments, "older_than", 1, state)
            .await?
            .map(|scalar| scalar_to_timestamp_ms(&scalar))
            .transpose()?;
        let retain_last = self
            .resolve_optional_named_arg(arguments, "retain_last", 2, state)
            .await?
            .map(|scalar| scalar_to_i32(&scalar))
            .transpose()?;
        let snapshot_ids = self
            .resolve_optional_named_arg(arguments, "snapshot_ids", 3, state)
            .await?
            .map(|scalar| scalar_to_snapshot_id_list(&scalar))
            .transpose()?
            .unwrap_or_default();
        Ok((table, older_than_ms, retain_last, snapshot_ids))
    }

    /// Resolves an argument by name if present (case-insensitive), otherwise by position
    /// among the unnamed arguments. Returns the argument's constant `ScalarValue`.
    async fn resolve_named_arg(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        name: &str,
        position: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        let expr = arguments
            .iter()
            .find(|(n, _)| {
                n.as_ref()
                    .is_some_and(|n| n.as_ref().eq_ignore_ascii_case(name))
            })
            .map(|(_, e)| e)
            .or_else(|| {
                // Positional fallback: only unnamed arguments count towards `position`,
                // so a named argument cannot shadow a positional slot.
                arguments
                    .iter()
                    .filter(|(n, _)| n.is_none())
                    .nth(position)
                    .map(|(_, e)| e)
            })
            .ok_or_else(|| {
                PlanError::invalid(format!("missing required argument '{name}' for CALL"))
            })?;
        self.evaluate_constant_expr(expr, state).await
    }

    /// Resolves an optional argument by name (case-insensitive) or by position among the
    /// unnamed arguments. Returns `None` when the argument is absent.
    async fn resolve_optional_named_arg(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        name: &str,
        position: usize,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<ScalarValue>> {
        let expr = arguments
            .iter()
            .find(|(n, _)| {
                n.as_ref()
                    .is_some_and(|n| n.as_ref().eq_ignore_ascii_case(name))
            })
            .map(|(_, e)| e)
            .or_else(|| {
                arguments
                    .iter()
                    .filter(|(n, _)| n.is_none())
                    .nth(position)
                    .map(|(_, e)| e)
            });
        let Some(expr) = expr else {
            return Ok(None);
        };
        self.evaluate_constant_expr(expr, state).await.map(Some)
    }

    /// Resolves a `spec::Expr` to a constant `ScalarValue`.
    async fn evaluate_constant_expr(
        &self,
        expr: &spec::Expr,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        let schema = Arc::new(DFSchema::empty());
        let resolved = self
            .resolve_expression(expr.clone(), &schema, state)
            .await?;
        LiteralEvaluator::new()
            .evaluate(&resolved)
            .map_err(|e| PlanError::invalid(format!("CALL argument must be a constant: {e}")))
    }
}

/// Rejects any named argument not in `accepted`, mirroring Spark's procedure
/// argument binding: an unrecognized parameter is an error, never a silent no-op.
fn validate_named_arguments(
    arguments: &[(Option<spec::Identifier>, spec::Expr)],
    accepted: &[&str],
) -> PlanResult<()> {
    for (name, _) in arguments {
        let Some(name) = name else {
            continue;
        };
        let name = String::from(name.clone());
        if !accepted.iter().any(|a| a.eq_ignore_ascii_case(&name)) {
            return Err(PlanError::invalid(format!(
                "unexpected argument '{name}' for CALL procedure; accepted: {}",
                accepted.join(", ")
            )));
        }
    }
    Ok(())
}

/// Extracts the `<snapshot_ids>` argument (an array of integer literals).
fn scalar_to_snapshot_id_list(scalar: &ScalarValue) -> PlanResult<Vec<i64>> {
    use datafusion::arrow::array::{Array, Int64Array};

    let array = match scalar {
        ScalarValue::List(array) => array.values().clone(),
        ScalarValue::LargeList(array) => array.values().clone(),
        _ => {
            return Err(PlanError::invalid(format!(
                "CALL snapshot_ids must be an array of integers, got '{scalar}'"
            )));
        }
    };
    let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        PlanError::invalid(format!(
            "CALL snapshot_ids must be an array of integers, got '{scalar}'"
        ))
    })?;
    Ok((0..values.len())
        .map(|i| values.value(i))
        .collect::<Vec<_>>())
}

/// Splits a dotted table reference like `db.table` into parts.
fn scalar_to_table_name_parts(scalar: &ScalarValue) -> PlanResult<Vec<String>> {
    let table = scalar_to_table_name(scalar)?;
    Ok(table.split('.').map(|s| s.to_string()).collect())
}

/// Extracts the `<table>` argument (a string literal) from a `ScalarValue`.
fn scalar_to_table_name(scalar: &ScalarValue) -> PlanResult<String> {
    match scalar {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Ok(v.clone()),
        _ => Err(PlanError::invalid(format!(
            "CALL table argument must be a string literal, got '{scalar}'"
        ))),
    }
}

/// Extracts the `<snapshot_id>` argument (an integer literal) from a `ScalarValue`.
fn scalar_to_snapshot_id(scalar: &ScalarValue) -> PlanResult<i64> {
    let value = match scalar {
        ScalarValue::Int8(Some(v)) => *v as i64,
        ScalarValue::Int16(Some(v)) => *v as i64,
        ScalarValue::Int32(Some(v)) => *v as i64,
        ScalarValue::Int64(Some(v)) => *v,
        ScalarValue::UInt8(Some(v)) => *v as i64,
        ScalarValue::UInt16(Some(v)) => *v as i64,
        ScalarValue::UInt32(Some(v)) => *v as i64,
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
            PlanError::invalid(format!("CALL snapshot_id is out of range: '{scalar}'"))
        })?,
        _ => {
            return Err(PlanError::invalid(format!(
                "CALL snapshot_id must be an integer literal, got '{scalar}'"
            )));
        }
    };
    Ok(value)
}

/// Extracts an integer literal (i32) from a `ScalarValue`, e.g. `retain_last`.
fn scalar_to_i32(scalar: &ScalarValue) -> PlanResult<i32> {
    let value = match scalar {
        ScalarValue::Int8(Some(v)) => *v as i32,
        ScalarValue::Int16(Some(v)) => *v as i32,
        ScalarValue::Int32(Some(v)) => *v,
        ScalarValue::Int64(Some(v)) => i32::try_from(*v).map_err(|_| {
            PlanError::invalid(format!("CALL integer argument is out of range: '{scalar}'"))
        })?,
        ScalarValue::UInt8(Some(v)) => *v as i32,
        ScalarValue::UInt16(Some(v)) => *v as i32,
        ScalarValue::UInt32(Some(v)) => i32::try_from(*v).map_err(|_| {
            PlanError::invalid(format!("CALL integer argument is out of range: '{scalar}'"))
        })?,
        ScalarValue::UInt64(Some(v)) => i32::try_from(*v).map_err(|_| {
            PlanError::invalid(format!("CALL integer argument is out of range: '{scalar}'"))
        })?,
        _ => {
            return Err(PlanError::invalid(format!(
                "CALL integer argument must be an integer literal, got '{scalar}'"
            )));
        }
    };
    Ok(value)
}

/// Extracts the `<older_than>` argument (a timestamp literal) from a `ScalarValue`.
fn scalar_to_timestamp_ms(scalar: &ScalarValue) -> PlanResult<i64> {
    match scalar {
        ScalarValue::TimestampSecond(Some(v), _) => Ok(*v * 1000),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Ok(v / 1000),
        ScalarValue::TimestampNanosecond(Some(v), _) => Ok(v / 1_000_000),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => parse_timestamp_ms(v),
        _ => Err(PlanError::invalid(format!(
            "CALL older_than must be a timestamp literal, got '{scalar}'"
        ))),
    }
}

/// Parses a timestamp string into milliseconds since the Unix epoch.
fn parse_timestamp_ms(value: &str) -> PlanResult<i64> {
    if let Ok(datetime) = DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.timestamp_millis());
    }
    let parse = |format: &str| NaiveDateTime::parse_from_str(value, format);
    let naive = parse("%Y-%m-%d %H:%M:%S")
        .or_else(|_| parse("%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| parse("%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| parse("%Y-%m-%dT%H:%M:%S%.f"))
        .map_err(|e| {
            PlanError::invalid(format!(
                "invalid timestamp '{value}' for CALL; expected e.g. 'YYYY-MM-DD HH:MM:SS': {e}"
            ))
        })?;
    Ok(naive.and_utc().timestamp_millis())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use sail_common::spec;

    use super::validate_named_arguments;

    fn named(name: &str) -> (Option<spec::Identifier>, spec::Expr) {
        (
            Some(name.into()),
            spec::Expr::Literal(spec::Literal::Int64 { value: Some(1) }),
        )
    }

    fn unnamed() -> (Option<spec::Identifier>, spec::Expr) {
        (
            None,
            spec::Expr::Literal(spec::Literal::Int64 { value: Some(1) }),
        )
    }

    #[test]
    fn accepts_known_named_arguments_case_insensitively() {
        let args = vec![named("TABLE"), named("Snapshot_Id")];
        validate_named_arguments(&args, &["table", "snapshot_id"]).unwrap();
    }

    #[test]
    fn ignores_positional_arguments() {
        let args = vec![unnamed(), unnamed()];
        validate_named_arguments(&args, &["table"]).unwrap();
    }

    #[test]
    fn rejects_unknown_named_arguments() {
        // `expire_snapshots` does not support `snapshot_ids` in this port, so a
        // caller passing it must get an error rather than a silent no-op.
        let args = vec![named("table"), named("snapshot_ids")];
        let error =
            validate_named_arguments(&args, &["table", "older_than", "retain_last"]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unexpected argument 'snapshot_ids'"),
            "unexpected error: {error}"
        );
    }
}
