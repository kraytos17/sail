use std::sync::Arc;

use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_common::{DFSchema, ScalarValue};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::{LakehouseOperation, TableKind};
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_logical_plan::call_procedure::{CallProcedure, CallProcedureNode};

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves `CALL <catalog>.system.<procedure>(...)`.
    ///
    /// Only the `system` procedures heimdall uses are supported:
    /// `rollback_to_snapshot`, `set_current_snapshot`, and `expire_snapshots`.
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

        let procedure = match procedure_name.to_ascii_lowercase().as_str() {
            "rollback_to_snapshot" => {
                let (table, snapshot_id) = self
                    .resolve_table_and_snapshot_id(&arguments, state)
                    .await?;
                CallProcedure::RollbackToSnapshot { table, snapshot_id }
            }
            "set_current_snapshot" => {
                let (table, snapshot_id, r#ref) = self
                    .resolve_table_and_snapshot_target(&arguments, state)
                    .await?;
                CallProcedure::SetCurrentSnapshot {
                    table,
                    snapshot_id,
                    r#ref,
                }
            }
            "expire_snapshots" => {
                let (table, older_than_ms, retain_last) = self
                    .resolve_table_and_expire_args(&arguments, state)
                    .await?;
                CallProcedure::ExpireSnapshots {
                    table,
                    older_than_ms,
                    retain_last,
                }
            }
            other => {
                return Err(PlanError::unsupported(format!(
                    "unsupported system procedure: {other}"
                )));
            }
        };

        // Resolve the target table; it must exist and be an Iceberg table.
        let table_parts: Vec<String> = split_dotted(&procedure.table_name());
        let table_name: Vec<String> = table_parts.clone();
        let catalog_manager = self.ctx.extension::<CatalogManager>()?;
        let table_status = catalog_manager
            .get_table_or_view(&table_parts)
            .await
            .map_err(PlanError::from)?;
        let (format, table_location, properties) = match &table_status.kind {
            TableKind::Table {
                location,
                format,
                properties,
                ..
            } => (format.clone(), location.clone(), properties.clone()),
            _ => {
                return Err(PlanError::unsupported(
                    "CALL procedures are only supported on tables, not views",
                ));
            }
        };
        if !format.eq_ignore_ascii_case("iceberg") {
            return Err(PlanError::unsupported(format!(
                "CALL procedures are only supported for Iceberg tables, got '{format}'"
            )));
        }
        let table_location = table_location
            .ok_or_else(|| PlanError::unsupported("CALL on tables without location"))?;

        let lakehouse_table = self
            .resolve_lakehouse_table_context(
                &table_name,
                LakehouseOperation::Maintenance,
                Some(&format),
                vec![],
            )
            .await?;

        let options = vec![OptionLayer::TablePropertyList { items: properties }];
        let node = CallProcedureNode::new(
            procedure,
            table_location,
            table_name,
            options,
            Some(lakehouse_table),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
    }

    /// Resolves the `<table>`, `<snapshot_id>` positional/named arguments for
    /// `rollback_to_snapshot`.
    async fn resolve_table_and_snapshot_id(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        state: &mut PlanResolverState,
    ) -> PlanResult<(String, i64)> {
        let table = self.resolve_named_arg(arguments, "table", 0, state).await?;
        let table = scalar_to_table_name(&table)?;
        let snapshot_id = self
            .resolve_named_arg(arguments, "snapshot_id", 1, state)
            .await?;
        let snapshot_id = scalar_to_snapshot_id(&snapshot_id)?;
        Ok((table, snapshot_id))
    }

    /// Resolves the `<table>`, `snapshot_id | ref` arguments for `set_current_snapshot`.
    ///
    /// Exactly one of `snapshot_id` (positional 1 or named) and `ref` (named only) must be
    /// provided, mirroring the Iceberg procedure contract.
    async fn resolve_table_and_snapshot_target(
        &self,
        arguments: &[(Option<spec::Identifier>, spec::Expr)],
        state: &mut PlanResolverState,
    ) -> PlanResult<(String, Option<i64>, Option<String>)> {
        let table = self.resolve_named_arg(arguments, "table", 0, state).await?;
        let table = scalar_to_table_name(&table)?;
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
    ) -> PlanResult<(String, Option<i64>, Option<i32>)> {
        let table = self.resolve_named_arg(arguments, "table", 0, state).await?;
        let table = scalar_to_table_name(&table)?;
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
        Ok((table, older_than_ms, retain_last))
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

/// Splits a dotted table reference like `db.table` into parts.
fn split_dotted(reference: &str) -> Vec<String> {
    reference.split('.').map(|s| s.to_string()).collect()
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
///
/// Matches the timestamp `ScalarValue` variants directly (per the time-travel idiom in
/// `resolver/query/time_travel.rs`) instead of round-tripping through a formatted string,
/// so `TIMESTAMP '...'` literals resolve to an epoch value rather than an unparseable
/// raw-integer string.
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
///
/// Mirrors `parse_timestamp_to_ms` in `sail-iceberg` (RFC3339 first, then a set of
/// naive datetime formats), mapping parse failures to a `PlanError`.
fn parse_timestamp_ms(value: &str) -> PlanResult<i64> {
    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.timestamp_millis());
    }
    let parse = |format: &str| chrono::NaiveDateTime::parse_from_str(value, format);
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
