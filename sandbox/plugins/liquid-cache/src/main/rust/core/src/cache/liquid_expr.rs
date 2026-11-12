use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, DynamicFilterPhysicalExpr, Literal, TryCastExpr,
};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};

use crate::sync::Arc;
use crate::utils::get_bytes_needle;

/// A predicate expression validated for LiquidCache predicate evaluation.
#[derive(Clone)]
pub struct LiquidExpr {
    expr: Arc<dyn PhysicalExpr>,
}

impl std::fmt::Debug for LiquidExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiquidExpr")
            .field("expr", &self.expr.to_string())
            .finish()
    }
}

impl LiquidExpr {
    /// Validate and wrap a physical expression for LiquidCache predicate evaluation.
    ///
    /// Returns `None` when the expression shape or operator is unsupported for the
    /// provided column type.
    pub fn try_new(expr: Arc<dyn PhysicalExpr>, data_type: &DataType) -> Option<Self> {
        let normalized = unwrap_dynamic_filter(&expr)?;
        if supports_expr(&normalized, data_type) {
            Some(Self { expr: normalized })
        } else {
            None
        }
    }

    /// Get the underlying validated physical expression.
    pub fn physical_expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    #[cfg(test)]
    pub(crate) fn new_unchecked(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { expr }
    }
}

fn unwrap_dynamic_filter(expr: &Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>> {
    if let Some(dynamic_filter) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
        dynamic_filter.current().ok()
    } else {
        Some(expr.clone())
    }
}

fn supports_expr(expr: &Arc<dyn PhysicalExpr>, data_type: &DataType) -> bool {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        return supports_binary_expr(binary, data_type);
    }

    if let Some(literal) = expr.downcast_ref::<Literal>() {
        return matches!(literal.value(), ScalarValue::Boolean(Some(_))) && is_byte_like(data_type);
    }

    false
}

fn supports_binary_expr(binary: &BinaryExpr, data_type: &DataType) -> bool {
    let Some(literal) = binary.right().downcast_ref::<Literal>() else {
        return false;
    };
    let op = binary.op();
    if is_byte_like(data_type) {
        if !is_column_like(binary.left()) {
            return false;
        }

        match op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => get_bytes_needle(literal.value()).is_some(),
            _ => false,
        }
    } else if is_numeric_like(data_type) {
        matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) && (is_column_like(binary.left()) || is_to_timestamp_seconds_column(binary.left()))
    } else {
        false
    }
}

fn is_column_like(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.downcast_ref::<Column>().is_some() {
        return true;
    }
    if let Some(cast_expr) = expr.downcast_ref::<CastExpr>() {
        return is_column_like(cast_expr.expr());
    }
    if let Some(try_cast_expr) = expr.downcast_ref::<TryCastExpr>() {
        return is_column_like(try_cast_expr.expr());
    }
    false
}

fn is_to_timestamp_seconds_column(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if let Some(func) = expr.downcast_ref::<ScalarFunctionExpr>()
        && func.name() == "to_timestamp_seconds"
        && let [arg] = func.args()
    {
        return is_column_like(arg);
    }
    false
}

fn is_byte_like(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::Utf8View | DataType::Binary | DataType::BinaryView => true,
        DataType::Dictionary(_, value_type) => is_byte_like(value_type.as_ref()),
        _ => false,
    }
}

fn is_numeric_like(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) || matches!(data_type, DataType::Timestamp(_, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column};

    #[test]
    fn validates_byte_comparison_with_literal() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("x".to_string())))),
        ));
        let liquid_expr = LiquidExpr::try_new(expr, &DataType::Utf8);
        assert!(liquid_expr.is_some());
    }

    #[test]
    fn validates_numeric_comparison() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(42)))),
        ));
        let liquid_expr = LiquidExpr::try_new(expr, &DataType::Int32);
        assert!(liquid_expr.is_some());
    }

    #[test]
    fn rejects_unsupported_like_expression() {
        use datafusion_physical_expr::expressions::LikeExpr;
        let expr: Arc<dyn PhysicalExpr> = Arc::new(LikeExpr::new(
            false,
            false,
            Arc::new(Column::new("c", 0)),
            Arc::new(Literal::new(ScalarValue::Utf8(Some("%abc%".to_string())))),
        ));
        let liquid_expr = LiquidExpr::try_new(expr, &DataType::Utf8);
        assert!(liquid_expr.is_none());
    }
}
