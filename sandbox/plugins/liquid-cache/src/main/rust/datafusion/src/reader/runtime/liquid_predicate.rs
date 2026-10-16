use std::sync::Arc;

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, LikeExpr, Literal, TryCastExpr,
};

/// Extract multiple column-literal expressions from a nested OR structure.
/// Returns a vector of (column_name, expression) pairs if all leaf expressions
/// are column-literal expressions connected by OR operators.
pub(crate) fn extract_multi_column_or(
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<Vec<(&str, Arc<dyn PhysicalExpr>)>> {
    let mut result = Vec::new();

    fn collect_or_expressions<'a>(
        expr: &'a Arc<dyn PhysicalExpr>,
        result: &mut Vec<(&'a str, Arc<dyn PhysicalExpr>)>,
    ) -> bool {
        if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
            && binary.op() == &Operator::Or
        {
            // Recursively collect from left and right
            return collect_or_expressions(binary.left(), result)
                && collect_or_expressions(binary.right(), result);
        }

        // Try to extract column-literal from this expression
        if let Some(column_literal) = extract_column_literal(expr) {
            result.push(column_literal);
            true
        } else {
            false
        }
    }

    if collect_or_expressions(expr, &mut result) && result.len() >= 2 {
        Some(result)
    } else {
        None
    }
}

fn extract_column_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<(&str, Arc<dyn PhysicalExpr>)> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
        && binary.right().is::<Literal>()
    {
        return extract_column_literal(binary.left());
    } else if let Some(like_expr) = expr.downcast_ref::<LikeExpr>()
        && like_expr.pattern().is::<Literal>()
    {
        return extract_column_literal(like_expr.expr());
    } else if let Some(cast_expr) = expr.downcast_ref::<CastExpr>() {
        return extract_column_literal(cast_expr.expr());
    } else if let Some(try_cast_expr) = expr.downcast_ref::<TryCastExpr>() {
        return extract_column_literal(try_cast_expr.expr());
    } else if let Some(column) = expr.downcast_ref::<Column>() {
        return Some((column.name(), Arc::clone(expr)));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    use datafusion::physical_plan::expressions::Column;

    #[test]
    fn test_extract_multi_column_or_valid_three_columns() {
        // Test case: a = 1 OR b = 2 OR c = 3
        // This should extract 3 column-literal pairs

        let expr_a: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        let expr_b: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        ));

        let expr_c: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c", 2)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3)))),
        ));

        // Build nested OR: (a = 1 OR b = 2) OR c = 3
        let expr_ab = Arc::new(BinaryExpr::new(expr_a, Operator::Or, expr_b));
        let expr_final: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(expr_ab, Operator::Or, expr_c));

        let result = extract_multi_column_or(&expr_final);

        assert!(result.is_some());
        let column_exprs = result.unwrap();
        assert_eq!(column_exprs.len(), 3);

        // Verify we got the correct column names
        let mut column_names: Vec<&str> = column_exprs.iter().map(|(name, _)| *name).collect();
        column_names.sort();
        assert_eq!(column_names, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_extract_multi_column_or_invalid_expression() {
        // Test case: a + b = 5 (not a column-literal OR expression)
        // This should return None

        let add_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        ));

        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            add_expr,
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(5)))),
        ));

        let result = extract_multi_column_or(&expr);
        assert!(result.is_none());

        // Test case: Single column expression (a = 1)
        // This should return None because we need >= 2 columns
        let single_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        let result = extract_multi_column_or(&single_expr);
        assert!(result.is_none());

        // Test case: Mixed valid and invalid OR (a = 1 OR (b + c))
        // This should return None because one branch is not column-literal
        let valid_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));

        let invalid_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 1)),
            Operator::Plus,
            Arc::new(Column::new("c", 2)),
        ));

        let mixed_expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(valid_expr, Operator::Or, invalid_expr));

        let result = extract_multi_column_or(&mixed_expr);
        assert!(result.is_none());
    }
}
