/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for Calcite's {@link SqlStdOperatorTable#ITEM} operator (element
 * access — {@code arr[N]}). PPL's {@code mvindex(arr, N)} single-element form
 * lowers through {@code MVIndexFunctionImp.resolveSingleElement} to ITEM with
 * a 1-based index (already converted from PPL's 0-based input).
 *
 * <p>Two transforms before substrait emission:
 *
 * <ol>
 *   <li><b>Rename to {@code array_element}.</b> DataFusion's native single-element
 *       array accessor is named {@code array_element} (also 1-based), declared
 *       in {@code opensearch_array_functions.yaml}. Calcite's ITEM operator name
 *       is {@code "ITEM"} which doesn't resolve to anything in the substrait
 *       extension catalog.
 *   <li><b>Coerce the index to {@code BIGINT}.</b> PPL's parser types positive
 *       integer literals as {@code DECIMAL(20,0)}; DataFusion's
 *       {@code array_element} signature accepts only integer indexes.
 * </ol>
 *
 * @opensearch.internal
 */
class ArrayElementAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator. Name matches DataFusion's native
     * {@code array_element}. Return-type inference is a placeholder — the
     * adapt method explicitly carries the original ITEM call's return type
     * (the element type).
     */
    static final SqlOperator LOCAL_ARRAY_ELEMENT_OP = new SqlFunction(
        "array_element",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.SYSTEM
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        List<RexNode> operands = original.getOperands();
        // Calcite's ITEM is polymorphic over arrays AND maps. PPL's `parse <field>
        // '<regex>'` lowers to ITEM(map<varchar, varchar>, '<group>') per named
        // group (see ParseAdapter); rewriting that to array_element would feed a
        // map argument to DataFusion's array accessor and crash at runtime.
        // Detect the map case via the first operand's SQL type and pass the
        // call through unchanged — DataFusionFragmentConvertor maps the
        // unrewritten ITEM operator to the "item" Substrait extension which
        // routes to the Rust map[key] UDF.
        if (!operands.isEmpty() && operands.get(0).getType().getSqlTypeName() == SqlTypeName.MAP) {
            return original;
        }
        if (operands.size() != 2) {
            return rexBuilder.makeCall(original.getType(), LOCAL_ARRAY_ELEMENT_OP, operands);
        }
        RexNode array = operands.get(0);
        RexNode index = operands.get(1);
        if (index.getType().getSqlTypeName() != SqlTypeName.BIGINT) {
            RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
            RelDataType nullableBigint = typeFactory.createTypeWithNullability(bigint, index.getType().isNullable());
            index = rexBuilder.makeCast(nullableBigint, index, true, false);
        }
        return rexBuilder.makeCall(original.getType(), LOCAL_ARRAY_ELEMENT_OP, List.of(array, index));
    }
}
