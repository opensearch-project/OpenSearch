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
     * Locally-declared target operator for the array-input case. Name matches
     * DataFusion's native {@code array_element}. Return-type inference is a
     * placeholder — the adapt method explicitly carries the original ITEM
     * call's return type (the element type).
     */
    static final SqlOperator LOCAL_ARRAY_ELEMENT_OP = new SqlFunction(
        "array_element",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.SYSTEM
    );

    /**
     * Locally-declared target operator for the map-input case. PPL `spath`'s
     * auto-extract mode lowers `result.user.name` to
     * {@code ITEM(JSON_EXTRACT_ALL($n), 'user.name')} where the container is a
     * {@code MAP<VARCHAR, VARCHAR>}. DataFusion's array_element only handles
     * List/Array; map keys must dispatch to {@code map_extract}. Same
     * placeholder return-type approach as {@link #LOCAL_ARRAY_ELEMENT_OP}.
     */
    static final SqlOperator LOCAL_MAP_EXTRACT_OP = new SqlFunction(
        "map_extract",
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
        if (operands.size() != 2) {
            return rexBuilder.makeCall(original.getType(), LOCAL_ARRAY_ELEMENT_OP, operands);
        }
        RexNode container = operands.get(0);
        RexNode key = operands.get(1);
        // Map dispatch — PPL spath's JSON_EXTRACT_ALL returns MAP, so
        // `result.user.name` lowers to `ITEM(map, 'user.name')` with a string
        // key. DataFusion's `map_extract(map, key)` returns `List<value>`
        // because maps in some semantics permit duplicate keys; wrap the call
        // in `array_element(..., 1)` to project the first (and, for our
        // unique-key maps, only) element back to a scalar that matches the
        // PPL ITEM call's declared return type (the map's value type).
        if (container.getType().getSqlTypeName() == SqlTypeName.MAP) {
            RelDataType keyType = container.getType().getKeyType();
            RelDataType valueType = container.getType().getValueType();
            // Coerce the lookup key to the map's declared key type. Calcite
            // produces CHAR(N) literals for `result.user.name`-style accesses,
            // which doesn't unify with the map's VARCHAR key type variable
            // (`any1`) inside Substrait's extension catalog. An explicit cast
            // collapses CHAR(N) → VARCHAR before isthmus emits the call.
            if (!key.getType().equals(keyType)) {
                RelDataType nullableKeyType = typeFactory.createTypeWithNullability(keyType, key.getType().isNullable());
                key = rexBuilder.makeCast(nullableKeyType, key, true, false);
            }
            RelDataType nullableValueType = typeFactory.createTypeWithNullability(valueType, true);
            RelDataType listOfValue = typeFactory.createArrayType(nullableValueType, -1);
            RexNode mapResult = rexBuilder.makeCall(listOfValue, LOCAL_MAP_EXTRACT_OP, List.of(container, key));
            RexNode one = rexBuilder.makeLiteral(java.math.BigDecimal.ONE, typeFactory.createSqlType(SqlTypeName.BIGINT), false);
            return rexBuilder.makeCall(original.getType(), LOCAL_ARRAY_ELEMENT_OP, List.of(mapResult, one));
        }
        // Array dispatch — coerce the index to BIGINT as before.
        if (key.getType().getSqlTypeName() != SqlTypeName.BIGINT) {
            RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
            RelDataType nullableBigint = typeFactory.createTypeWithNullability(bigint, key.getType().isNullable());
            key = rexBuilder.makeCast(nullableBigint, key, true, false);
        }
        return rexBuilder.makeCall(original.getType(), LOCAL_ARRAY_ELEMENT_OP, List.of(container, key));
    }
}
