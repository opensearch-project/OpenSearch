/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code make_struct('name0', value0, 'name1', value1, ...)} → ROW.
 *
 * <p>Reassembles an OpenSearch {@code object} from the flat dotted leaf columns a scan produces, so
 * a projection or aggregate can address the object as one value. Sub-objects nest the call:
 *
 * <pre>
 * make_struct('top', $1, 'properties', make_struct('name', $2, 'value', $3))
 * </pre>
 *
 * <p>The interleaved {@code (name, value, …)} form is what backends consume; how one lowers and
 * serializes the call is its own concern. The return type is always supplied by the caller via
 * {@link #makeCall} — operand-driven inference is a placeholder, since the authoritative ROW type
 * comes from the index mapping.
 *
 * @opensearch.internal
 */
public final class MakeStructFunction {

    /** The function name used in Calcite plans and Substrait serialization. */
    public static final String NAME = "make_struct";

    /** Singleton Calcite SqlFunction: {@code make_struct(VARCHAR, ANY, ...) → ROW}. */
    public static final SqlFunction FUNCTION = new SqlFunction(
        NAME,
        SqlKind.OTHER_FUNCTION,
        opBinding -> opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY),
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private MakeStructFunction() {}

    /**
     * Builds {@code make_struct('f0', v0, 'f1', v1, ...)} with an explicit ROW return type.
     *
     * @param rexBuilder  builder for the enclosing plan
     * @param structType  the ROW type this call produces (from the index mapping)
     * @param fieldNames  struct field names, in order
     * @param fieldValues struct field value expressions, positionally paired with {@code fieldNames}
     */
    public static RexNode makeCall(RexBuilder rexBuilder, RelDataType structType, List<String> fieldNames, List<RexNode> fieldValues) {
        if (fieldNames.size() != fieldValues.size()) {
            throw new IllegalArgumentException(
                "make_struct requires one value per field name; got " + fieldNames.size() + " names and " + fieldValues.size() + " values"
            );
        }
        // VARCHAR, not CHAR: makeLiteral(String) yields CHAR(n), whose padding semantics are wrong
        // for a field name, and backends depend on the distinction (see MakeStructCallConverter).
        RelDataType nameType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        List<RexNode> operands = new ArrayList<>(fieldNames.size() * 2);
        for (int i = 0; i < fieldNames.size(); i++) {
            operands.add(rexBuilder.makeLiteral(fieldNames.get(i), nameType, true));
            operands.add(fieldValues.get(i));
        }
        return rexBuilder.makeCall(structType, FUNCTION, operands);
    }
}
