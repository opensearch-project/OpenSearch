/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link RegexpQueryBuilder} to a delegated relevance RexCall.
 * The pattern is passed verbatim to Lucene — no wrapping, anchoring, or escaping.
 */
public class RegexpQueryTranslator implements QueryTranslator {

    private static final SqlFunction REGEXP_QUERY_FUNCTION = new SqlFunction(
        "REGEXP_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return RegexpQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        // Block 1: Cast
        RegexpQueryBuilder regexpQuery = (RegexpQueryBuilder) query;

        // Block 2: Reject unsupported parameters
        if (regexpQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Regexp query parameter 'boost' is not supported");
        }
        if (regexpQuery.queryName() != null) {
            throw new ConversionException("Regexp query parameter '_name' is not supported");
        }

        // Block 3: Field resolution + VARCHAR gate
        String fieldName = regexpQuery.fieldName();
        RelDataTypeField field = ctx.getField(fieldName);
        if (field.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Can only use regexp queries on keyword and text fields - not on ["
                    + fieldName
                    + "] which is of type ["
                    + field.getType().getSqlTypeName()
                    + "]"
            );
        }

        // Block 4: Build MAP operands
        RexBuilder rexBuilder = ctx.getRexBuilder();
        List<RexNode> operands = new ArrayList<>();

        operands.add(makeMapOperand(rexBuilder, "field", rexBuilder.makeInputRef(field.getType(), field.getIndex())));
        operands.add(makeMapOperand(rexBuilder, "query", rexBuilder.makeLiteral(regexpQuery.value())));

        if (regexpQuery.caseInsensitive()) {
            operands.add(makeMapLiteralOperand(rexBuilder, "case_insensitive", "true"));
        }
        if (regexpQuery.flags() != RegexpQueryBuilder.DEFAULT_FLAGS_VALUE) {
            // Carry the raw int bitmask — lossless by construction. Matches the vanilla
            // flags_value JSON field parsed by RegexpQueryBuilder.fromXContent.
            operands.add(makeMapLiteralOperand(rexBuilder, "flags", String.valueOf(regexpQuery.flags())));
        }
        if (regexpQuery.maxDeterminizedStates() != RegexpQueryBuilder.DEFAULT_DETERMINIZE_WORK_LIMIT) {
            operands.add(makeMapLiteralOperand(rexBuilder, "max_determinized_states", String.valueOf(regexpQuery.maxDeterminizedStates())));
        }
        if (regexpQuery.rewrite() != null) {
            operands.add(makeMapLiteralOperand(rexBuilder, "rewrite", regexpQuery.rewrite()));
        }

        // Block 5: Build the RexCall
        return rexBuilder.makeCall(REGEXP_QUERY_FUNCTION, operands);
    }

    private static RexNode makeMapOperand(RexBuilder rexBuilder, String key, RexNode value) {
        return rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral(key), value);
    }

    private static RexNode makeMapLiteralOperand(RexBuilder rexBuilder, String key, String value) {
        return rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral(key), rexBuilder.makeLiteral(value));
    }

}
