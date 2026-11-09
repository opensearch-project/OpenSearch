/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles a delegated-relevance RexCall from pre-validated inputs.
 */
final class DelegatedRelevanceCallHelper {

    private DelegatedRelevanceCallHelper() {}

    /**
     * Builds the RexCall for a delegated relevance query.
     *
     * @param field      already-resolved field; caller is responsible for type validation
     * @param queryValue passed verbatim with no escaping
     */
    static RexNode buildDelegatedRelevanceCall(
        String functionName,
        RelDataTypeField field,
        String queryValue,
        boolean caseInsensitive,
        String rewrite,
        ConversionContext ctx
    ) throws ConversionException {
        SqlFunction function = new SqlFunction(
            functionName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        RexNode fieldMap = ctx.getRexBuilder()
            .makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, ctx.getRexBuilder().makeLiteral("field"), fieldRef);
        RexNode queryMap = ctx.getRexBuilder()
            .makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                ctx.getRexBuilder().makeLiteral("query"),
                ctx.getRexBuilder().makeLiteral(queryValue)
            );

        List<RexNode> operands = new ArrayList<>(List.of(fieldMap, queryMap));

        if (caseInsensitive) {
            operands.add(
                ctx.getRexBuilder()
                    .makeCall(
                        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                        ctx.getRexBuilder().makeLiteral("case_insensitive"),
                        ctx.getRexBuilder().makeLiteral("true")
                    )
            );
        }

        // Pass rewrite through without validation — the data node validates via
        // QueryParsers.parseRewriteMethod when the query is built on the shard.
        if (rewrite != null) {
            operands.add(
                ctx.getRexBuilder()
                    .makeCall(
                        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                        ctx.getRexBuilder().makeLiteral("rewrite"),
                        ctx.getRexBuilder().makeLiteral(rewrite)
                    )
            );
        }

        return ctx.getRexBuilder().makeCall(function, operands);
    }
}
