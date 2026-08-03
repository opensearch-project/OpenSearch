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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.util.ArrayList;
import java.util.List;

/**
 * Assembles the common scaffolding for delegated-relevance RexCalls (field resolution,
 * VARCHAR gate, MAP-operand list with field + query entries).
 */
final class DelegatedRelevanceCallHelper {

    private DelegatedRelevanceCallHelper() {}

    /**
     * Builds the RexCall for a delegated relevance query.
     *
     * @param functionName  SQL function name (e.g. "PREFIX_QUERY", "WILDCARD_QUERY_DSL")
     * @param queryTypeNoun noun for error messages (e.g. "prefix", "wildcard") — must match
     *                      the existing exception text byte-for-byte
     * @param fieldName     target field name from the DSL query
     * @param queryValue    the query/pattern value to pass verbatim
     * @param caseInsensitive whether the case_insensitive param should be added
     * @param rewrite       the rewrite method string, or null if not set
     * @param ctx           conversion context for field resolution and RexBuilder access
     */
    static RexNode buildDelegatedRelevanceCall(
        String functionName,
        String queryTypeNoun,
        String fieldName,
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

        RelDataTypeField field = ctx.getField(fieldName);

        // MappedFieldType.prefixQuery:291-297 / MappedFieldType.wildcardQuery:309-317
        // — only keyword and text fields support prefix/wildcard queries
        if (field.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Can only use "
                    + queryTypeNoun
                    + " queries on keyword and text fields - not on ["
                    + fieldName
                    + "] which is of type ["
                    + field.getType().getSqlTypeName()
                    + "]"
            );
        }

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
