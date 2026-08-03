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
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link PrefixQueryBuilder} to a PREFIX_QUERY RexCall that delegates to Lucene
 * via the analytics backend serializer. The prefix value is passed verbatim — no SQL LIKE
 * escaping or pattern construction occurs.
 */
public class PrefixQueryTranslator implements QueryTranslator {

    private static final SqlFunction PREFIX_QUERY_FUNCTION = new SqlFunction(
        "PREFIX_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return PrefixQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        PrefixQueryBuilder prefixQuery = (PrefixQueryBuilder) query;

        if (prefixQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Prefix query parameter 'boost' is not supported");
        }
        if (prefixQuery.rewrite() != null) {
            throw new ConversionException("Prefix query parameter 'rewrite' is not supported");
        }

        String fieldName = prefixQuery.fieldName();
        String prefix = prefixQuery.value();
        boolean caseInsensitive = prefixQuery.caseInsensitive();

        RelDataTypeField field = ctx.getField(fieldName);

        // MappedFieldType.prefixQuery:291-297 — only keyword and text fields support prefix queries
        if (field.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Can only use prefix queries on keyword and text fields - not on ["
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
                ctx.getRexBuilder().makeLiteral(prefix)
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

        return ctx.getRexBuilder().makeCall(PREFIX_QUERY_FUNCTION, operands);
    }
}
