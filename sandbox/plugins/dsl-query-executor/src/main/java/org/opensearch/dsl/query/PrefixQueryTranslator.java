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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.Locale;

/**
 * Converts a {@link PrefixQueryBuilder} to a Calcite {@code LIKE 'prefix%' ESCAPE '\'} expression.
 * Case-insensitive mode wraps the field and pattern in {@code LOWER()}.
 */
public class PrefixQueryTranslator implements QueryTranslator {

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

        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        if (caseInsensitive) {
            fieldRef = ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LOWER, fieldRef);
            prefix = prefix.toLowerCase(Locale.ROOT);
        }

        String likePattern = escapeLikePattern(prefix) + "%";
        RexNode patternLiteral = ctx.getRexBuilder().makeLiteral(likePattern);

        // ESCAPE operand is required so Calcite knows '\' is the escape character in the pattern
        RexNode escapeChar = ctx.getRexBuilder().makeLiteral("\\");

        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LIKE, fieldRef, patternLiteral, escapeChar);
    }

    /**
     * Escapes SQL LIKE metacharacters ({@code %}, {@code _}, {@code \}) in the prefix value.
     * Backslash is escaped first to avoid double-escaping.
     */
    private String escapeLikePattern(String value) {
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
