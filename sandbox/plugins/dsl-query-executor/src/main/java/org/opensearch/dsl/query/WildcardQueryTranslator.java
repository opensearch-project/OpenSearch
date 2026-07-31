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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.Locale;

/**
 * Converts a {@link WildcardQueryBuilder} to a Calcite {@code LIKE ... ESCAPE '\'} expression.
 * Translates {@code *} to {@code %} and {@code ?} to {@code _}, escaping SQL metacharacters
 * and honouring WildcardQueryBuilder backslash-escape semantics.
 */
public class WildcardQueryTranslator implements QueryTranslator {

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return WildcardQueryBuilder.class;
    }

    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        WildcardQueryBuilder wildcardQuery = (WildcardQueryBuilder) query;

        if (wildcardQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Wildcard query parameter 'boost' is not supported");
        }
        if (wildcardQuery.rewrite() != null) {
            throw new ConversionException("Wildcard query parameter 'rewrite' is not supported");
        }

        String fieldName = wildcardQuery.fieldName();
        String pattern = wildcardQuery.value();
        boolean caseInsensitive = wildcardQuery.caseInsensitive();

        RelDataTypeField field = ctx.getField(fieldName);

        // MappedFieldType.wildcardQuery:309-317 — only keyword and text fields support wildcard queries
        if (field.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
            throw new ConversionException(
                "Can only use wildcard queries on keyword and text fields - not on ["
                    + fieldName
                    + "] which is of type ["
                    + field.getType().getSqlTypeName()
                    + "]"
            );
        }

        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        if (caseInsensitive) {
            fieldRef = ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LOWER, fieldRef);
            pattern = pattern.toLowerCase(Locale.ROOT);
        }

        String likePattern = convertWildcardToLike(pattern);
        RexNode patternLiteral = ctx.getRexBuilder().makeLiteral(likePattern);

        // ESCAPE operand is required so Calcite knows '\' is the escape character in the pattern
        RexNode escapeChar = ctx.getRexBuilder().makeLiteral("\\");

        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LIKE, fieldRef, patternLiteral, escapeChar);
    }

    /**
     * Converts an OpenSearch wildcard pattern to a SQL LIKE pattern.
     * <p>
     * Escape-conversion contract (backslash is the OpenSearch escape character):
     * <pre>
     *   Input        LIKE output   Reason
     *   ─────────    ───────────   ──────
     *   *            %             wildcard → SQL any-chars
     *   ?            _             wildcard → SQL single-char
     *   \*           *             escaped wildcard → literal star
     *   \?           ?             escaped wildcard → literal question
     *   \\           \\            escaped backslash → LIKE-escaped literal backslash
     *   \{other}     {other}       escape consumed, matches Lucene WildcardQuery.toAutomaton
     *   trailing \   \\            lone trailing backslash → LIKE-escaped literal backslash
     *   literal %    \%            SQL metachar must be escaped in LIKE
     *   literal _    \_            SQL metachar must be escaped in LIKE
     * </pre>
     * <p>
     * Note: if the char following the consumed escape is itself a SQL LIKE metacharacter
     * ({@code %} or {@code _}), it is additionally LIKE-escaped to remain literal.
     */
    private String convertWildcardToLike(String wildcardPattern) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < wildcardPattern.length(); i++) {
            char c = wildcardPattern.charAt(i);
            switch (c) {
                case '\\':
                    if (i + 1 < wildcardPattern.length()) {
                        char next = wildcardPattern.charAt(i + 1);
                        if (next == '*' || next == '?') {
                            result.append(next);
                        } else if (next == '\\') {
                            result.append("\\\\");
                        } else {
                            // Lucene WildcardQuery.toAutomaton (L99-108): escape consumed, emit next char
                            if (next == '%' || next == '_') {
                                result.append('\\');
                            }
                            result.append(next);
                        }
                        i++; // consume the next character
                    } else {
                        // Trailing lone backslash treated as literal
                        result.append("\\\\");
                    }
                    break;
                case '%':
                    result.append("\\%");
                    break;
                case '_':
                    result.append("\\_");
                    break;
                case '*':
                    result.append('%');
                    break;
                case '?':
                    result.append('_');
                    break;
                default:
                    result.append(c);
            }
        }
        return result.toString();
    }
}
