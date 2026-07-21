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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.Locale;

/**
 * Converts a {@link WildcardQueryBuilder} to a Calcite LIKE expression.
 * <p>
 * Translates OpenSearch wildcard patterns to SQL LIKE patterns:
 * <ul>
 *   <li>{@code *} (matches any character sequence) → {@code %} (SQL any-characters wildcard)</li>
 *   <li>{@code ?} (matches any single character) → {@code _} (SQL single-character wildcard)</li>
 * </ul>
 * <p>
 * <b>Examples:</b>
 * <ul>
 *   <li>{@code {"wildcard": {"name": "lap*"}}} → {@code name LIKE 'lap%'}</li>
 *   <li>{@code {"wildcard": {"name": "l?ptop"}}} → {@code name LIKE 'l_ptop'}</li>
 *   <li>{@code {"wildcard": {"name": "*book*"}}} → {@code name LIKE '%book%'}</li>
 *   <li>{@code {"wildcard": {"name": {"value": "LAP*", "case_insensitive": true}}}} → {@code LOWER(name) LIKE 'lap%'}</li>
 * </ul>
 * <p>
 * <b>Supported parameters:</b>
 * <ul>
 *   <li>{@code value} - The wildcard pattern with {@code *} and {@code ?} characters</li>
 *   <li>{@code case_insensitive} - When true, applies LOWER() to both field and pattern (default: false)</li>
 * </ul>
 * <p>
 * <b>Unsupported parameters</b> (throw {@link ConversionException}):
 * <ul>
 *   <li>{@code boost} - Query boosting not supported in analytics engine</li>
 *   <li>{@code rewrite} - Lucene-specific rewrite methods not applicable to Calcite</li>
 * </ul>
 * <p>
 * <b>Special character handling:</b>
 * SQL LIKE special characters ({@code %}, {@code _}, {@code \}) in the pattern are escaped
 * before wildcard conversion to prevent unintended matching.
 * <p>
 * Example: {@code {"wildcard": {"name": "a%b_c\\d*"}}} → {@code name LIKE 'a\%b\_c\\\\d%'}
 */
public class WildcardQueryTranslator implements QueryTranslator {

    /**
     * Returns the query type this translator handles.
     *
     * @return {@link WildcardQueryBuilder} class
     */
    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return WildcardQueryBuilder.class;
    }

    /**
     * Converts a wildcard query to a Calcite LIKE expression.
     * <p>
     * Validates field existence, checks for unsupported parameters, applies case-insensitive
     * transformation if needed, and converts wildcard pattern to SQL LIKE pattern.
     *
     * @param query the wildcard query to convert
     * @param ctx the conversion context with schema and RexBuilder
     * @return RexNode representing {@code field LIKE 'pattern'} or {@code LOWER(field) LIKE 'pattern'}
     * @throws ConversionException if field not found, or boost/rewrite parameters are set
     */
    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        WildcardQueryBuilder wildcardQuery = (WildcardQueryBuilder) query;

        // Check for unsupported parameters
        if (wildcardQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Wildcard query parameter 'boost' is not supported");
        }
        if (wildcardQuery.rewrite() != null) {
            throw new ConversionException("Wildcard query parameter 'rewrite' is not supported");
        }

        String fieldName = wildcardQuery.fieldName();
        String pattern = wildcardQuery.value();
        boolean caseInsensitive = wildcardQuery.caseInsensitive();

        // Validate field exists in schema
        RelDataTypeField field = ctx.getRowType().getField(fieldName, false, false);
        if (field == null) {
            throw new ConversionException("Field '" + fieldName + "' not found in schema");
        }

        // Create field reference
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());

        // Apply LOWER() if case insensitive
        if (caseInsensitive) {
            fieldRef = ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LOWER, fieldRef);
            pattern = pattern.toLowerCase(Locale.ROOT);
        }

        // Convert wildcard pattern to LIKE pattern
        String likePattern = convertWildcardToLike(pattern);
        RexNode patternLiteral = ctx.getRexBuilder().makeLiteral(likePattern);

        // Create escape character literal for explicit ESCAPE clause
        RexNode escapeChar = ctx.getRexBuilder().makeLiteral("\\");

        // Return LIKE expression with explicit ESCAPE clause
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LIKE, fieldRef, patternLiteral, escapeChar);
    }

    /**
     * Converts OpenSearch wildcard pattern to SQL LIKE pattern.
     * <p>
     * OpenSearch wildcard syntax uses backslash as an escape character:
     * <ul>
     *   <li>{@code \*} → literal {@code *} (not a wildcard)</li>
     *   <li>{@code \?} → literal {@code ?} (not a wildcard)</li>
     *   <li>{@code \\} → literal backslash, escaped as {@code \\} in LIKE</li>
     *   <li>A trailing lone {@code \} with no following character is treated as a literal backslash</li>
     * </ul>
     * <p>
     * Unescaped wildcards are converted to SQL LIKE equivalents:
     * <ul>
     *   <li>{@code *} → {@code %} (matches any sequence of characters)</li>
     *   <li>{@code ?} → {@code _} (matches any single character)</li>
     * </ul>
     * <p>
     * SQL LIKE metacharacters ({@code %}, {@code _}) appearing as literal data are escaped
     * to prevent unintended matching.
     *
     * @param wildcardPattern the OpenSearch wildcard pattern with {@code *} and {@code ?}
     * @return SQL LIKE pattern with {@code %} and {@code _}
     */
    private String convertWildcardToLike(String wildcardPattern) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < wildcardPattern.length(); i++) {
            char c = wildcardPattern.charAt(i);
            switch (c) {
                case '\\':
                    // Lookahead: backslash escapes the next character
                    if (i + 1 < wildcardPattern.length()) {
                        char next = wildcardPattern.charAt(i + 1);
                        if (next == '*' || next == '?') {
                            // \* or \? → emit the literal character (no LIKE metachar meaning)
                            result.append(next);
                        } else if (next == '\\') {
                            // \\ → literal backslash, escaped for LIKE
                            result.append("\\\\");
                        } else {
                            // \<other> → literal backslash + character
                            // The backslash is itself a LIKE escape char, so emit \\ for the literal backslash
                            // then escape the character if it's a SQL LIKE metacharacter
                            if (next == '%' || next == '_') {
                                result.append("\\\\");
                                result.append('\\');
                            } else {
                                result.append("\\\\");
                            }
                            result.append(next);
                        }
                        i++; // consume the next character
                    } else {
                        // Trailing lone backslash: treat as literal backslash
                        result.append("\\\\");
                    }
                    break;
                case '%':
                    // Escape SQL LIKE metacharacter
                    result.append("\\%");
                    break;
                case '_':
                    // Escape SQL LIKE metacharacter
                    result.append("\\_");
                    break;
                case '*':
                    // Convert to SQL any-characters wildcard
                    result.append('%');
                    break;
                case '?':
                    // Convert to SQL single-character wildcard
                    result.append('_');
                    break;
                default:
                    result.append(c);
            }
        }
        return result.toString();
    }
}
