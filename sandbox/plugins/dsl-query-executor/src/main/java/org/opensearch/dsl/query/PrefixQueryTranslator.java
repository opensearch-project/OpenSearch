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
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Converts a {@link PrefixQueryBuilder} to a Calcite LIKE expression.
 * <p>
 * Converts prefix queries to SQL LIKE patterns with a trailing wildcard:
 * <ul>
 *   <li>{@code {"prefix": {"name": "lap"}}} → {@code name LIKE 'lap%'}</li>
 *   <li>{@code {"prefix": {"name": {"value": "lap", "case_insensitive": true}}}} → {@code LOWER(name) LIKE 'lap%'}</li>
 * </ul>
 * <p>
 * <b>Supported parameters:</b>
 * <ul>
 *   <li>{@code value} - The prefix string to match</li>
 *   <li>{@code case_insensitive} - When true, applies LOWER() to both field and pattern (default: false)</li>
 * </ul>
 * <p>
 * <b>Unsupported parameters</b> (throw {@link ConversionException}):
 * <ul>
 *   <li>{@code boost} - Query boosting not supported in analytics engine</li>
 *   <li>{@code rewrite} - Lucene-specific rewrite methods not applicable to Calcite</li>
 * </ul>
 * <p>
 * <b>Special character escaping:</b>
 * SQL LIKE special characters in the prefix value are automatically escaped:
 * <ul>
 *   <li>{@code %} → {@code \%} (SQL any-characters wildcard)</li>
 *   <li>{@code _} → {@code \_} (SQL single-character wildcard)</li>
 *   <li>{@code \} → {@code \\} (escape character)</li>
 * </ul>
 * <p>
 * Example: {@code {"prefix": {"path": "C:\\test_"}}} → {@code path LIKE 'C:\\\\test\\_%'}
 */
public class PrefixQueryTranslator implements QueryTranslator {

    /**
     * Returns the query type this translator handles.
     *
     * @return {@link PrefixQueryBuilder} class
     */
    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return PrefixQueryBuilder.class;
    }

    /**
     * Converts a prefix query to a Calcite LIKE expression.
     * <p>
     * Validates field existence, checks for unsupported parameters, applies case-insensitive
     * transformation if needed, escapes SQL special characters, and appends trailing wildcard.
     *
     * @param query the prefix query to convert
     * @param ctx the conversion context with schema and RexBuilder
     * @return RexNode representing {@code field LIKE 'prefix%'} or {@code LOWER(field) LIKE 'prefix%'}
     * @throws ConversionException if field not found, or boost/rewrite parameters are set
     */
    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        PrefixQueryBuilder prefixQuery = (PrefixQueryBuilder) query;
        
        // Check for unsupported parameters
        if (prefixQuery.boost() != 1.0f) {
            throw new ConversionException("Prefix query parameter 'boost' is not supported");
        }
        if (prefixQuery.rewrite() != null) {
            throw new ConversionException("Prefix query parameter 'rewrite' is not supported");
        }

        String fieldName = prefixQuery.fieldName();
        String prefix = prefixQuery.value();
        boolean caseInsensitive = prefixQuery.caseInsensitive();

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
            prefix = prefix.toLowerCase();
        }

        // Create LIKE pattern: prefix + '%'
        String likePattern = escapeLikePattern(prefix) + "%";
        RexNode patternLiteral = ctx.getRexBuilder().makeLiteral(likePattern);

        // Return LIKE expression
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.LIKE, fieldRef, patternLiteral);
    }

    /**
     * Escapes SQL LIKE special characters in the prefix value.
     * <p>
     * Escapes characters that have special meaning in SQL LIKE patterns:
     * <ul>
     *   <li>{@code \} → {@code \\} (must be escaped first to avoid double-escaping)</li>
     *   <li>{@code %} → {@code \%} (matches any sequence of characters)</li>
     *   <li>{@code _} → {@code \_} (matches any single character)</li>
     * </ul>
     * <p>
     * Example: {@code "test_50%"} → {@code "test\_50\%"}
     *
     * @param value the prefix value to escape
     * @return escaped value safe for use in LIKE pattern
     */
    private String escapeLikePattern(String value) {
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}
