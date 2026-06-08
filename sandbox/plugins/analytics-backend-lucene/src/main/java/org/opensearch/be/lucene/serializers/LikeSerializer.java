/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.List;

/**
 * Serializer for SQL {@code LIKE} predicates ({@code col LIKE 'pattern'}). Compiles to a Lucene
 * {@link WildcardQueryBuilder} on the field's exact-match subfield (or the field itself for
 * keywords). The SQL {@code %} → Lucene {@code *} and SQL {@code _} → Lucene {@code ?} mapping
 * mirrors {@link WildcardQuerySerializer}; backslash escapes both a SQL wildcard ({@code \%},
 * {@code \_}) and another backslash ({@code \\}).
 *
 * <p>Expected RexCall shape: {@code LIKE($colIdx, literal)}. NOT LIKE arrives as
 * {@code NOT(LIKE($colIdx, literal))} and the surrounding {@code BoolQuery.mustNot}
 * wrapping is performed by {@link org.opensearch.be.lucene.LuceneSubtreeConvertor} —
 * this serializer only handles the LIKE leaf.
 */
public class LikeSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        // Calcite's SqlStdOperatorTable.LIKE supplies an explicit escape literal — usually the
        // SQL default '\' — as a third operand even when the user didn't write an ESCAPE clause.
        // We only support the SQL default. If a custom escape arrives, fail loud so a future
        // caller doesn't get silent backslash semantics on a query that meant something else.
        int n = call.getOperands().size();
        if (n != 2 && n != 3) {
            throw new IllegalArgumentException("LIKE expects 2 or 3 operands, got " + n);
        }
        if (n == 3) {
            RexNode escapeOp = call.getOperands().get(2);
            if (!(escapeOp instanceof RexLiteral escapeLit)) {
                throw new IllegalArgumentException("LIKE escape operand must be a string literal, got " + escapeOp);
            }
            Object escapeObj = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(escapeLit);
            if (!"\\".equals(escapeObj)) {
                throw new IllegalArgumentException(
                    "LIKE with custom ESCAPE other than '\\\\' is not yet supported by Lucene delegation; got " + escapeObj
                );
            }
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (!(left instanceof RexInputRef columnRef) || !(right instanceof RexLiteral patternLit)) {
            throw new IllegalArgumentException(
                "LIKE performance-delegation requires (RexInputRef, RexLiteral); got " + left + " LIKE " + right
            );
        }
        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        Object patternObj = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(patternLit);
        if (!(patternObj instanceof String pattern)) {
            throw new IllegalArgumentException("LIKE pattern must be a string literal, got " + patternObj);
        }
        return new WildcardQueryBuilder(fieldName, sqlPatternToLucene(pattern));
    }

    /**
     * Converts SQL {@code %} → Lucene {@code *} and SQL {@code _} → Lucene {@code ?}. Backslash
     * escapes either: a SQL wildcard remains literal, another backslash becomes a literal
     * backslash. Mirrors {@link WildcardQuerySerializer#convertSqlWildcardToLucene}.
     */
    static String sqlPatternToLucene(String text) {
        final char ESCAPE = '\\';
        StringBuilder result = new StringBuilder(text.length());
        boolean escaped = false;
        for (char c : text.toCharArray()) {
            if (escaped) {
                switch (c) {
                    case '%' -> result.append('%');
                    case '_' -> result.append('_');
                    case ESCAPE -> result.append(ESCAPE);
                    default -> {
                        result.append(ESCAPE);
                        result.append(c);
                    }
                }
                escaped = false;
            } else if (c == ESCAPE) {
                escaped = true;
            } else if (c == '%') {
                result.append('*');
            } else if (c == '_') {
                result.append('?');
            } else {
                result.append(c);
            }
        }
        if (escaped) {
            result.append(ESCAPE);
        }
        return result.toString();
    }
}
