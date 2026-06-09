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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.List;

/**
 * Serializer for LIKE ({@code col LIKE pattern}). Converts the SQL LIKE pattern
 * (% = any chars, _ = one char) to a Lucene WildcardQuery pattern (* and ?).
 */
public class LikeSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() < 2) {
            throw new IllegalArgumentException("LIKE expects at least 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        RexInputRef columnRef;
        RexLiteral patternLit;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            patternLit = r;
        } else {
            throw new IllegalArgumentException(
                "LIKE performance-delegation requires (RexInputRef, RexLiteral); got " + left + " LIKE " + right
            );
        }

        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        String fieldName = resolveFieldName(field);
        String sqlPattern = patternLit.getValueAs(String.class);
        String lucenePattern = convertSqlLikeToLuceneWildcard(sqlPattern);
        return new WildcardQueryBuilder(fieldName, lucenePattern).caseInsensitive(true);
    }

    static String convertSqlLikeToLuceneWildcard(String pattern) {
        final char ESCAPE = '\\';
        StringBuilder result = new StringBuilder(pattern.length());
        boolean escaped = false;

        for (char c : pattern.toCharArray()) {
            if (escaped) {
                switch (c) {
                    case '%' -> result.append('%');
                    case '_' -> result.append('_');
                    case ESCAPE -> result.append(ESCAPE);
                    default -> { result.append(ESCAPE); result.append(c); }
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
