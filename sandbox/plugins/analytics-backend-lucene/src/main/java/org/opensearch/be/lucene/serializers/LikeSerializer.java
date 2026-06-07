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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.SqlLikePattern;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.List;

/**
 * Serializer for SQL/PPL {@code LIKE} / {@code ILIKE} on a column with a literal pattern. Delegated as
 * a <em>performance</em> pre-filter — DataFusion re-verifies the exact predicate, so the Lucene query
 * need only be a <b>superset</b> of true matches.
 *
 * <p>Dispatch (via {@link SqlLikePattern}): {@code 'prefix%'} → {@link PrefixQueryBuilder} (term-dict
 * prefix scan); anything else → {@link WildcardQueryBuilder} (SQL {@code %}/{@code _} → {@code *}/{@code ?}).
 * {@code ILIKE} (PPL default {@code like}) sets {@code caseInsensitive(true)}.
 *
 * <p>Negation is handled by the boolean path, not here: Calcite forbids a negated LIKE as a
 * {@code RexCall}, so {@code NOT LIKE} arrives as {@code NOT(LIKE(...))} and this only sees positive LIKE.
 */
public class LikeSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();
        if (operands.size() < 2) {
            throw new IllegalArgumentException("LIKE expects at least 2 operands, got " + operands.size());
        }
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        RexInputRef columnRef;
        RexLiteral patternLit;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            patternLit = r;
        } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            columnRef = r;
            patternLit = l;
        } else {
            throw new IllegalArgumentException("LIKE delegation requires (RexInputRef, RexLiteral); got " + left + " LIKE " + right);
        }

        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        // Route to the field's exact-match subfield when it has one (a text field's .keyword): a
        // wildcard over the raw keyword term is the valid superset, whereas a wildcard over the
        // analyzed text field is not. Marking only delegates text LIKE when this subfield exists.
        String fieldName = field.getExactMatchSubfield() != null
            ? field.getFieldName() + "." + field.getExactMatchSubfield()
            : field.getFieldName();
        Object patternValue = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(patternLit);
        if (patternValue == null) {
            throw new IllegalArgumentException("LIKE pattern must be a non-null literal");
        }
        String pattern = patternValue.toString();
        boolean caseInsensitive = operator instanceof SqlLikeOperator likeOp && !likeOp.isCaseSensitive();

        if (SqlLikePattern.classify(pattern) == SqlLikePattern.Shape.PREFIX) {
            PrefixQueryBuilder qb = new PrefixQueryBuilder(fieldName, SqlLikePattern.prefixLiteral(pattern));
            qb.caseInsensitive(caseInsensitive);
            return qb;
        }
        WildcardQueryBuilder qb = new WildcardQueryBuilder(fieldName, convertSqlWildcardToLucene(pattern));
        qb.caseInsensitive(caseInsensitive);
        return qb;
    }

    /**
     * Converts SQL wildcard characters ({@code %} and {@code _}) to Lucene wildcard characters
     * ({@code *} and {@code ?}). Escaped wildcards ({@code \%}, {@code \_}) are treated as literals,
     * and Lucene's own metacharacters ({@code *}, {@code ?}, {@code \}) in the literal text are escaped
     * so they match literally.
     */
    private static String convertSqlWildcardToLucene(String text) {
        final char ESCAPE = '\\';
        StringBuilder result = new StringBuilder(text.length());
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (escaped) {
                // Previous char was the SQL escape: this char is a literal (escape Lucene metachars).
                appendLiteral(result, c);
                escaped = false;
            } else if (c == ESCAPE) {
                escaped = true;
            } else if (c == '%') {
                result.append('*');
            } else if (c == '_') {
                result.append('?');
            } else {
                appendLiteral(result, c);
            }
        }
        if (escaped) {
            appendLiteral(result, ESCAPE); // dangling trailing backslash → literal backslash
        }
        return result.toString();
    }

    /** Append {@code c} as a literal, escaping Lucene wildcard metacharacters. */
    private static void appendLiteral(StringBuilder sb, char c) {
        if (c == '*' || c == '?' || c == '\\') {
            sb.append('\\');
        }
        sb.append(c);
    }
}
