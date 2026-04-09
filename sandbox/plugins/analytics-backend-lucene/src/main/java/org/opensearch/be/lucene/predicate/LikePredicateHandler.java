/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.predicate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.List;

/**
 * Handles LIKE predicates: {@code field LIKE 'pattern%'} →
 * {@link PrefixQueryBuilder} (trailing % only) or {@link WildcardQueryBuilder}.
 */
public class LikePredicateHandler implements PredicateHandler {

    @Override
    public SqlKind sqlKind() {
        return SqlKind.LIKE;
    }

    @Override
    public SqlOperator sqlOperator() {
        return SqlStdOperatorTable.LIKE;
    }

    @Override
    public boolean canHandle(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        if (call.getKind() != SqlKind.LIKE) {
            return false;
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (!(left instanceof RexInputRef fieldRef) || !(right instanceof RexLiteral)) {
            return false;
        }

        // Only handle VARCHAR fields (keyword, text, ip in OpenSearch)
        SqlTypeName fieldType = inputRowType.getFieldList().get(fieldRef.getIndex()).getType().getSqlTypeName();
        return fieldType == SqlTypeName.VARCHAR || fieldType == SqlTypeName.CHAR;
    }

    @Override
    public QueryBuilder convert(RexCall call, RelDataType inputRowType, MapperService mapperService) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        if (!(left instanceof RexInputRef fieldRef) || !(right instanceof RexLiteral literal)) {
            throw new IllegalArgumentException(
                "LIKE operands must be a field reference and a literal, got: "
                    + left.getClass().getSimpleName() + ", " + right.getClass().getSimpleName()
            );
        }

        String fieldName = inputRowType.getFieldList().get(fieldRef.getIndex()).getName();
        String pattern = literal.getValueAs(String.class);

        if (isTrailingPercentOnly(pattern)) {
            String prefix = pattern.substring(0, pattern.length() - 1);
            return new PrefixQueryBuilder(fieldName, prefix);
        }

        String lucenePattern = translateSqlWildcards(pattern);
        return new WildcardQueryBuilder(fieldName, lucenePattern);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> namedWriteableEntries() {
        return List.of(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, PrefixQueryBuilder.NAME, PrefixQueryBuilder::new),
            new NamedWriteableRegistry.Entry(QueryBuilder.class, WildcardQueryBuilder.NAME, WildcardQueryBuilder::new)
        );
    }

    public static boolean isTrailingPercentOnly(String pattern) {
        if (pattern.endsWith("%") == false) {
            return false;
        }
        String body = pattern.substring(0, pattern.length() - 1);
        return body.indexOf('%') < 0 && body.indexOf('_') < 0;
    }

    public static String translateSqlWildcards(String sqlPattern) {
        StringBuilder sb = new StringBuilder(sqlPattern.length());
        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);
            if (c == '%') {
                sb.append('*');
            } else if (c == '_') {
                sb.append('?');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
