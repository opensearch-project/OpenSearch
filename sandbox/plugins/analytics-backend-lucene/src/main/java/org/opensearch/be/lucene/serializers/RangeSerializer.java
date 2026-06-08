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
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;

import java.util.List;

/**
 * Serializer for SQL range comparisons ({@code &lt;}, {@code &lt;=}, {@code &gt;}, {@code &gt;=})
 * on a column and a literal. Compiles to a Lucene {@link RangeQueryBuilder} with the
 * inclusive/exclusive flags set per operator. The Lucene side uses point/BKD ranges on numeric
 * and date fields and term-range scans on keyword.
 *
 * <p>Expected RexCall shape: {@code op($colIdx, literal)} or {@code op(literal, $colIdx)} —
 * for the latter, the operator is mirrored ({@code 5 &lt; col} ⇔ {@code col &gt; 5}).
 */
public class RangeSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException(call.getKind() + " expects 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        SqlKind kind = call.getKind();
        RexInputRef columnRef;
        RexLiteral valueLit;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            valueLit = r;
        } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            columnRef = r;
            valueLit = l;
            kind = mirror(kind);
        } else {
            throw new IllegalArgumentException(
                "Range performance-delegation requires (RexInputRef, RexLiteral); got " + left + " " + kind + " " + right
            );
        }
        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        Object value = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(valueLit);
        RangeQueryBuilder rq = new RangeQueryBuilder(fieldName);
        switch (kind) {
            case GREATER_THAN -> rq.gt(value);
            case GREATER_THAN_OR_EQUAL -> rq.gte(value);
            case LESS_THAN -> rq.lt(value);
            case LESS_THAN_OR_EQUAL -> rq.lte(value);
            default -> throw new IllegalArgumentException("Unsupported range op: " + kind);
        }
        return rq;
    }

    private static SqlKind mirror(SqlKind k) {
        return switch (k) {
            case GREATER_THAN -> SqlKind.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> SqlKind.LESS_THAN_OR_EQUAL;
            case LESS_THAN -> SqlKind.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> SqlKind.GREATER_THAN_OR_EQUAL;
            default -> k;
        };
    }
}
