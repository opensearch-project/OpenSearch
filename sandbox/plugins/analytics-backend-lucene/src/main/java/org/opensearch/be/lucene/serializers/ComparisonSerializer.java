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
 * Serializer for bare comparison predicates ({@code >}, {@code >=}, {@code <}, {@code <=}).
 * Produces a single-bound {@link RangeQueryBuilder}.
 */
public class ComparisonSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("Comparison expects 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        RexInputRef columnRef;
        RexLiteral valueLit;
        boolean reversed;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            valueLit = r;
            reversed = false;
        } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            columnRef = r;
            valueLit = l;
            reversed = true;
        } else {
            throw new IllegalArgumentException(
                "Comparison performance-delegation requires (RexInputRef, RexLiteral); got " + left + " op " + right
            );
        }

        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        String fieldName = field.getExactMatchSubfield() != null
            ? field.getFieldName() + "." + field.getExactMatchSubfield()
            : field.getFieldName();
        Object value = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(valueLit);

        RangeQueryBuilder range = new RangeQueryBuilder(fieldName);
        SqlKind kind = reversed ? flipKind(call.getKind()) : call.getKind();
        switch (kind) {
            case GREATER_THAN -> range.gt(value);
            case GREATER_THAN_OR_EQUAL -> range.gte(value);
            case LESS_THAN -> range.lt(value);
            case LESS_THAN_OR_EQUAL -> range.lte(value);
            default -> throw new IllegalArgumentException("Unsupported comparison kind: " + kind);
        }
        return range;
    }

    private static SqlKind flipKind(SqlKind kind) {
        return switch (kind) {
            case GREATER_THAN -> SqlKind.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> SqlKind.LESS_THAN_OR_EQUAL;
            case LESS_THAN -> SqlKind.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> SqlKind.GREATER_THAN_OR_EQUAL;
            default -> kind;
        };
    }
}
