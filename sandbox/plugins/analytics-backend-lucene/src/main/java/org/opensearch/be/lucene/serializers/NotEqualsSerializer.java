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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;

/**
 * Serializer for NOT_EQUALS ({@code col != value}). Produces
 * {@code bool{ mustNot: [term(field, value)] }}.
 */
public class NotEqualsSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("NOT_EQUALS expects 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        RexInputRef columnRef;
        RexLiteral valueLit;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            valueLit = r;
        } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            columnRef = r;
            valueLit = l;
        } else {
            throw new IllegalArgumentException(
                "NOT_EQUALS performance-delegation requires (RexInputRef, RexLiteral); got " + left + " <> " + right
            );
        }

        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        String fieldName = field.getExactMatchSubfield() != null
            ? field.getFieldName() + "." + field.getExactMatchSubfield()
            : field.getFieldName();
        Object value = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(valueLit);
        return new BoolQueryBuilder().mustNot(new TermQueryBuilder(fieldName, value));
    }
}
