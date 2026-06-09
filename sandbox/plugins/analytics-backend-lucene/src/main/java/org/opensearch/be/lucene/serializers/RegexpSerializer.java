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
import org.opensearch.index.query.RegexpQueryBuilder;

import java.util.List;

/**
 * Serializer for the {@code REGEXP(col, 'pattern')} predicate. Compiles to a Lucene
 * {@link RegexpQueryBuilder} on the field's exact-match subfield (or the field itself for
 * keyword). Lucene's regexp engine on the term dictionary is much cheaper for anchored or
 * sub-expression patterns than scanning every doc value through a Java {@code Pattern}.
 *
 * <p>Expected RexCall shape: {@code REGEXP($colIdx, 'pattern')}.
 */
public class RegexpSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("REGEXP expects 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        if (!(left instanceof RexInputRef columnRef) || !(right instanceof RexLiteral patternLit)) {
            throw new IllegalArgumentException(
                "REGEXP performance-delegation requires (RexInputRef, RexLiteral); got " + left + ", " + right
            );
        }
        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        Object patternObj = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(patternLit);
        if (!(patternObj instanceof String pattern)) {
            throw new IllegalArgumentException("REGEXP pattern must be a string literal, got " + patternObj);
        }
        return new RegexpQueryBuilder(fieldName, pattern);
    }
}
