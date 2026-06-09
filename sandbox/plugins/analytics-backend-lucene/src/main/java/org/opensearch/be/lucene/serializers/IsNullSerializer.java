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
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Serializer for SQL {@code IS NULL} / {@code IS NOT NULL} predicates. Compiles to:
 * <ul>
 *   <li>{@code IS NULL} → {@code BoolQuery.mustNot(ExistsQuery(field))}
 *   <li>{@code IS NOT NULL} → {@code ExistsQuery(field)}
 * </ul>
 *
 * <p>Expected RexCall shape: {@code IS_NULL($colIdx)} / {@code IS_NOT_NULL($colIdx)}.
 *
 * <p>{@code negated} is set per-instance at registration time (one instance for each kind),
 * keeping the leaf serializer single-purpose.
 */
public class IsNullSerializer extends AbstractQuerySerializer {

    private final boolean negated;

    public IsNullSerializer(boolean negated) {
        this.negated = negated;
    }

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 1) {
            throw new IllegalArgumentException(
                (negated ? "IS_NOT_NULL" : "IS_NULL") + " expects 1 operand, got " + call.getOperands().size()
            );
        }
        RexNode operand = call.getOperands().get(0);
        if (!(operand instanceof RexInputRef columnRef)) {
            throw new IllegalArgumentException(
                (negated ? "IS_NOT_NULL" : "IS_NULL") + " performance-delegation requires a column reference, got " + operand
            );
        }
        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        ExistsQueryBuilder exists = new ExistsQueryBuilder(fieldName);
        return negated ? exists : new BoolQueryBuilder().mustNot(exists);
    }
}
