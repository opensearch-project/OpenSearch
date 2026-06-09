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
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Serializer for IS_NULL and IS_NOT_NULL.
 * <ul>
 *   <li>IS_NOT_NULL → {@code ExistsQuery(field)}</li>
 *   <li>IS_NULL → {@code bool{ mustNot: ExistsQuery(field) }}</li>
 * </ul>
 */
public class IsNullSerializer extends AbstractQuerySerializer {

    private final boolean negated;

    public IsNullSerializer(boolean negated) {
        this.negated = negated;
    }

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 1 || !(call.getOperands().get(0) instanceof RexInputRef columnRef)) {
            throw new IllegalArgumentException("IS_NULL/IS_NOT_NULL expects a single column reference");
        }

        FieldStorageInfo field = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex());
        String fieldName = field.getExactMatchSubfield() != null
            ? field.getFieldName() + "." + field.getExactMatchSubfield()
            : field.getFieldName();

        if (negated) {
            return new ExistsQueryBuilder(fieldName);
        }
        return new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(fieldName));
    }
}
