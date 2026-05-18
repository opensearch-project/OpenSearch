/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Base class for query serializers. Implements the {@link DelegatedPredicateSerializer}
 * contract by delegating to a template method that builds the {@link QueryBuilder}.
 */
public abstract class AbstractQuerySerializer implements DelegatedPredicateSerializer {

    @Override
    public final byte[] serialize(RexCall call, List<FieldStorageInfo> fieldStorage) {
        QueryBuilder queryBuilder = buildQueryBuilder(call, fieldStorage);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    protected abstract QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage);
}
