/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Serializer for the MATCHALL function.
 * Extends AbstractQuerySerializer directly (not AbstractRelevanceSerializer)
 * because MATCHALL is a zero-argument function with no MAP_VALUE_CONSTRUCTOR operands.
 */
public class MatchAllSerializer extends AbstractQuerySerializer {

    @Override
    protected QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        return new MatchAllQueryBuilder();
    }
}
