/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegatedSubtreeConvertor;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.be.lucene.serializers.AbstractQuerySerializer;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;

/**
 * Converts a maximal same-backend delegated RexNode subtree into a single
 * serialized {@link BoolQueryBuilder}. Walks the subtree once, building
 * {@link QueryBuilder} objects for leaves via existing serializers, composing
 * them with {@link BoolQueryBuilder} for AND/OR/NOT, and serializes once at the root.
 *
 * @opensearch.internal
 */
final class LuceneSubtreeConvertor implements DelegatedSubtreeConvertor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneSubtreeConvertor.class);

    private final Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers;

    LuceneSubtreeConvertor(Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers) {
        this.leafSerializers = leafSerializers;
    }

    @Override
    public byte[] convertSubtree(RexNode subtree, List<FieldStorageInfo> fieldStorage) {
        QueryBuilder qb = toQueryBuilder(subtree, fieldStorage);
        return ConversionUtils.serializeQueryBuilder(qb);
    }

    private QueryBuilder toQueryBuilder(RexNode node, List<FieldStorageInfo> fieldStorage) {
        if (node instanceof AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof RexCall call) {
            switch (call.getKind()) {
                case AND: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.must(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case OR: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.should(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case NOT: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    b.mustNot(toQueryBuilder(call.getOperands().get(0), fieldStorage));
                    return b;
                }
                default:
                    return leafToQueryBuilder(call, fieldStorage);
            }
        }
        throw new IllegalStateException("Unexpected RexNode in delegated subtree: " + node);
    }

    private QueryBuilder leafToQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
        if (fn == null) {
            throw new IllegalStateException("Unrecognized operator in delegated subtree: " + call.getOperator());
        }
        DelegatedPredicateSerializer serializer = leafSerializers.get(fn);
        if (serializer == null) {
            throw new IllegalStateException("No serializer for [" + fn + "] inside delegated subtree");
        }
        QueryBuilder qb = ((AbstractQuerySerializer) serializer).buildQueryBuilder(call, fieldStorage);
        // Breadcrumb at DEBUG so a future capability flip-on can be verified end-to-end without
        // a debugger: enable `org.opensearch.be.lucene.LuceneSubtreeConvertor` at DEBUG and grep
        // logs for `[analytics.delegated]` to confirm a predicate flowed through the convertor.
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("[analytics.delegated] op=[{}] queryBuilder=[{}]", fn, qb);
        }
        return qb;
    }
}
