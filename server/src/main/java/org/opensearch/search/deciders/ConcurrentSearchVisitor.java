/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.deciders;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;

import java.util.Objects;
import java.util.Set;

/**
 * Class to traverse the QueryBuilder tree and invoke the
 * {@link ConcurrentSearchRequestDecider#evaluateForQuery} at each node of the query tree
 */
@ExperimentalApi
public class ConcurrentSearchVisitor implements QueryBuilderVisitor {

    private final Set<ConcurrentSearchRequestDecider> deciders;
    private final IndexSettings indexSettings;

    public ConcurrentSearchVisitor(Set<ConcurrentSearchRequestDecider> concurrentSearchVisitorDeciders, IndexSettings idxSettings) {
        Objects.requireNonNull(concurrentSearchVisitorDeciders, "Concurrent search deciders cannot be null");
        deciders = concurrentSearchVisitorDeciders;
        indexSettings = idxSettings;
    }

    @Override
    public void accept(QueryBuilder qb) {
        // for each of the deciders, invoke evaluateForQuery using the current query builder and index settings.
        deciders.forEach(concurrentSearchDecider -> { concurrentSearchDecider.evaluateForQuery(qb, indexSettings); });
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return this;
    }
}
