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
 * Visitor to traverse QueryBuilder tree and invoke IntraSegmentSearchRequestDecider
 * for each query node.
 */
@ExperimentalApi
public class IntraSegmentSearchVisitor implements QueryBuilderVisitor {

    private final Set<IntraSegmentSearchRequestDecider> deciders;
    private final IndexSettings indexSettings;

    public IntraSegmentSearchVisitor(Set<IntraSegmentSearchRequestDecider> deciders, IndexSettings indexSettings) {
        Objects.requireNonNull(deciders, "Intra-segment search deciders cannot be null");
        this.deciders = deciders;
        this.indexSettings = indexSettings;
    }

    @Override
    public void accept(QueryBuilder qb) {
        deciders.forEach(decider -> decider.evaluateForQuery(qb, indexSettings));
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return this;
    }
}
