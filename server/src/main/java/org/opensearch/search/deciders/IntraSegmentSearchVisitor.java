/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.deciders;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;

/**
 * Visitor to traverse QueryBuilder tree and invoke IntraSegmentSearchDecider
 * for each query node.
 */
public class IntraSegmentSearchVisitor implements QueryBuilderVisitor {

    private final IntraSegmentSearchDecider decider;
    private final IndexSettings indexSettings;

    public IntraSegmentSearchVisitor(IntraSegmentSearchDecider decider, IndexSettings indexSettings) {
        this.decider = decider;
        this.indexSettings = indexSettings;
    }

    @Override
    public void accept(QueryBuilder qb) {
        decider.evaluateForQuery(qb, indexSettings);
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return this;
    }
}
