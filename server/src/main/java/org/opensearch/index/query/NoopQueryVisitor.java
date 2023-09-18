/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.BooleanClause;

/**
 *  NoopQueryVisitor is a default implementation of QueryBuilderVisitor.
 *  When a user does not want to implement QueryBuilderVisitor and have to just pass an empty object then this class will be used.
 *
 */
public class NoopQueryVisitor implements QueryBuilderVisitor {
    @Override
    public void accept(QueryBuilder qb) {
        // It is a no operation method
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return null;
    }
}
