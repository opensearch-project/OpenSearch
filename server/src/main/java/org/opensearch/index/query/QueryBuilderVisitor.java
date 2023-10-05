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
 * QueryBuilderVisitor is an interface to define Visitor Object to be traversed in QueryBuilder tree.
 */
public interface QueryBuilderVisitor {

    /**
     * Accept method is called when the visitor accepts the queryBuilder object to be traversed in the query tree.
     * @param qb is a queryBuilder object which is accepeted by the visitor.
     * @param level level of the current query builder based on the QueryBuilder tree. 0 is top/root level.
     */
    void accept(QueryBuilder qb, int level);

    /**
     * Fetches the child sub visitor from the main QueryBuilderVisitor Object.
     * @param occur defines the occurrence of the result fetched from the search query in the final search result.
     * @return a child queryBuilder Visitor Object.
     */
    QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur);

    /**
     *  NoopQueryVisitor is a default implementation of QueryBuilderVisitor.
     *  When a user does not want to implement QueryBuilderVisitor and have to just pass an empty object then this class will be used.
     *
     */
    QueryBuilderVisitor NO_OP_VISITOR = new QueryBuilderVisitor() {
        @Override
        public void accept(QueryBuilder qb, int level) {
            // Do nothing
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            return this;
        }
    };

}
