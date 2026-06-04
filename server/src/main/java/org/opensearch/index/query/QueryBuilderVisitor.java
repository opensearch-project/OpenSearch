/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.annotation.PublicApi;

/**
 * QueryBuilderVisitor is an interface to define Visitor Object to be traversed in QueryBuilder tree.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.11.0")
public interface QueryBuilderVisitor {

    /**
     * Accept method is called when the visitor accepts the queryBuilder object to be traversed in the query tree.
     * @param qb is a queryBuilder object which is accepeted by the visitor.
     */
    void accept(QueryBuilder qb);

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
        public void accept(QueryBuilder qb) {
            // Do nothing
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            return this;
        }
    };

}
