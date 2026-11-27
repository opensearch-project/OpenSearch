/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.common.lucene.search.function.Functions;

import java.io.IOException;
import java.util.Objects;

/**
 * A transparent query wrapper that includes a query name in its explanation.
 * 23046
 * @opensearch.internal
 */
public final class NamedQuery extends Query {

    private final Query query;
    private final String queryName;

    public NamedQuery(Query query, String queryName) {
        this.query = Objects.requireNonNull(query);
        this.queryName = Objects.requireNonNull(queryName);
    }

    public Query getQuery() {
        return query;
    }

    public String getQueryName() {
        return queryName;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewritten = query.rewrite(searcher);
        if (rewritten != query) {
            return new NamedQuery(rewritten, queryName);
        }
        return super.rewrite(searcher);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight weight = query.createWeight(searcher, scoreMode, boost);
        return new FilterWeight(this, weight) {
            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return Functions.explainWithName(in.explain(context, doc), queryName);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        query.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public String toString(String field) {
        return query.toString(field);
    }

    @Override
    public boolean equals(Object object) {
        if (sameClassAs(object) == false) {
            return false;
        }
        NamedQuery other = (NamedQuery) object;
        return query.equals(other.query) && queryName.equals(other.queryName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), query, queryName);
    }
}
