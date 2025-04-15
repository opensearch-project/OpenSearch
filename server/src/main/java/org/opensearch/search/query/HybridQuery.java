/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.search.internal.HybridQueryContext;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Implementation of Query interface for type "hybrid". It allows execution of multiple sub-queries and collect individual
 * scores for each sub-query.
 */
public final class HybridQuery extends Query implements Iterable<Query> {

    private final List<Query> subQueries;
    private final HybridQueryContext queryContext;

    /**
     * Create new instance of hybrid query object based on collection of sub queries and filter query
     * @param subQueries collection of queries that are executed individually and contribute to a final list of combined scores
     * @param filterQueries list of filters that will be applied to each sub query. Each filter from the list is added as bool "filter" clause. If this is null sub queries will be executed as is
     */
    public HybridQuery(final Collection<Query> subQueries, final List<Query> filterQueries, final HybridQueryContext hybridQueryContext) {
        this(
            subQueries,
            hybridQueryContext,
            filterQueries == null
                ? null
                : filterQueries.stream().map(query -> new BooleanClause(query, BooleanClause.Occur.FILTER)).collect(Collectors.toList())
        );
    }

    /**
     * Create new instance of hybrid query object based on collection of sub queries and boolean clauses that are used as filters for each sub-query
     * @param subQueries
     * @param hybridQueryContext
     * @param booleanClauses
     */
    public HybridQuery(
        final Collection<Query> subQueries,
        final HybridQueryContext hybridQueryContext,
        final List<BooleanClause> booleanClauses
    ) {
        Objects.requireNonNull(subQueries, "collection of queries must not be null");
        if (subQueries.isEmpty()) {
            throw new IllegalArgumentException("collection of queries must not be empty");
        }
        Integer paginationDepth = hybridQueryContext.getPaginationDepth();
        if (Objects.nonNull(paginationDepth) && paginationDepth == 0) {
            throw new IllegalArgumentException("pagination_depth must not be zero");
        }
        if (Objects.isNull(booleanClauses) || booleanClauses.isEmpty()) {
            this.subQueries = new ArrayList<>(subQueries);
        } else {
            List<Query> modifiedSubQueries = new ArrayList<>();
            for (Query subQuery : subQueries) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(subQuery, BooleanClause.Occur.MUST);
                booleanClauses.forEach(filterQuery -> builder.add(booleanClauses));
                modifiedSubQueries.add(builder.build());
            }
            this.subQueries = modifiedSubQueries;
        }
        this.queryContext = hybridQueryContext;
    }

    public HybridQuery(final Collection<Query> subQueries, final HybridQueryContext hybridQueryContext) {
        this(subQueries, List.of(), hybridQueryContext);
    }

    /**
     * Returns an iterator over sub-queries that are parts of this hybrid query
     * @return iterator
     */
    @Override
    public Iterator<Query> iterator() {
        return getSubQueries().iterator();
    }

    /**
     * Prints a query to a string, with field assumed to be the default field and omitted.
     * @param field default field
     * @return string representation of hybrid query
     */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("(");
        Iterator<Query> it = subQueries.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Query subquery = it.next();
            if (subquery instanceof BooleanQuery) { // wrap sub-boolean in parents
                buffer.append("(");
                buffer.append(subquery.toString(field));
                buffer.append(")");
            } else {
                buffer.append(subquery.toString(field));
            }
            if (i != subQueries.size() - 1) {
                buffer.append(" | ");
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    /**
     * Re-writes queries into primitive queries. Callers are expected to call rewrite multiple times if necessary,
     * until the rewritten query is the same as the original query.
     * @param indexSearcher
     * @return
     * @throws IOException
     */
    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        if (subQueries.isEmpty()) {
            return new MatchNoDocsQuery("empty HybridQuery");
        }

        final HybridQueryRewriteCollectorManager manager = new HybridQueryRewriteCollectorManager(indexSearcher);
        final List<Callable<Void>> queryRewriteTasks = new ArrayList<>();
        final List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors = new ArrayList<>();
        for (Query subQuery : subQueries) {
            final HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector = manager.newCollector();
            collectors.add(collector);
            queryRewriteTasks.add(() -> rewriteQuery(subQuery, collector));
        }

        HybridQueryExecutor.getExecutor().invokeAll(queryRewriteTasks);

        final boolean isAnyQueryRewritten = manager.anyQueryRewrite(collectors);
        if (isAnyQueryRewritten == false) {
            return super.rewrite(indexSearcher);
        }
        final List<Query> rewrittenSubQueries = manager.getQueriesAfterRewrite(collectors);
        return new HybridQuery(rewrittenSubQueries, queryContext);
    }

    private Void rewriteQuery(Query query, HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector) {
        collector.collect(indexSearcher -> {
            try {
                Query rewrittenQuery = query.rewrite(indexSearcher);
                /* we keep rewrite sub-query unless it's not equal to itself, it may take multiple levels of recursive calls
                queries need to be rewritten from high-level clauses into lower-level clauses because low-level clauses
                perform better. For hybrid query we need to track progress of re-write for all sub-queries */

                boolean actuallyRewritten = rewrittenQuery != query;
                return new AbstractMap.SimpleEntry(rewrittenQuery, actuallyRewritten);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return null;
    }

    /**
     * Recurse through the query tree, visiting all child queries and execute provided visitor. Part of multiple
     * standard workflows, e.g. IndexSearcher.rewrite
     * @param queryVisitor a QueryVisitor to be called by each query in the tree
     */
    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        for (Query q : subQueries) {
            q.visit(v);
        }
    }

    /**
     * Override and implement query instance equivalence properly in a subclass. This is required so that QueryCache works properly.
     * @param other query object that when compare with this query object
     * @return
     */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(HybridQuery other) {
        return Objects.equals(subQueries, other.subQueries);
    }

    /**
     * Override and implement query hash code properly in a subclass. This is required so that QueryCache works properly.
     * @return hash code of this object
     */
    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Objects.hashCode(subQueries);
        return h;
    }

    public Collection<Query> getSubQueries() {
        return Collections.unmodifiableCollection(subQueries);
    }

    public HybridQueryContext getQueryContext() {
        return queryContext;
    }

    /**
     * Create the Weight used to score this query
     *
     * @param searcher
     * @param scoreMode How the produced scorers will be consumed.
     * @param boost The boost that is propagated by the parent queries.
     * @return
     * @throws IOException
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new HybridQueryWeight(this, searcher, scoreMode, boost);
    }
}
