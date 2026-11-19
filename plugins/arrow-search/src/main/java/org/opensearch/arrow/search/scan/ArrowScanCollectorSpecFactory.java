/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QueryCollectorArguments;
import org.opensearch.search.query.QueryCollectorContextSpec;
import org.opensearch.search.query.QueryCollectorContextSpecFactory;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Replaces the top-docs collector of a shard scan's query phase with {@link ArrowScanCollector}s. Only the data node that
 * runs the scan marks the phase, with an {@link ArrowScanExt}. Segments collected one after another share one collector;
 * under concurrent segment search, each slice has its own.
 */
public final class ArrowScanCollectorSpecFactory implements QueryCollectorContextSpecFactory {

    /** Creates the factory. */
    public ArrowScanCollectorSpecFactory() {}

    @Override
    public Optional<QueryCollectorContextSpec> createQueryCollectorContextSpec(
        SearchContext searchContext,
        Query query,
        QueryCollectorArguments queryCollectorArguments
    ) {
        if (searchContext.getSearchExt(ArrowScanExt.NAME) instanceof ArrowScanExt ext) {
            // The query phase runs without a timeout because it waits for the coordinator. This check enforces each slice's
            // compute budget while the searcher scores.
            final Runnable budgetCheck = searchContext.searcher().addQueryCancellation(ArrowScanCollector.budgetCheck(ext.state()));
            return Optional.of(new Spec(ext.state(), searchContext, () -> searchContext.searcher().removeQueryCancellation(budgetCheck)));
        }
        return Optional.empty();
    }

    private static final class Spec implements QueryCollectorContextSpec {
        private final ScanState state;
        private final SearchContext searchContext;
        private final Runnable removeBudgetCheck;
        /** The collector of the sequential path, the only path that calls {@link #postProcess}. */
        private ArrowScanCollector collector;

        Spec(ScanState state, SearchContext searchContext, Runnable removeBudgetCheck) {
            this.state = state;
            this.searchContext = searchContext;
            this.removeBudgetCheck = removeBudgetCheck;
        }

        @Override
        public String getContextName() {
            return ArrowScanExt.NAME;
        }

        @Override
        public Collector create(Collector in) {
            collector = new ArrowScanCollector(state, searchContext.getQueryShardContext());
            return collector;
        }

        @Override
        public CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) {
            return new CollectorManager<ArrowScanCollector, ReduceableSearchResult>() {
                @Override
                public ArrowScanCollector newCollector() {
                    return new ArrowScanCollector(state, searchContext.getQueryShardContext());
                }

                @Override
                public ReduceableSearchResult reduce(Collection<ArrowScanCollector> collectors) {
                    final long rows = complete(collectors);
                    return result -> setTotalHits(result, rows);
                }
            };
        }

        @Override
        public void postProcess(QuerySearchResult result) {
            setTotalHits(result, complete(List.of(collector)));
        }

        /**
         * Ends the slices once collection is over.
         *
         * @return the rows the shard streams: those sent and those the slices still hold
         */
        private long complete(Collection<ArrowScanCollector> collectors) {
            state.collected = true;
            removeBudgetCheck.run();
            long unsent = 0;
            for (ArrowScanCollector slice : collectors) {
                slice.finish();
                unsent += slice.rowsUnsent();
            }
            return state.rowsSent.get() + unsent;
        }

        private static void setTotalHits(QuerySearchResult result, long rows) {
            result.topDocs(
                new TopDocsAndMaxScore(new TopDocs(new TotalHits(rows, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                new DocValueFormat[0]
            );
        }
    }
}
