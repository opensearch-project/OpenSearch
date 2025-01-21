/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchException;
import org.opensearch.arrow.custom.ArrowDocIdCollector;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamTicket;
import org.opensearch.search.SearchContextSourcePrinter;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.stream.StreamSearchResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * StreamSearchPhase is the search phase for streaming search.
 */
public class StreamSearchPhase extends QueryPhase {

    private static final Logger LOGGER = LogManager.getLogger(StreamSearchPhase.class);
    public static final QueryPhaseSearcher DEFAULT_QUERY_PHASE_SEARCHER = new DefaultStreamSearchPhaseSearcher();

    public StreamSearchPhase() {
        super(DEFAULT_QUERY_PHASE_SEARCHER);
    }

    @Override
    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }
        executeInternal(searchContext, this.getQueryPhaseSearcher());
        if (searchContext.getProfilers() != null) {
            ProfileShardResult shardResults = SearchProfileShardResults.buildShardResults(
                searchContext.getProfilers(),
                searchContext.request()
            );
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    /**
     * DefaultStreamSearchPhaseSearcher
     */
    public static class DefaultStreamSearchPhaseSearcher extends DefaultQueryPhaseSearcher {

        @Override
        public boolean searchWith(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }

        @Override
        public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
            return new AggregationProcessor() {
                @Override
                public void preProcess(SearchContext context) {

                }

                @Override
                public void postProcess(SearchContext context) {

                }
            };
        }

        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) {
            return searchWithCollector(searchContext, searcher, query, collectors, hasTimeout);
        }

        private boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean timeoutSet
        ) {

            QuerySearchResult queryResult = searchContext.queryResult();
            StreamManager streamManager = searchContext.streamManager();
            if (streamManager == null) {
                throw new RuntimeException("StreamManager not setup");
            }
            final boolean[] isCancelled = { false };
            StreamTicket ticket = streamManager.registerStream(new StreamProducer() {

                @Override
                public void close() {
                    isCancelled[0] = true;
                }

                @Override
                public BatchedJob createJob(BufferAllocator allocator) {
                    return new BatchedJob() {

                        @Override
                        public void run(VectorSchemaRoot root, StreamProducer.FlushSignal flushSignal) {
                            try {
                                Collector collector = QueryCollectorContext.createQueryCollector(collectors);
                                final ArrowDocIdCollector arrowDocIdCollector = new ArrowDocIdCollector(collector, root, flushSignal, 1000);
                                try {
                                    searcher.addQueryCancellation(() -> {
                                        if (isCancelled[0] == true) {
                                            throw new OpenSearchException("Stream for query results cancelled.");
                                        }
                                    });
                                    searcher.search(query, arrowDocIdCollector);
                                } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                                    // EarlyTerminationException is not caught in ContextIndexSearcher to allow force termination of
                                    // collection. Postcollection
                                    // still needs to be processed for Aggregations when early termination takes place.
                                    searchContext.bucketCollectorProcessor().processPostCollection(arrowDocIdCollector);
                                    queryResult.terminatedEarly(true);
                                }
                                if (searchContext.isSearchTimedOut()) {
                                    assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
                                    if (searchContext.request().allowPartialSearchResults() == false) {
                                        throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
                                    }
                                    queryResult.searchTimedOut(true);
                                }
                                if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER
                                    && queryResult.terminatedEarly() == null) {
                                    queryResult.terminatedEarly(false);
                                }

                                for (QueryCollectorContext ctx : collectors) {
                                    ctx.postProcess(queryResult);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void onCancel() {
                            isCancelled[0] = true;
                        }

                        @Override
                        public boolean isCancelled() {
                            return searchContext.isCancelled() || isCancelled();
                        }
                    };
                }

                @Override
                public VectorSchemaRoot createRoot(BufferAllocator allocator) {
                    IntVector docIDVector = new IntVector("docID", allocator);
                    FieldVector[] vectors = new FieldVector[] { docIDVector };
                    return new VectorSchemaRoot(Arrays.asList(vectors));
                }

                @Override
                public int estimatedRowCount() {
                    return searcher.getIndexReader().numDocs();
                }

                @Override
                public String getAction() {
                    return searchContext.getTask().getAction();
                }
            }, searchContext.getTask().getParentTaskId());
            StreamSearchResult streamSearchResult = searchContext.streamSearchResult();
            streamSearchResult.flights(List.of(new OSTicket(ticket.toBytes())));
            return false;
        }
    }
}
