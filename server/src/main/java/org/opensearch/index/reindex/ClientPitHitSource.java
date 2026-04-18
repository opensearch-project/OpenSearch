/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.ShardDocSortBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.ParentTaskAssigningClient;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.core.common.util.CollectionUtils.isEmpty;

/**
 * A scrollable source of hits using Point in Time (PIT) + search_after instead of scroll API.
 * <p>
 * PIT provides a lightweight, consistent view of the data at a point in time without holding
 * heavyweight scroll contexts that block segment merges. Combined with search_after, this
 * provides stateless pagination that is resumable on failure.
 *
 * @opensearch.internal
 */
public class ClientPitHitSource extends ScrollableHitSource {
    private final ParentTaskAssigningClient client;
    private final SearchRequest firstSearchRequest;
    private final TimeValue keepAlive;
    private volatile String pitId;
    private volatile Object[] lastSortValues;

    public ClientPitHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail,
        ParentTaskAssigningClient client,
        SearchRequest firstSearchRequest
    ) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, onResponse, fail);
        this.client = client;
        this.firstSearchRequest = firstSearchRequest;
        this.keepAlive = firstSearchRequest.scroll() != null
            ? firstSearchRequest.scroll().keepAlive()
            : TimeValue.timeValueMinutes(5);
        firstSearchRequest.allowPartialSearchResults(false);
    }

    @Override
    public void doStart(RejectAwareActionListener<Response> searchListener) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "creating PIT for reindex against {}",
                isEmpty(firstSearchRequest.indices()) ? "all indices" : firstSearchRequest.indices()
            );
        }
        // Step 1: Create PIT
        CreatePitRequest createPitRequest = new CreatePitRequest(keepAlive, false, firstSearchRequest.indices());
        client.execute(CreatePitAction.INSTANCE, createPitRequest, new ActionListener<CreatePitResponse>() {
            @Override
            public void onResponse(CreatePitResponse createPitResponse) {
                pitId = createPitResponse.getId();
                // Step 2: Execute first search with PIT (no search_after for first request)
                executeSearch(searchListener);
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrap(e, OpenSearchRejectedExecutionException.class) != null) {
                    searchListener.onRejection(e);
                } else {
                    searchListener.onFailure(e);
                }
            }
        });
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        // For PIT, scrollId is actually the pitId (we reuse the field from parent class for compatibility)
        // extraKeepAlive extends the PIT keep_alive
        executeSearch(searchListener);
    }

    private void executeSearch(RejectAwareActionListener<Response> searchListener) {
        // Build a search request with PIT + search_after
        SearchRequest searchRequest = buildPitSearchRequest();
        client.search(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                // Update PIT ID (may change between requests)
                if (searchResponse.pointInTimeId() != null) {
                    pitId = searchResponse.pointInTimeId();
                }
                // Track last sort values for search_after
                SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits != null && hits.length > 0) {
                    lastSortValues = hits[hits.length - 1].getSortValues();
                }
                searchListener.onResponse(wrapSearchResponse(searchResponse));
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrap(e, OpenSearchRejectedExecutionException.class) != null) {
                    searchListener.onRejection(e);
                } else {
                    searchListener.onFailure(e);
                }
            }
        });
    }

    private SearchRequest buildPitSearchRequest() {
        // Build a fresh search request for PIT — don't mutate the original
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);

        // Copy the source builder with a new sorts list to avoid mutating the original
        SearchSourceBuilder newSource = firstSearchRequest.source().shallowCopy();
        searchRequest.source(newSource);

        // Set PIT with keep_alive
        PointInTimeBuilder pitBuilder = new PointInTimeBuilder(pitId);
        pitBuilder.setKeepAlive(keepAlive);
        newSource.pointInTimeBuilder(pitBuilder);

        // Replace sorts with _shard_doc for deterministic PIT ordering
        newSource.sort(new ShardDocSortBuilder());

        // Set search_after if we have previous sort values
        if (lastSortValues != null) {
            newSource.searchAfter(lastSortValues);
        }

        return searchRequest;
    }

    @Override
    public void clearScroll(String scrollId, Runnable onCompletion) {
        // Delete the PIT instead of clearing scroll
        if (pitId != null) {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            client.unwrap().execute(DeletePitAction.INSTANCE, deletePitRequest, new ActionListener<DeletePitResponse>() {
                @Override
                public void onResponse(DeletePitResponse response) {
                    logger.debug("Deleted PIT [{}]", pitId);
                    onCompletion.run();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(() -> new ParameterizedMessage("Failed to delete PIT [{}]", pitId), e);
                    onCompletion.run();
                }
            });
        } else {
            onCompletion.run();
        }
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        onCompletion.run();
    }

    /**
     * Returns the current PIT ID. Useful for resumability — callers can save this
     * along with lastSortValues to resume from where they left off.
     */
    public String getPitId() {
        return pitId;
    }

    /**
     * Returns the last sort values used for search_after pagination.
     */
    public Object[] getLastSortValues() {
        return lastSortValues;
    }

    private Response wrapSearchResponse(SearchResponse response) {
        List<SearchFailure> failures;
        if (response.getShardFailures() == null) {
            failures = emptyList();
        } else {
            failures = new ArrayList<>(response.getShardFailures().length);
            for (ShardSearchFailure failure : response.getShardFailures()) {
                String nodeId = failure.shard() == null ? null : failure.shard().getNodeId();
                failures.add(new SearchFailure(failure.getCause(), failure.index(), failure.shardId(), nodeId));
            }
        }
        List<Hit> hits;
        if (response.getHits().getHits() == null || response.getHits().getHits().length == 0) {
            hits = emptyList();
        } else {
            hits = new ArrayList<>(response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                hits.add(new ClientPitHit(hit));
            }
            hits = unmodifiableList(hits);
        }
        long total = response.getHits().getTotalHits().value();
        // Use pitId as the "scrollId" for compatibility with the parent class
        return new Response(response.isTimedOut(), failures, total, hits, pitId);
    }

    /**
     * A hit from PIT-based search.
     */
    private static class ClientPitHit implements Hit {
        private final SearchHit delegate;
        private final BytesReference source;

        ClientPitHit(SearchHit delegate) {
            this.delegate = delegate;
            source = delegate.hasSource() ? delegate.getSourceRef() : null;
        }

        @Override
        public String getIndex() {
            return delegate.getIndex();
        }

        @Override
        public String getId() {
            return delegate.getId();
        }

        @Override
        public BytesReference getSource() {
            return source;
        }

        @Override
        public MediaType getMediaType() {
            return MediaTypeRegistry.xContentType(source);
        }

        @Override
        public long getVersion() {
            return delegate.getVersion();
        }

        @Override
        public long getSeqNo() {
            return delegate.getSeqNo();
        }

        @Override
        public long getPrimaryTerm() {
            return delegate.getPrimaryTerm();
        }

        @Override
        public String getRouting() {
            return fieldValue(RoutingFieldMapper.NAME);
        }

        private <T> T fieldValue(String fieldName) {
            DocumentField field = delegate.field(fieldName);
            return field == null ? null : field.getValue();
        }
    }
}
