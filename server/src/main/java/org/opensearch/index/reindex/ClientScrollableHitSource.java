/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.Client;
import org.opensearch.client.ParentTaskAssigningClient;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.search.SearchHit;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.common.unit.TimeValue.timeValueNanos;
import static org.opensearch.core.common.util.CollectionUtils.isEmpty;

/**
 * A scrollable source of hits from a {@linkplain Client} instance.
 *
 * @opensearch.internal
 */
public class ClientScrollableHitSource extends ScrollableHitSource {
    private final ParentTaskAssigningClient client;
    private final SearchRequest firstSearchRequest;

    public ClientScrollableHitSource(
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
        firstSearchRequest.allowPartialSearchResults(false);
    }

    @Override
    public void doStart(RejectAwareActionListener<Response> searchListener) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                "executing initial scroll against {}",
                isEmpty(firstSearchRequest.indices()) ? "all indices" : firstSearchRequest.indices()
            );
        }
        client.search(firstSearchRequest, wrapListener(searchListener));
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        SearchScrollRequest request = new SearchScrollRequest();
        // Add the wait time into the scroll timeout so it won't timeout while we wait for throttling
        request.scrollId(scrollId).scroll(timeValueNanos(firstSearchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos()));
        client.searchScroll(request, wrapListener(searchListener));
    }

    private ActionListener<SearchResponse> wrapListener(RejectAwareActionListener<Response> searchListener) {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
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
        };
    }

    @Override
    public void clearScroll(String scrollId, Runnable onCompletion) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        /*
         * Unwrap the client so we don't set our task as the parent. If we *did* set our ID then the clear scroll would be cancelled as
         * if this task is cancelled. But we want to clear the scroll regardless of whether or not the main request was cancelled.
         */
        client.unwrap().clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse response) {
                logger.debug("Freed [{}] contexts", response.getNumFreed());
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("Failed to clear scroll [{}]", scrollId), e);
                onCompletion.run();
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        onCompletion.run();
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
                hits.add(new ClientHit(hit));
            }
            hits = unmodifiableList(hits);
        }
        long total = response.getHits().getTotalHits().value();
        return new Response(response.isTimedOut(), failures, total, hits, response.getScrollId());
    }

    /**
     * A Client hit
     *
     * @opensearch.internal
     */
    private static class ClientHit implements Hit {
        private final SearchHit delegate;
        private final BytesReference source;

        ClientHit(SearchHit delegate) {
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
