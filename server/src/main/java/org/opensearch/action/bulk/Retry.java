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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Encapsulates synchronous and asynchronous retry logic.
 *
 * @opensearch.internal
 */
public class Retry {
    private final BackoffPolicy backoffPolicy;
    private final Scheduler scheduler;

    public Retry(BackoffPolicy backoffPolicy, Scheduler scheduler) {
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception and delegates results to the
     * provided listener. Retries will be scheduled using the class's thread pool.
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @param listener A listener that is invoked when the bulk request finishes or completes with an exception. The listener is not
     */
    public void withBackoff(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BulkRequest bulkRequest,
        ActionListener<BulkResponse> listener
    ) {
        RetryHandler r = new RetryHandler(backoffPolicy, consumer, listener, scheduler);
        r.execute(bulkRequest);
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception. Retries will be scheduled using
     * the class's thread pool.
     *
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @return a future representing the bulk response returned by the client.
     */
    public PlainActionFuture<BulkResponse> withBackoff(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BulkRequest bulkRequest
    ) {
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        withBackoff(consumer, bulkRequest, future);
        return future;
    }

    /**
     * Retry handler
     *
     * @opensearch.internal
     */
    static class RetryHandler implements ActionListener<BulkResponse> {
        private static final RestStatus RETRY_STATUS = RestStatus.TOO_MANY_REQUESTS;
        private static final Logger logger = LogManager.getLogger(RetryHandler.class);

        private final Scheduler scheduler;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final ActionListener<BulkResponse> listener;
        private final Iterator<TimeValue> backoff;
        // Access only when holding a client-side lock, see also #addResponses()
        private final List<BulkItemResponse> responses = new ArrayList<>();
        private final long startTimestampNanos;
        // needed to construct the next bulk request based on the response to the previous one
        // volatile as we're called from a scheduled thread
        private volatile BulkRequest currentBulkRequest;
        private volatile Scheduler.Cancellable retryCancellable;

        RetryHandler(
            BackoffPolicy backoffPolicy,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            ActionListener<BulkResponse> listener,
            Scheduler scheduler
        ) {
            this.backoff = backoffPolicy.iterator();
            this.consumer = consumer;
            this.listener = listener;
            this.scheduler = scheduler;
            // in contrast to System.currentTimeMillis(), nanoTime() uses a monotonic clock under the hood
            this.startTimestampNanos = System.nanoTime();
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (!bulkItemResponses.hasFailures()) {
                // we're done here, include all responses
                addResponses(bulkItemResponses, (r -> true));
                finishHim();
            } else {
                if (canRetry(bulkItemResponses)) {
                    addResponses(bulkItemResponses, (r -> !r.isFailed()));
                    retry(createBulkRequestForRetry(bulkItemResponses));
                } else {
                    addResponses(bulkItemResponses, (r -> true));
                    finishHim();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof RemoteTransportException && ((RemoteTransportException) e).status() == RETRY_STATUS && backoff.hasNext()) {
                retry(currentBulkRequest);
            } else {
                try {
                    listener.onFailure(e);
                } finally {
                    if (retryCancellable != null) {
                        retryCancellable.cancel();
                    }
                }
            }
        }

        private void retry(BulkRequest bulkRequestForRetry) {
            assert backoff.hasNext();
            TimeValue next = backoff.next();
            logger.trace("Retry of bulk request scheduled in {} ms.", next.millis());
            retryCancellable = scheduler.schedule(() -> this.execute(bulkRequestForRetry), next, ThreadPool.Names.SAME);
        }

        private BulkRequest createBulkRequestForRetry(BulkResponse bulkItemResponses) {
            // global pipeline should be set in the new Bulk Request
            String globalPipeline = this.currentBulkRequest.pipeline();
            BulkRequest requestToReissue = new BulkRequest().pipeline(globalPipeline);
            int index = 0;
            for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    DocWriteRequest<?> docWriteRequest = currentBulkRequest.requests().get(index);
                    // when executing pipeline failed with retryable exception, the pipeline needs to be executed again
                    if (bulkItemResponse.getFailure().getSource() == BulkItemResponse.Failure.FailureSource.PIPELINE
                        && docWriteRequest instanceof IndexRequest indexRequest) {
                        // Reset pipeline configuration for retry, after the first execution, the pipeline was set to _none, so we need to
                        // reset it
                        // to the global pipeline if the global pipeline exists,
                        // if not, set to null to ensure the default pipeline can be resolved and set
                        // see org.opensearch.ingest.IngestService.resolvePipelines()
                        indexRequest.setPipeline(globalPipeline);
                        indexRequest.isPipelineResolved(false);
                    }
                    requestToReissue.add(docWriteRequest);
                }
                index++;
            }
            return requestToReissue;
        }

        private boolean canRetry(BulkResponse bulkItemResponses) {
            if (!backoff.hasNext()) {
                return false;
            }
            for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                if (bulkItemResponse.isFailed()) {
                    final RestStatus status = bulkItemResponse.status();
                    if (status != RETRY_STATUS) {
                        return false;
                    }
                }
            }
            return true;
        }

        private void finishHim() {
            try {
                listener.onResponse(getAccumulatedResponse());
            } finally {
                if (retryCancellable != null) {
                    retryCancellable.cancel();
                }
            }
        }

        private void addResponses(BulkResponse response, Predicate<BulkItemResponse> filter) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (filter.test(bulkItemResponse)) {
                    // Use client-side lock here to avoid visibility issues. This method may be called multiple times
                    // (based on how many retries we have to issue) and relying that the response handling code will be
                    // scheduled on the same thread is fragile.
                    synchronized (responses) {
                        responses.add(bulkItemResponse);
                    }
                }
            }
        }

        private BulkResponse getAccumulatedResponse() {
            BulkItemResponse[] itemResponses;
            synchronized (responses) {
                itemResponses = responses.toArray(new BulkItemResponse[1]);
            }
            long stopTimestamp = System.nanoTime();
            long totalLatencyMs = TimeValue.timeValueNanos(stopTimestamp - startTimestampNanos).millis();
            return new BulkResponse(itemResponses, totalLatencyMs);
        }

        public void execute(BulkRequest bulkRequest) {
            this.currentBulkRequest = bulkRequest;
            consumer.accept(bulkRequest, this);
        }
    }
}
