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

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Base builder for bulk by scroll requests
 *
 * @opensearch.internal
 */
public abstract class AbstractBulkByScrollRequestBuilder<
    Request extends AbstractBulkByScrollRequest<Request>,
    Self extends AbstractBulkByScrollRequestBuilder<Request, Self>> extends ActionRequestBuilder<Request, BulkByScrollResponse> {
    private final SearchRequestBuilder source;

    protected AbstractBulkByScrollRequestBuilder(
        OpenSearchClient client,
        ActionType<BulkByScrollResponse> action,
        SearchRequestBuilder source,
        Request request
    ) {
        super(client, action, request);
        this.source = source;
    }

    protected abstract Self self();

    /**
     * The search used to find documents to process.
     */
    public SearchRequestBuilder source() {
        return source;
    }

    /**
     * Set the source indices.
     */
    public Self source(String... indices) {
        source.setIndices(indices);
        return self();
    }

    /**
     * Set the query that will filter the source. Just a convenience method for
     * easy chaining.
     */
    public Self filter(QueryBuilder filter) {
        source.setQuery(filter);
        return self();
    }

    /**
     * Maximum number of processed documents. Defaults to processing all
     * documents.
     * @deprecated please use maxDocs(int) instead.
     */
    @Deprecated
    public Self size(int size) {
        return maxDocs(size);
    }

    /**
     * Maximum number of processed documents. Defaults to processing all
     * documents.
     */
    public Self maxDocs(int maxDocs) {
        request.setMaxDocs(maxDocs);
        return self();
    }

    /**
     * Set whether or not version conflicts cause the action to abort.
     */
    public Self abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.setAbortOnVersionConflict(abortOnVersionConflict);
        return self();
    }

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public Self refresh(boolean refresh) {
        request.setRefresh(refresh);
        return self();
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request.
     */
    public Self timeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return self();
    }

    /**
     * The number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public Self waitForActiveShards(ActiveShardCount activeShardCount) {
        request.setWaitForActiveShards(activeShardCount);
        return self();
    }

    /**
     * Initial delay after a rejection before retrying a bulk request. With the default maxRetries the total backoff for retrying rejections
     * is about one minute per bulk request. Once the entire bulk request is successful the retry counter resets.
     */
    public Self setRetryBackoffInitialTime(TimeValue retryBackoffInitialTime) {
        request.setRetryBackoffInitialTime(retryBackoffInitialTime);
        return self();
    }

    /**
     * Total number of retries attempted for rejections. There is no way to ask for unlimited retries.
     */
    public Self setMaxRetries(int maxRetries) {
        request.setMaxRetries(maxRetries);
        return self();
    }

    /**
     * Set the throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    public Self setRequestsPerSecond(float requestsPerSecond) {
        request.setRequestsPerSecond(requestsPerSecond);
        return self();
    }

    /**
     * Should this task store its result after it has finished?
     */
    public Self setShouldStoreResult(boolean shouldStoreResult) {
        request.setShouldStoreResult(shouldStoreResult);
        return self();
    }

    /**
     * The number of slices this task should be divided into. Defaults to 1 meaning the task isn't sliced into subtasks.
     */
    public Self setSlices(int slices) {
        request.setSlices(slices);
        return self();
    }
}
