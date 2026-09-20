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

package org.opensearch.action.search;

import org.opensearch.common.MemoizedSupplier;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Task storing information about a currently running search shard request.
 * See {@link ShardSearchRequest}, {@link ShardFetchSearchRequest}, ...
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchShardTask extends WorkloadGroupTask implements SearchBackpressureTask {
    // generating metadata in a lazy way since source can be quite big
    private final MemoizedSupplier<String> metadataSupplier;

    // Time this shard task spent queued on the search thread pool before a worker picked it up. Written once by
    // the search worker thread on dequeue and read later from the same thread, but kept volatile because slow log
    // and profile consumers are not guaranteed to be the writing thread for every phase.
    private volatile long queueWaitNanos = -1;

    public SearchShardTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, () -> "");
    }

    public SearchShardTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        Supplier<String> metadataSupplier
    ) {
        super(id, type, action, description, parentTaskId, headers);
        this.metadataSupplier = new MemoizedSupplier<>(metadataSupplier);
    }

    public String getTaskMetadata() {
        return metadataSupplier.get();
    }

    /**
     * Time spent waiting in the search thread pool queue, or -1 if the task was never dispatched through a
     * queueing executor (for example when it runs inline on the calling thread).
     */
    public long getQueueWaitNanos() {
        return queueWaitNanos;
    }

    public void setQueueWaitNanos(long queueWaitNanos) {
        this.queueWaitNanos = queueWaitNanos;
    }

    @Override
    public boolean supportsResourceTracking() {
        return true;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }
}
