/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.MemoizedSupplier;
import org.opensearch.search.fetch.ShardFetchSearchRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.ProtobufCancellableTask;
import org.opensearch.tasks.SearchBackpressureTask;
import org.opensearch.tasks.ProtobufTaskId;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Task storing information about a currently running search shard request.
 * See {@link ShardSearchRequest}, {@link ShardFetchSearchRequest}, ...
 *
 * @opensearch.internal
 */
public class ProtobufSearchShardTask extends ProtobufCancellableTask implements SearchBackpressureTask {
    // generating metadata in a lazy way since source can be quite big
    private final MemoizedSupplier<String> metadataSupplier;

    public ProtobufSearchShardTask(long id, String type, String action, String description, ProtobufTaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, () -> "");
    }

    public ProtobufSearchShardTask(
        long id,
        String type,
        String action,
        String description,
        ProtobufTaskId parentTaskId,
        Map<String, String> headers,
        Supplier<String> metadataSupplier
    ) {
        super(id, type, action, description, parentTaskId, headers);
        this.metadataSupplier = new MemoizedSupplier<>(metadataSupplier);
    }

    public String getTaskMetadata() {
        return metadataSupplier.get();
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
