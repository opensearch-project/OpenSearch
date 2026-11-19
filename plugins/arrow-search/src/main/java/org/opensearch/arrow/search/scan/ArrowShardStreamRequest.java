/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.Map;

/**
 * Asks a shard copy to stream the doc values of the matches of a search.
 * <p>
 * It is the shard search request the coordinator built, plus the scan's columns and batch sizing, so everything that
 * applies to shard search requests, such as document-level security, applies to it too.
 */
public final class ArrowShardStreamRequest extends ShardSearchRequest {

    private final ArrowScanSpec spec;

    /**
     * Adds a scan spec to the coordinator's shard request.
     *
     * @param shardRequest the shard request the coordinator built: query, alias filter, routing and start time
     * @param spec         the columns and batch sizing
     */
    public ArrowShardStreamRequest(ShardSearchRequest shardRequest, ArrowScanSpec spec) {
        super(shardRequest);
        this.spec = spec;
    }

    /**
     * Reads a request written by {@link #writeTo}.
     *
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public ArrowShardStreamRequest(StreamInput in) throws IOException {
        super(in);
        spec = new ArrowScanSpec(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        spec.writeTo(out);
    }

    /** Returns the scan's columns and batch sizing. */
    public ArrowScanSpec spec() {
        return spec;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new ArrowShardScanTask(id, type, action, getDescription(), parentTaskId, headers, this::getMetadataSupplier);
    }

    @Override
    public String getDescription() {
        return super.getDescription() + " columns" + spec.columns().stream().map(c -> c.name()).toList();
    }
}
