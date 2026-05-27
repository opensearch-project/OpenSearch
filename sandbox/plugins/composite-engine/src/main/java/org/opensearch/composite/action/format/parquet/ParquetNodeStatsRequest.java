/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.parquet;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for parquet node-level stats using broadcast-by-node routing.
 * Always targets all indices ({@code _all}) since this endpoint is cluster-scoped.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetNodeStatsRequest extends BroadcastRequest<ParquetNodeStatsRequest> {

    private final boolean shardLevel;
    private final String nodeFilter;

    public ParquetNodeStatsRequest(boolean shardLevel, String nodeFilter) {
        super("_all");
        this.shardLevel = shardLevel;
        this.nodeFilter = nodeFilter;
    }

    public ParquetNodeStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        this.nodeFilter = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeOptionalString(nodeFilter);
    }

    @Override
    public String[] indices() {
        // Always cluster-scoped: target all indices
        return new String[] { "_all" };
    }

    public boolean isShardLevel() {
        return shardLevel;
    }

    public String getNodeFilter() {
        return nodeFilter;
    }
}
