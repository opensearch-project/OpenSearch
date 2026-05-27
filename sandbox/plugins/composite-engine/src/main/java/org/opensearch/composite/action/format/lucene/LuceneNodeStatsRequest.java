/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.lucene;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for Lucene node-level stats. Targets all indices and optionally filters by node.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneNodeStatsRequest extends BroadcastRequest<LuceneNodeStatsRequest> {

    private final boolean shardLevel;
    private final String nodeFilter;

    public LuceneNodeStatsRequest(boolean shardLevel, String nodeFilter) {
        super("_all");
        this.shardLevel = shardLevel;
        this.nodeFilter = nodeFilter;
    }

    public LuceneNodeStatsRequest(StreamInput in) throws IOException {
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
        return new String[] { "_all" };
    }

    public boolean isShardLevel() {
        return shardLevel;
    }

    public String getNodeFilter() {
        return nodeFilter;
    }
}
