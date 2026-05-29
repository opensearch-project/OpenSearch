/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Broadcast request for per-format index-level stats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatStatsRequest extends BroadcastRequest<FormatStatsRequest> {

    private final String formatName;
    private final boolean shardLevel;
    private final int[] shardFilter;
    private final String[] nodeFilter;

    public FormatStatsRequest(String formatName, String[] indices, boolean shardLevel, int[] shardFilter, String[] nodeFilter) {
        super(indices);
        this.formatName = formatName;
        this.shardLevel = shardLevel;
        this.shardFilter = shardFilter;
        this.nodeFilter = nodeFilter;
    }

    public FormatStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.formatName = in.readString();
        this.shardLevel = in.readBoolean();
        this.shardFilter = in.readBoolean() ? in.readVIntArray() : null;
        this.nodeFilter = in.readBoolean() ? in.readStringArray() : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formatName);
        out.writeBoolean(shardLevel);
        if (shardFilter == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVIntArray(shardFilter);
        }
        if (nodeFilter == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeStringArray(nodeFilter);
        }
    }

    public String formatName() {
        return formatName;
    }

    public boolean isShardLevel() {
        return shardLevel;
    }

    public int[] getShardFilter() {
        return shardFilter;
    }

    public String[] getNodeFilter() {
        return nodeFilter;
    }
}
