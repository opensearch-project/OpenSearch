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
    private final Integer shardFilter;
    private final String nodeFilter;

    public FormatStatsRequest(String formatName, String[] indices, boolean shardLevel, Integer shardFilter, String nodeFilter) {
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
        this.shardFilter = in.readOptionalVInt();
        this.nodeFilter = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formatName);
        out.writeBoolean(shardLevel);
        out.writeOptionalVInt(shardFilter);
        out.writeOptionalString(nodeFilter);
    }

    public String formatName() {
        return formatName;
    }

    public boolean isShardLevel() {
        return shardLevel;
    }

    public Integer getShardFilter() {
        return shardFilter;
    }

    public String getNodeFilter() {
        return nodeFilter;
    }
}
