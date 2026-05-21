/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for parquet analyze using broadcast-by-node routing.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetAnalyzeRequest extends BroadcastRequest<ParquetAnalyzeRequest> {

    private final boolean shardLevel;
    private final Integer shardFilter;
    private final String nodeFilter;
    private final boolean fileLevel;

    public ParquetAnalyzeRequest(String index, boolean shardLevel, Integer shardFilter, String nodeFilter, boolean fileLevel) {
        super(index);
        this.shardLevel = shardLevel;
        this.shardFilter = shardFilter;
        this.nodeFilter = nodeFilter;
        this.fileLevel = fileLevel;
    }

    public ParquetAnalyzeRequest(StreamInput in) throws IOException {
        super(in);
        this.shardLevel = in.readBoolean();
        this.shardFilter = in.readOptionalVInt();
        this.nodeFilter = in.readOptionalString();
        this.fileLevel = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(shardLevel);
        out.writeOptionalVInt(shardFilter);
        out.writeOptionalString(nodeFilter);
        out.writeBoolean(fileLevel);
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

    public boolean isFileLevel() {
        return fileLevel;
    }
}
