/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Response model that holds the remote store metadata (segment and translog) for a shard.
 *
 * @opensearch.internal
 */
@PublicApi(since = "3.0.0")
public class RemoteStoreMetadata implements Writeable, ToXContentFragment {
    private final Map<String, Object> segments;
    private final Map<String, Object> translog;
    private final String indexName;
    private final int shardId;

    public RemoteStoreMetadata(
        Map<String, Object> segments,
        Map<String, Object> translog,
        String indexName,
        int shardId
    ) {
        this.segments = segments;
        this.translog = translog;
        this.indexName = indexName;
        this.shardId = shardId;
    }

    public RemoteStoreMetadata(StreamInput in) throws IOException {
        this.segments = in.readMap();
        this.translog = in.readMap();
        this.indexName = in.readString();
        this.shardId = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(segments);
        out.writeMap(translog);
        out.writeString(indexName);
        out.writeInt(shardId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        
        builder.field("index", indexName);
        builder.field("shard", shardId);

        builder.startObject("segments");
        for (Map.Entry<String, Object> entry : segments.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.startObject("translog");
        for (Map.Entry<String, Object> entry : translog.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        return builder.endObject();
    }

    public String getIndexName() {
        return indexName;
    }

    public int getShardId() {
        return shardId;
    }
}
