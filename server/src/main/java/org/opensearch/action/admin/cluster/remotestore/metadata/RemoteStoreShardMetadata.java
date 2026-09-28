/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Response model that holds the remote store metadata (segment and translog) for a shard.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteStoreShardMetadata implements Writeable, ToXContentFragment {

    private final String indexName;
    private final int shardId;
    private final Map<String, Map<String, Object>> segmentMetadataFiles;
    private final Map<String, Map<String, Object>> translogMetadataFiles;
    private final String latestSegmentMetadataFileName;
    private final String latestTranslogMetadataFileName;

    public RemoteStoreShardMetadata(
        String indexName,
        int shardId,
        Map<String, Map<String, Object>> segmentMetadataFiles,
        Map<String, Map<String, Object>> translogMetadataFiles,
        String latestSegmentMetadataFileName,
        String latestTranslogMetadataFileName
    ) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.segmentMetadataFiles = segmentMetadataFiles;
        this.translogMetadataFiles = translogMetadataFiles;
        this.latestSegmentMetadataFileName = latestSegmentMetadataFileName;
        this.latestTranslogMetadataFileName = latestTranslogMetadataFileName;
    }

    @SuppressWarnings("unchecked")
    public RemoteStoreShardMetadata(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.shardId = in.readInt();
        this.segmentMetadataFiles = (Map<String, Map<String, Object>>) in.readGenericValue();
        this.translogMetadataFiles = (Map<String, Map<String, Object>>) in.readGenericValue();
        this.latestSegmentMetadataFileName = in.readOptionalString();
        this.latestTranslogMetadataFileName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeInt(shardId);
        out.writeGenericValue(segmentMetadataFiles);
        out.writeGenericValue(translogMetadataFiles);
        out.writeOptionalString(latestSegmentMetadataFileName);
        out.writeOptionalString(latestTranslogMetadataFileName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("index", indexName);
        builder.field("shard", shardId);

        if (latestSegmentMetadataFileName != null) {
            builder.field("latest_segment_metadata_filename", latestSegmentMetadataFileName);
        }

        if (latestTranslogMetadataFileName != null) {
            builder.field("latest_translog_metadata_filename", latestTranslogMetadataFileName);
        }

        builder.startObject("available_segment_metadata_files");
        for (Map.Entry<String, Map<String, Object>> entry : segmentMetadataFiles.entrySet()) {
            builder.startObject(entry.getKey());
            for (Map.Entry<String, Object> inner : entry.getValue().entrySet()) {
                builder.field(inner.getKey(), inner.getValue());
            }
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("available_translog_metadata_files");
        for (Map.Entry<String, Map<String, Object>> entry : translogMetadataFiles.entrySet()) {
            builder.startObject(entry.getKey());
            for (Map.Entry<String, Object> inner : entry.getValue().entrySet()) {
                builder.field(inner.getKey(), inner.getValue());
            }
            builder.endObject();
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

    public Map<String, Map<String, Object>> getSegmentMetadataFiles() {
        return segmentMetadataFiles;
    }

    public Map<String, Map<String, Object>> getTranslogMetadataFiles() {
        return translogMetadataFiles;
    }

    public String getLatestSegmentMetadataFileName() {
        return latestSegmentMetadataFileName;
    }

    public String getLatestTranslogMetadataFileName() {
        return latestTranslogMetadataFileName;
    }
}
