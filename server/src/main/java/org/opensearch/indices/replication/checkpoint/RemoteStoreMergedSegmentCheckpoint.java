/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a remote store merged segment checkpoint.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteStoreMergedSegmentCheckpoint extends MergedSegmentCheckpoint {
    private final Map<String, String> localToRemoteSegmentFilenameMap;

    public RemoteStoreMergedSegmentCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long length,
        String codec,
        Map<String, StoreFileMetadata> metadataMap,
        String segmentName,
        @Nullable Map<String, String> localToRemoteSegmentFilenameMap
    ) {
        super(shardId, primaryTerm, length, codec, metadataMap, segmentName);
        this.localToRemoteSegmentFilenameMap = localToRemoteSegmentFilenameMap == null ? new HashMap<>() : localToRemoteSegmentFilenameMap;
    }

    public RemoteStoreMergedSegmentCheckpoint(StreamInput in) throws IOException {
        super(in);
        this.localToRemoteSegmentFilenameMap = in.readMap(StreamInput::readString, StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(getLocalToRemoteSegmentFilenameMap(), StreamOutput::writeString, StreamOutput::writeString);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RemoteStoreMergedSegmentCheckpoint that = (RemoteStoreMergedSegmentCheckpoint) o;
        return getPrimaryTerm() == that.getPrimaryTerm()
            && segmentName.equals(that.segmentName)
            && Objects.equals(getShardId(), that.getShardId())
            && getCodec().equals(that.getCodec());
    }

    @Override
    public String toString() {
        return "RemoteStoreMergedSegmentCheckpoint{"
            + "shardId="
            + getShardId()
            + ", primaryTerm="
            + getPrimaryTerm()
            + ", segmentName="
            + getSegmentName()
            + ", localToRemoteSegmentFilenameSize="
            + getLocalToRemoteSegmentFilenameMap().size()
            + '}';
    }

    public Map<String, String> getLocalToRemoteSegmentFilenameMap() {
        return this.localToRemoteSegmentFilenameMap;
    }

    public void updateLocalToRemoteSegmentFilenameMap(String localSegmentFilename, String remoteSegmentFilename) {
        localToRemoteSegmentFilenameMap.put(localSegmentFilename, remoteSegmentFilename);
    }
}
