/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a Pre-copy merged segment which is sent to a replica shard.
 * Inherit {@link ReplicationCheckpoint}, but the segmentsGen and segmentInfosVersion will not be used.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class MergeSegmentCheckpoint extends ReplicationCheckpoint {
    private final String segmentName;

    public MergeSegmentCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long length,
        String codec,
        Map<String, StoreFileMetadata> metadataMap,
        String segmentName
    ) {
        super(shardId, primaryTerm, SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED, length, codec, metadataMap);
        this.segmentName = segmentName;
    }

    public MergeSegmentCheckpoint(StreamInput in) throws IOException {
        super(in);
        segmentName = in.readString();
    }

    /**
     * The segmentName
     *
     * @return the segmentCommitInfo name
     */
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(segmentName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergeSegmentCheckpoint that = (MergeSegmentCheckpoint) o;
        return getPrimaryTerm() == that.getPrimaryTerm()
            && segmentName.equals(that.segmentName)
            && Objects.equals(getShardId(), that.getShardId())
            && getCodec().equals(that.getCodec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getShardId(), getPrimaryTerm(), segmentName);
    }

    @Override
    public String toString() {
        return "ReplicationCheckpoint{"
            + "shardId="
            + getShardId()
            + ", primaryTerm="
            + getPrimaryTerm()
            + ", segmentsGen="
            + getSegmentsGen()
            + ", version="
            + getSegmentInfosVersion()
            + ", size="
            + getLength()
            + ", codec="
            + getCodec()
            + ", timestamp="
            + getCreatedTimeStamp()
            + ", segmentName="
            + segmentName
            + '}';
    }
}
