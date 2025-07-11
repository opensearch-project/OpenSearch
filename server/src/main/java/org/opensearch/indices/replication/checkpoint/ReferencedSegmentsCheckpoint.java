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
import java.util.Set;

/**
 * Represents referenced segments which is sent to a replica shard.
 * Inherit {@link ReplicationCheckpoint}, but the segmentsGen will not be used.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class ReferencedSegmentsCheckpoint extends ReplicationCheckpoint {
    private final Set<String> segmentNames;

    public ReferencedSegmentsCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long segmentInfosVersion,
        long length,
        String codec,
        Map<String, StoreFileMetadata> metadataMap,
        Set<String> segmentNames
    ) {
        super(shardId, primaryTerm, SequenceNumbers.NO_OPS_PERFORMED, segmentInfosVersion, length, codec, metadataMap);
        this.segmentNames = segmentNames;
    }

    public ReferencedSegmentsCheckpoint(StreamInput in) throws IOException {
        super(in);
        segmentNames = in.readSet(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(segmentNames);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReferencedSegmentsCheckpoint that = (ReferencedSegmentsCheckpoint) o;
        return getPrimaryTerm() == that.getPrimaryTerm()
            && getSegmentInfosVersion() == that.getSegmentInfosVersion()
            && segmentNames.equals(that.segmentNames)
            && Objects.equals(getShardId(), that.getShardId())
            && getCodec().equals(that.getCodec());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getShardId(), getPrimaryTerm(), getSegmentInfosVersion(), segmentNames);
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
            + ", segmentNames="
            + segmentNames
            + '}';
    }

    public Set<String> getSegmentNames() {
        return segmentNames;
    }
}
