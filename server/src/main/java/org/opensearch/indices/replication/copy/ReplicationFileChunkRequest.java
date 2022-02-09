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

package org.opensearch.indices.replication.copy;

import org.apache.lucene.util.Version;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

public final class ReplicationFileChunkRequest extends TransportRequest {
    private final boolean lastChunk;
    private final ShardId shardId;
    private final long position;
    private final BytesReference content;
    private final StoreFileMetadata metadata;
    private final long sourceThrottleTimeInNanos;

    private final long replicationId;
    private final long requestSeqNo;

    public ReplicationFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        replicationId = in.readLong();
        requestSeqNo = in.readLong();
        String name = in.readString();
        position = in.readVLong();
        long length = in.readVLong();
        String checksum = in.readString();
        content = in.readBytesReference();
        Version writtenBy = Lucene.parseVersionLenient(in.readString(), null);
        assert writtenBy != null;
        metadata = new StoreFileMetadata(name, length, checksum, writtenBy);
        lastChunk = in.readBoolean();
        sourceThrottleTimeInNanos = in.readLong();
    }

    public ReplicationFileChunkRequest(
        long replicationId,
        final long requestSeqNo,
        ShardId shardId,
        StoreFileMetadata metadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        long sourceThrottleTimeInNanos) {
        this.replicationId = replicationId;
        this.requestSeqNo = requestSeqNo;
        this.shardId = shardId;
        this.metadata = metadata;
        this.position = position;
        this.content = content;
        this.lastChunk = lastChunk;
        this.sourceThrottleTimeInNanos = sourceThrottleTimeInNanos;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String name() {
        return metadata.name();
    }

    public long position() {
        return position;
    }

    public long length() {
        return metadata.length();
    }

    public BytesReference content() {
        return content;
    }

    public long sourceThrottleTimeInNanos() {
        return sourceThrottleTimeInNanos;
    }

    public long getReplicationId() {
        return replicationId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeLong(replicationId);
        out.writeLong(requestSeqNo);
        out.writeString(metadata.name());
        out.writeVLong(position);
        out.writeVLong(metadata.length());
        out.writeString(metadata.checksum());
        out.writeBytesReference(content);
        out.writeString(metadata.writtenBy().toString());
        out.writeBoolean(lastChunk);
        out.writeLong(sourceThrottleTimeInNanos);
    }

    @Override
    public String toString() {
        return shardId + ": name='" + name() + '\'' + ", position=" + position + ", length=" + length();
    }

    public StoreFileMetadata metadata() {
        return metadata;
    }

    /**
     * Returns <code>true</code> if this chunk is the last chunk in the stream.
     */
    public boolean lastChunk() {
        return lastChunk;
    }

    public long requestSeqNo() {
        return requestSeqNo;
    }
}
