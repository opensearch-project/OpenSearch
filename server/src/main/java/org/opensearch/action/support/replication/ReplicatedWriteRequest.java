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

package org.opensearch.action.support.replication;

import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Requests that are both {@linkplain ReplicationRequest}s (run on a shard's primary first, then the replica) and {@linkplain WriteRequest}
 * (modify documents on a shard), for example {@link BulkShardRequest}, {@link IndexRequest}, and {@link DeleteRequest}.
 *
 * @opensearch.internal
 */
public abstract class ReplicatedWriteRequest<R extends ReplicatedWriteRequest<R>> extends ReplicationRequest<R> implements WriteRequest<R> {
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    /**
     * Constructor for thin deserialization.
     */
    public ReplicatedWriteRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    /**
     * Constructor for deserialization.
     */
    public ReplicatedWriteRequest(StreamInput in) throws IOException {
        super(in);
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public ReplicatedWriteRequest(@Nullable ShardId shardId) {
        super(shardId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public R setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return (R) this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        refreshPolicy.writeTo(out);
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        refreshPolicy.writeTo(out);
    }
}
