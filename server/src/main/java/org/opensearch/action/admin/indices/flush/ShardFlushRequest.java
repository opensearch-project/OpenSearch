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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.indices.flush;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Transport request for flushing one or more indices
 *
 * @opensearch.internal
 */
public class ShardFlushRequest extends ReplicationRequest<ShardFlushRequest> {

    private final FlushRequest request;

    public ShardFlushRequest(FlushRequest request, ShardId shardId) {
        super(shardId);
        this.request = request;
        this.waitForActiveShards = ActiveShardCount.NONE; // don't wait for any active shards before proceeding, by default
    }

    public ShardFlushRequest(StreamInput in) throws IOException {
        super(in);
        request = new FlushRequest(in);
    }

    FlushRequest getRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    @Override
    public String toString() {
        return "flush {" + shardId + "}";
    }
}
