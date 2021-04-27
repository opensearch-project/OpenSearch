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

package org.opensearch.action.bulk;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.support.WriteResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;

public class BulkShardResponse extends ReplicationResponse implements WriteResponse {

    private static final Version COMPACT_SHARD_ID_VERSION = LegacyESVersion.V_7_9_0;

    private final ShardId shardId;
    private final BulkItemResponse[] responses;

    BulkShardResponse(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        responses = in.readArray(in.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION)
                ? i -> new BulkItemResponse(shardId, i) : BulkItemResponse::new, BulkItemResponse[]::new);
    }

    // NOTE: public for testing only
    public BulkShardResponse(ShardId shardId, BulkItemResponse[] responses) {
        this.shardId = shardId;
        this.responses = responses;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public BulkItemResponse[] getResponses() {
        return responses;
    }

    @Override
    public void setForcedRefresh(boolean forcedRefresh) {
        /*
         * Each DocWriteResponse already has a location for whether or not it forced a refresh so we just set that information on the
         * response.
         */
        for (BulkItemResponse response : responses) {
            DocWriteResponse r = response.getResponse();
            if (r != null) {
                r.setForcedRefresh(forcedRefresh);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeArray(out.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION)
                ? (o, item) -> item.writeThin(out) : (o, item) -> item.writeTo(o), responses);
    }
}
