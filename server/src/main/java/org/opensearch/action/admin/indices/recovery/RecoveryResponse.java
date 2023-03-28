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

package org.opensearch.action.admin.indices.recovery;

import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.recovery.RecoveryState;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Information regarding the recovery state of indices and their associated shards.
 *
 * @opensearch.internal
 */
public class RecoveryResponse extends BroadcastResponse {

    private final Map<String, List<RecoveryState>> shardRecoveryStates;

    public RecoveryResponse(StreamInput in) throws IOException {
        super(in);
        shardRecoveryStates = in.readMapOfLists(StreamInput::readString, RecoveryState::new);
    }

    /**
     * Constructs recovery information for a collection of indices and associated shards. Keeps track of how many total shards
     * were seen, and out of those how many were successfully processed and how many failed.
     *
     * @param totalShards       Total count of shards seen
     * @param successfulShards  Count of shards successfully processed
     * @param failedShards      Count of shards which failed to process
     * @param shardRecoveryStates    Map of indices to shard recovery information
     * @param shardFailures     List of failures processing shards
     */
    public RecoveryResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        Map<String, List<RecoveryState>> shardRecoveryStates,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardRecoveryStates = shardRecoveryStates;
    }

    public boolean hasRecoveries() {
        return shardRecoveryStates.size() > 0;
    }

    public Map<String, List<RecoveryState>> shardRecoveryStates() {
        return shardRecoveryStates;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (hasRecoveries()) {
            for (String index : shardRecoveryStates.keySet()) {
                List<RecoveryState> recoveryStates = shardRecoveryStates.get(index);
                if (recoveryStates == null || recoveryStates.size() == 0) {
                    continue;
                }
                builder.startObject(index);
                builder.startArray("shards");
                for (RecoveryState recoveryState : recoveryStates) {
                    builder.startObject();
                    recoveryState.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMapOfLists(shardRecoveryStates, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }
}
