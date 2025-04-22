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

package org.opensearch.action.admin.indices.upgrade.get;

import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transport Response for retrieving status of upgrading an Index
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class UpgradeStatusResponse extends BroadcastResponse {
    private ShardUpgradeStatus[] shards;

    private Map<String, IndexUpgradeStatus> indicesUpgradeStatus;

    UpgradeStatusResponse(StreamInput in) throws IOException {
        super(in);
        shards = new ShardUpgradeStatus[in.readVInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = new ShardUpgradeStatus(in);
        }
    }

    UpgradeStatusResponse(
        ShardUpgradeStatus[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    public Map<String, IndexUpgradeStatus> getIndices() {
        if (indicesUpgradeStatus != null) {
            return indicesUpgradeStatus;
        }
        Map<String, IndexUpgradeStatus> indicesUpgradeStats = new HashMap<>();

        Set<String> indices = new HashSet<>();
        for (ShardUpgradeStatus shard : shards) {
            indices.add(shard.getIndex());
        }

        for (String indexName : indices) {
            List<ShardUpgradeStatus> shards = new ArrayList<>();
            for (ShardUpgradeStatus shard : this.shards) {
                if (shard.getShardRouting().getIndexName().equals(indexName)) {
                    shards.add(shard);
                }
            }
            indicesUpgradeStats.put(indexName, new IndexUpgradeStatus(indexName, shards.toArray(new ShardUpgradeStatus[0])));
        }
        this.indicesUpgradeStatus = indicesUpgradeStats;
        return indicesUpgradeStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shards.length);
        for (ShardUpgradeStatus shard : shards) {
            shard.writeTo(out);
        }
    }

    public long getTotalBytes() {
        long totalBytes = 0;
        for (IndexUpgradeStatus indexShardUpgradeStatus : getIndices().values()) {
            totalBytes += indexShardUpgradeStatus.getTotalBytes();
        }
        return totalBytes;
    }

    public long getToUpgradeBytes() {
        long upgradeBytes = 0;
        for (IndexUpgradeStatus indexShardUpgradeStatus : getIndices().values()) {
            upgradeBytes += indexShardUpgradeStatus.getToUpgradeBytes();
        }
        return upgradeBytes;
    }

    public long getToUpgradeBytesAncient() {
        long upgradeBytesAncient = 0;
        for (IndexUpgradeStatus indexShardUpgradeStatus : getIndices().values()) {
            upgradeBytesAncient += indexShardUpgradeStatus.getToUpgradeBytesAncient();
        }
        return upgradeBytesAncient;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getTotalBytes()));
        builder.humanReadableField(Fields.SIZE_TO_UPGRADE_IN_BYTES, Fields.SIZE_TO_UPGRADE, new ByteSizeValue(getToUpgradeBytes()));
        builder.humanReadableField(
            Fields.SIZE_TO_UPGRADE_ANCIENT_IN_BYTES,
            Fields.SIZE_TO_UPGRADE_ANCIENT,
            new ByteSizeValue(getToUpgradeBytesAncient())
        );

        String level = params.param("level", "indices");
        boolean outputShards = "shards".equals(level);
        boolean outputIndices = "indices".equals(level) || outputShards;
        if (outputIndices) {
            builder.startObject(Fields.INDICES);
            for (IndexUpgradeStatus indexUpgradeStatus : getIndices().values()) {
                builder.startObject(indexUpgradeStatus.getIndex());

                builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(indexUpgradeStatus.getTotalBytes()));
                builder.humanReadableField(
                    Fields.SIZE_TO_UPGRADE_IN_BYTES,
                    Fields.SIZE_TO_UPGRADE,
                    new ByteSizeValue(indexUpgradeStatus.getToUpgradeBytes())
                );
                builder.humanReadableField(
                    Fields.SIZE_TO_UPGRADE_ANCIENT_IN_BYTES,
                    Fields.SIZE_TO_UPGRADE_ANCIENT,
                    new ByteSizeValue(indexUpgradeStatus.getToUpgradeBytesAncient())
                );
                if (outputShards) {
                    builder.startObject(Fields.SHARDS);
                    for (IndexShardUpgradeStatus indexShardUpgradeStatus : indexUpgradeStatus) {
                        builder.startArray(Integer.toString(indexShardUpgradeStatus.getShardId().id()));
                        for (ShardUpgradeStatus shardUpgradeStatus : indexShardUpgradeStatus) {
                            builder.startObject();

                            builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getTotalBytes()));
                            builder.humanReadableField(
                                Fields.SIZE_TO_UPGRADE_IN_BYTES,
                                Fields.SIZE_TO_UPGRADE,
                                new ByteSizeValue(getToUpgradeBytes())
                            );
                            builder.humanReadableField(
                                Fields.SIZE_TO_UPGRADE_ANCIENT_IN_BYTES,
                                Fields.SIZE_TO_UPGRADE_ANCIENT,
                                new ByteSizeValue(getToUpgradeBytesAncient())
                            );

                            builder.startObject(Fields.ROUTING);
                            builder.field(Fields.STATE, shardUpgradeStatus.getShardRouting().state());
                            builder.field(Fields.PRIMARY, shardUpgradeStatus.getShardRouting().primary());
                            builder.field(Fields.NODE, shardUpgradeStatus.getShardRouting().currentNodeId());
                            if (shardUpgradeStatus.getShardRouting().relocatingNodeId() != null) {
                                builder.field(Fields.RELOCATING_NODE, shardUpgradeStatus.getShardRouting().relocatingNodeId());
                            }
                            builder.endObject();

                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String SIZE_TO_UPGRADE = "size_to_upgrade";
        static final String SIZE_TO_UPGRADE_ANCIENT = "size_to_upgrade_ancient";
        static final String SIZE_TO_UPGRADE_IN_BYTES = "size_to_upgrade_in_bytes";
        static final String SIZE_TO_UPGRADE_ANCIENT_IN_BYTES = "size_to_upgrade_ancient_in_bytes";
    }
}
