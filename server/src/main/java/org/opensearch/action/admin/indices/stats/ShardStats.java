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

package org.opensearch.action.admin.indices.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.seqno.RetentionLeaseStats;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;

/**
 * Shard Stats for OpenSearch
 *
 * @opensearch.internal
 */
public class ShardStats implements Writeable, ToXContentFragment {

    private ShardRouting shardRouting;
    private CommonStats commonStats;
    @Nullable
    private CommitStats commitStats;
    @Nullable
    private SeqNoStats seqNoStats;

    @Nullable
    private RetentionLeaseStats retentionLeaseStats;

    /**
     * Gets the current retention lease stats.
     *
     * @return the current retention lease stats
     */
    public RetentionLeaseStats getRetentionLeaseStats() {
        return retentionLeaseStats;
    }

    private String dataPath;
    private String statePath;
    private boolean isCustomDataPath;

    public ShardStats(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        commonStats = new CommonStats(in);
        commitStats = CommitStats.readOptionalCommitStatsFrom(in);
        statePath = in.readString();
        dataPath = in.readString();
        isCustomDataPath = in.readBoolean();
        seqNoStats = in.readOptionalWriteable(SeqNoStats::new);
        retentionLeaseStats = in.readOptionalWriteable(RetentionLeaseStats::new);
    }

    public ShardStats(
        final ShardRouting routing,
        final ShardPath shardPath,
        final CommonStats commonStats,
        final CommitStats commitStats,
        final SeqNoStats seqNoStats,
        final RetentionLeaseStats retentionLeaseStats
    ) {
        this.shardRouting = routing;
        this.dataPath = shardPath.getRootDataPath().toString();
        this.statePath = shardPath.getRootStatePath().toString();
        this.isCustomDataPath = shardPath.isCustomDataPath();
        this.commitStats = commitStats;
        this.commonStats = commonStats;
        this.seqNoStats = seqNoStats;
        this.retentionLeaseStats = retentionLeaseStats;
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public CommonStats getStats() {
        return this.commonStats;
    }

    @Nullable
    public CommitStats getCommitStats() {
        return this.commitStats;
    }

    @Nullable
    public SeqNoStats getSeqNoStats() {
        return this.seqNoStats;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getStatePath() {
        return statePath;
    }

    public boolean isCustomDataPath() {
        return isCustomDataPath;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        commonStats.writeTo(out);
        out.writeOptionalWriteable(commitStats);
        out.writeString(statePath);
        out.writeString(dataPath);
        out.writeBoolean(isCustomDataPath);
        out.writeOptionalWriteable(seqNoStats);
        out.writeOptionalWriteable(retentionLeaseStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ROUTING)
            .field(Fields.STATE, shardRouting.state())
            .field(Fields.PRIMARY, shardRouting.primary())
            .field(Fields.NODE, shardRouting.currentNodeId())
            .field(Fields.RELOCATING_NODE, shardRouting.relocatingNodeId())
            .endObject();

        commonStats.toXContent(builder, params);
        if (commitStats != null) {
            commitStats.toXContent(builder, params);
        }
        if (seqNoStats != null) {
            seqNoStats.toXContent(builder, params);
        }
        if (retentionLeaseStats != null) {
            retentionLeaseStats.toXContent(builder, params);
        }
        builder.startObject(Fields.SHARD_PATH);
        builder.field(Fields.STATE_PATH, statePath);
        builder.field(Fields.DATA_PATH, dataPath);
        builder.field(Fields.IS_CUSTOM_DATA_PATH, isCustomDataPath);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String ROUTING = "routing";
        static final String STATE = "state";
        static final String STATE_PATH = "state_path";
        static final String DATA_PATH = "data_path";
        static final String IS_CUSTOM_DATA_PATH = "is_custom_data_path";
        static final String SHARD_PATH = "shard_path";
        static final String PRIMARY = "primary";
        static final String NODE = "node";
        static final String RELOCATING_NODE = "relocating_node";
    }

}
