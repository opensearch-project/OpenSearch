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

package org.opensearch.snapshots;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.Collections;
import java.util.Map;

/**
 * Information about a snapshot shard size
 *
 * @opensearch.internal
 */
public class SnapshotShardSizeInfo {

    public static final SnapshotShardSizeInfo EMPTY = new SnapshotShardSizeInfo(Map.of());

    private final Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes;

    public SnapshotShardSizeInfo(final Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes) {
        this.snapshotShardSizes = Collections.unmodifiableMap(snapshotShardSizes);
    }

    public Long getShardSize(ShardRouting shardRouting) {
        if (shardRouting.primary()
            && shardRouting.active() == false
            && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
            final RecoverySource.SnapshotRecoverySource snapshotRecoverySource = (RecoverySource.SnapshotRecoverySource) shardRouting
                .recoverySource();
            return snapshotShardSizes.get(
                new InternalSnapshotsInfoService.SnapshotShard(
                    snapshotRecoverySource.snapshot(),
                    snapshotRecoverySource.index(),
                    shardRouting.shardId()
                )
            );
        }
        assert false : "Expected shard with snapshot recovery source but was " + shardRouting;
        return null;
    }

    public long getShardSize(ShardRouting shardRouting, long fallback) {
        final Long shardSize = getShardSize(shardRouting);
        if (shardSize == null || shardSize == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            return fallback;
        }
        return shardSize;
    }
}
