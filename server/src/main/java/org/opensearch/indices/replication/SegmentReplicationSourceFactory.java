/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

/**
 * Factory to build {@link SegmentReplicationSource} used by {@link SegmentReplicationTargetService}.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceFactory {

    private TransportService transportService;
    private RecoverySettings recoverySettings;
    private ClusterService clusterService;

    public SegmentReplicationSourceFactory(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    public SegmentReplicationSource get(IndexShard shard) {
        // TODO: Default to an implementation that uses the primary shard.
        return null;
    }
}
