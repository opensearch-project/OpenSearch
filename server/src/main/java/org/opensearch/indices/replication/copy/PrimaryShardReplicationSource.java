/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.copy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.RetryableAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Map;

public class PrimaryShardReplicationSource {

    private static final Logger logger = LogManager.getLogger(PrimaryShardReplicationSource.class);
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;

    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final SegmentReplicationReplicaService segmentReplicationReplicaService;

    public PrimaryShardReplicationSource(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        RecoverySettings recoverySettings,
        SegmentReplicationReplicaService segmentReplicationReplicaShardService
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.threadPool = transportService.getThreadPool();
        this.segmentReplicationReplicaService = segmentReplicationReplicaShardService;
    }
}
