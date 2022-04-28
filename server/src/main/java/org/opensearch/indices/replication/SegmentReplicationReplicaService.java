/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class SegmentReplicationReplicaService {
    private final ThreadPool threadPool;
    private final RecoverySettings recoverySettings;
    private final TransportService transportService;


    public SegmentReplicationReplicaService(
        final ThreadPool threadPool,
        final RecoverySettings recoverySettings,
        final TransportService transportService
    ) {
        this.threadPool = threadPool;
        this.recoverySettings = recoverySettings;
        this.transportService = transportService;
    }
}
