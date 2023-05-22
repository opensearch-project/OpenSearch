/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.indices.IndicesService;

public class SegmentReplicationUpgradeService implements Releasable {

    private final ClusterApplierService clusterApplierService;
    private final SegmentReplicationUpgradeListener clusterStateListener;

    public SegmentReplicationUpgradeService(IndicesService indicesService, ClusterApplierService clusterApplierService) {
        SegmentReplicationUpgradeListener clusterStateListener = new SegmentReplicationUpgradeListener(indicesService);
        this.clusterApplierService = clusterApplierService;
        this.clusterStateListener = clusterStateListener;
        this.clusterApplierService.addListener(this.clusterStateListener);
    }

    @Override
    public void close() {
        this.clusterApplierService.removeListener(this.clusterStateListener);
    }
}
