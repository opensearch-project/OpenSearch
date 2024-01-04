/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;

import static org.opensearch.indices.IndicesService.CLUSTER_SETTING_REPLICATION_TYPE;

public class SegmentReplicationSimpleClusterStateIT extends SimpleClusterStateIT {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

}
