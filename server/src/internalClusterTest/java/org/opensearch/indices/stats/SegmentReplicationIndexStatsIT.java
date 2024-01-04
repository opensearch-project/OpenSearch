/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.stats;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.junit.Ignore;

import static org.opensearch.indices.IndicesService.CLUSTER_SETTING_REPLICATION_TYPE;

public class SegmentReplicationIndexStatsIT extends IndexStatsIT {
    public SegmentReplicationIndexStatsIT(Settings settings) {
        super(settings);
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CLUSTER_SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @Override
    @Ignore("The testSimpleStats is not compatible with Segment Replication behaviour as the test asserts the index operation count on primary and replica shards."
        + "With Segment Replication the index operation is not performed on replica shards, so the assertion fails.")
    public void testSimpleStats() throws Exception {}

}
