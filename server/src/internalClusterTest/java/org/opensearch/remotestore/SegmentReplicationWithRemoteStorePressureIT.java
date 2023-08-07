/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.SegmentReplicationPressureIT;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * This class executes the SegmentReplicationPressureIT suite with remote store integration enabled.
 * Setup is similar to SegmentReplicationPressureIT but this also enables the segment replication using remote store which
 * is behind SEGMENT_REPLICATION_EXPERIMENTAL flag.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationWithRemoteStorePressureIT extends SegmentReplicationPressureIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";

    @Override
    protected boolean segmentReplicationWithRemoteEnabled() {
        return true;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.REMOTE_STORE, "true")
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(remoteStoreClusterSettings(REPOSITORY_NAME)).build();
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(Settings.builder().put("location", absolutePath))
        );
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
    }
}
