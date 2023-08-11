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
import org.opensearch.indices.replication.SegmentReplicationIT;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * This class runs Segment Replication Integ test suite with remote store enabled.
 * Setup is similar to SegmentReplicationRemoteStoreIT but this also enables the segment replication using remote store which
 * is behind SEGMENT_REPLICATION_EXPERIMENTAL flag. After this is moved out of experimental, we can combine and keep only one
 * test suite for Segment and Remote store integration tests.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationUsingRemoteStoreIT extends SegmentReplicationIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(remoteStoreClusterSettings(REPOSITORY_NAME)).build();
    }

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

    @Override
    public void testPressureServiceStats() throws Exception {
        super.testPressureServiceStats();
    }
}
