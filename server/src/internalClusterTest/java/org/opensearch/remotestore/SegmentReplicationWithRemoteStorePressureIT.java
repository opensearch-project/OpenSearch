/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.SegmentReplicationPressureIT;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;

/**
 * This class executes the SegmentReplicationPressureIT suite with remote store integration enabled.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationWithRemoteStorePressureIT extends SegmentReplicationPressureIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    @Override
    protected boolean segmentReplicationWithRemoteEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    @Before
    public void setup() {
        absolutePath = randomRepoPath().toAbsolutePath();
        internalCluster().startClusterManagerOnlyNode();
    }

    @After
    public void teardown() {
        internalCluster().wipeIndices("_all");
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }
}
