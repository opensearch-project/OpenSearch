/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.SegmentReplicationIT;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

/**
 * This class runs Segment Replication Integ test suite with remote store enabled.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationUsingRemoteStoreIT extends SegmentReplicationIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    protected boolean segmentReplicationWithRemoteEnabled() {
        return true;
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }
}
