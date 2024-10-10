/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

/**
 * Tests the compatibility between types of nodes based on the configured repositories
 *  Non Remote node [No Repositories configured]
 *  Remote Publish Configured Node [Cluster State + Routing Table]
 *  Remote Node [Cluster State + Segment + Translog]
 *  Remote Node With Routing Table [Cluster State + Segment + Translog + Routing Table]
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemotePublicationConfigurationIT extends RemoteRepositoryConfigurationIT {
    @Before
    public void setUp() throws Exception {
        if (segmentRepoPath == null || translogRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
            translogRepoPath = randomRepoPath().toAbsolutePath();
        }
        super.remoteRepoPrefix = "remote_publication";
        super.setUp();
    }

}
