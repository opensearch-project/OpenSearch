/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

public class ClusterStatsRequestBuilderTests extends OpenSearchTestCase {

    private NoOpClient testClient;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.testClient = new NoOpClient(getTestName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        this.testClient.close();
        super.tearDown();
    }

    public void testUseAggregatedNodeLevelResponses() {
        ClusterStatsRequestBuilder clusterStatsRequestBuilder = new ClusterStatsRequestBuilder(
            this.testClient,
            ClusterStatsAction.INSTANCE
        );
        clusterStatsRequestBuilder.useAggregatedNodeLevelResponses(false);
        assertFalse(clusterStatsRequestBuilder.request().useAggregatedNodeLevelResponses());
    }

    public void testIncludeMappingStats() {
        ClusterStatsRequestBuilder clusterStatsRequestBuilder = new ClusterStatsRequestBuilder(
            this.testClient,
            ClusterStatsAction.INSTANCE
        );
        clusterStatsRequestBuilder.includeMappingStats(false);
        assertFalse(clusterStatsRequestBuilder.request().isIncludeMappingStats());
    }

    public void testIncludeAnalysisStats() {
        ClusterStatsRequestBuilder clusterStatsRequestBuilder = new ClusterStatsRequestBuilder(
            this.testClient,
            ClusterStatsAction.INSTANCE
        );
        clusterStatsRequestBuilder.includeAnalysisStats(false);
        assertFalse(clusterStatsRequestBuilder.request().isIncludeAnalysisStats());
    }
}
