/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.IndexMetric;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.util.Set;

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

    public void testApplyMetricFiltering() {
        ClusterStatsRequestBuilder clusterStatsRequestBuilder = new ClusterStatsRequestBuilder(
            this.testClient,
            ClusterStatsAction.INSTANCE
        );
        assertFalse(clusterStatsRequestBuilder.request().applyMetricFiltering());
        clusterStatsRequestBuilder.applyMetricFiltering(true);
        assertTrue(clusterStatsRequestBuilder.request().applyMetricFiltering());
    }

    public void testRequestedMetrics() {
        ClusterStatsRequestBuilder clusterStatsRequestBuilder = new ClusterStatsRequestBuilder(
            this.testClient,
            ClusterStatsAction.INSTANCE
        );
        clusterStatsRequestBuilder.applyMetricFiltering(true);
        clusterStatsRequestBuilder.requestMetrics(Set.of(Metric.OS, Metric.JVM));
        assertTrue(clusterStatsRequestBuilder.request().applyMetricFiltering());
        assertEquals(Set.of(Metric.OS, Metric.JVM), clusterStatsRequestBuilder.request().requestedMetrics());
    }

    public void testIndicesMetrics() {
        ClusterStatsRequestBuilder clusterStatsRequestBuilder = new ClusterStatsRequestBuilder(
            this.testClient,
            ClusterStatsAction.INSTANCE
        );
        clusterStatsRequestBuilder.applyMetricFiltering(true);
        clusterStatsRequestBuilder.requestMetrics(Set.of(Metric.INDICES, Metric.JVM));
        clusterStatsRequestBuilder.indexMetrics(Set.of(IndexMetric.MAPPINGS, IndexMetric.ANALYSIS));
        assertTrue(clusterStatsRequestBuilder.request().applyMetricFiltering());
        assertEquals(Set.of(Metric.INDICES, Metric.JVM), clusterStatsRequestBuilder.request().requestedMetrics());
        assertEquals(Set.of(IndexMetric.MAPPINGS, IndexMetric.ANALYSIS), clusterStatsRequestBuilder.request().indicesMetrics());
    }

}
