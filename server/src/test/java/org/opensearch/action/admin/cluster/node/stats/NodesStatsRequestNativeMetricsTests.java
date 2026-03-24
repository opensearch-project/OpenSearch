/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Unit tests for NATIVE_METRICS metric wiring in NodesStatsRequest.
 * Validates: Requirements 4.1, 4.5, 4.6
 */
public class NodesStatsRequestNativeMetricsTests extends OpenSearchTestCase {

    /**
     * Verify that allMetrics() contains "native_metrics".
     */
    public void testNativeRuntimeInAllMetrics() {
        Set<String> allMetrics = NodesStatsRequest.Metric.allMetrics();
        assertTrue("allMetrics() should contain 'native_metrics'", allMetrics.contains("native_metrics"));
    }

    /**
     * Verify that the NATIVE_METRICS enum value has the correct metric name.
     */
    public void testNativeRuntimeMetricEnum() {
        assertEquals("native_metrics", NodesStatsRequest.Metric.NATIVE_METRICS.metricName());
    }

    /**
     * Verify that NATIVE_METRICS.containedIn works correctly with a set containing "native_metrics".
     */
    public void testNativeRuntimeContainedIn() {
        assertTrue(
            "NATIVE_METRICS should be contained in a set with 'native_metrics'",
            NodesStatsRequest.Metric.NATIVE_METRICS.containedIn(Set.of("native_metrics"))
        );
        assertFalse(
            "NATIVE_METRICS should not be contained in an empty set",
            NodesStatsRequest.Metric.NATIVE_METRICS.containedIn(Set.of())
        );
        assertFalse(
            "NATIVE_METRICS should not be contained in a set without 'native_metrics'",
            NodesStatsRequest.Metric.NATIVE_METRICS.containedIn(Set.of("jvm", "os"))
        );
    }

    /**
     * Verify that calling all() includes native_metrics in the requested metrics.
     */
    public void testAllIncludesNativeRuntime() {
        NodesStatsRequest request = new NodesStatsRequest("node");
        request.all();
        assertTrue(
            "all() should include 'native_metrics' in requested metrics",
            request.requestedMetrics().contains("native_metrics")
        );
    }
}
