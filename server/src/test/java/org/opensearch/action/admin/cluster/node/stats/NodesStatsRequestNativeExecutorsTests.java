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
 * Unit tests for NATIVE_EXECUTORS metric wiring in NodesStatsRequest.
 * Validates: Requirements 4.1, 4.5, 4.6
 */
public class NodesStatsRequestNativeExecutorsTests extends OpenSearchTestCase {

    /**
     * Verify that allMetrics() contains "native_executors".
     */
    public void testNativeRuntimeInAllMetrics() {
        Set<String> allMetrics = NodesStatsRequest.Metric.allMetrics();
        assertTrue("allMetrics() should contain 'native_executors'", allMetrics.contains("native_executors"));
    }

    /**
     * Verify that the NATIVE_EXECUTORS enum value has the correct metric name.
     */
    public void testNativeRuntimeMetricEnum() {
        assertEquals("native_executors", NodesStatsRequest.Metric.NATIVE_EXECUTORS.metricName());
    }

    /**
     * Verify that NATIVE_EXECUTORS.containedIn works correctly with a set containing "native_executors".
     */
    public void testNativeRuntimeContainedIn() {
        assertTrue(
            "NATIVE_EXECUTORS should be contained in a set with 'native_executors'",
            NodesStatsRequest.Metric.NATIVE_EXECUTORS.containedIn(Set.of("native_executors"))
        );
        assertFalse(
            "NATIVE_EXECUTORS should not be contained in an empty set",
            NodesStatsRequest.Metric.NATIVE_EXECUTORS.containedIn(Set.of())
        );
        assertFalse(
            "NATIVE_EXECUTORS should not be contained in a set without 'native_executors'",
            NodesStatsRequest.Metric.NATIVE_EXECUTORS.containedIn(Set.of("jvm", "os"))
        );
    }

    /**
     * Verify that calling all() includes native_executors in the requested metrics.
     */
    public void testAllIncludesNativeRuntime() {
        NodesStatsRequest request = new NodesStatsRequest("node");
        request.all();
        assertTrue(
            "all() should include 'native_executors' in requested metrics",
            request.requestedMetrics().contains("native_executors")
        );
    }
}
