/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.engine.exec.OperatorType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

/**
 * Unit tests for {@link DataFusionPlugin} back-end migration.
 * Validates Requirements 6.1, 6.2, 6.3.
 */
public class DataFusionPluginAnalyticsBackEndPluginTests extends OpenSearchTestCase {

    public void testImplementsAnalyticsBackEndPlugin() throws IOException {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertTrue("DataFusionPlugin must implement AnalyticsBackEndPlugin", plugin instanceof AnalyticsBackEndPlugin);
        }
    }

    public void testImplementsEngineDescriptor() throws IOException {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertTrue("DataFusionPlugin must implement EngineDescriptor", plugin instanceof EngineDescriptor);
        }
    }

    public void testSupportedOperators() throws IOException {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            Set<OperatorType> expected = Set.of(
                OperatorType.SCAN,
                OperatorType.FILTER,
                OperatorType.PROJECT,
                OperatorType.AGGREGATE,
                OperatorType.SORT
            );
            assertEquals(expected, plugin.supportedOperators());
        }
    }

    public void testBridgeReturnsDataFusionBridge() throws IOException {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertNotNull("bridge() must not return null", plugin.bridge());
            assertTrue("bridge() must return a DataFusionBridge instance", plugin.bridge() instanceof DataFusionBridge);
        }
    }

    public void testNameReturnsDatafusion() throws IOException {
        try (DataFusionPlugin plugin = new DataFusionPlugin()) {
            assertEquals("datafusion", plugin.name());
        }
    }
}
