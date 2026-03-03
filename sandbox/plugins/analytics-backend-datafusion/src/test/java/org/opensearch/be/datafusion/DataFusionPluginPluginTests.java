/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.engine.engine.ExecutionEnginePlugin;
import org.opensearch.engine.exec.OperatorType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Tests for {@link DataFusionPlugin}.
 */
public class DataFusionPluginPluginTests extends OpenSearchTestCase {

    public void testImplementsExecutionEnginePlugin() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        assertTrue(plugin instanceof ExecutionEnginePlugin);
    }

    public void testNameIsDatafusion() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        assertEquals("datafusion", plugin.name());
    }

    public void testSupportedOperators() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        Set<OperatorType> ops = plugin.supportedOperators();
        assertTrue(ops.contains(OperatorType.FILTER));
        assertTrue(ops.contains(OperatorType.PROJECT));
        assertTrue(ops.contains(OperatorType.AGGREGATE));
        assertTrue(ops.contains(OperatorType.SORT));
        assertFalse("DataFusionPlugin does not handle SCAN", ops.contains(OperatorType.SCAN));
    }

    public void testBridgeIsNotNull() {
        DataFusionPlugin plugin = new DataFusionPlugin();
        EngineBridge bridge = plugin.bridge();
        assertNotNull(bridge);
        assertTrue(bridge instanceof DataFusionBridge);
    }
}
