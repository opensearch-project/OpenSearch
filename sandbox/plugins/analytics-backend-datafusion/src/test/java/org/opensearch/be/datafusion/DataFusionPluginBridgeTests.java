/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DataFusionBridge}.
 */
public class DataFusionPluginBridgeTests extends OpenSearchTestCase {

    public void testImplementsNativeEngineBridge() {
        DataFusionBridge bridge = new DataFusionBridge();
        assertTrue(bridge instanceof EngineBridge);
    }

    public void testExecutePassthroughReturnsInput() {
        DataFusionBridge bridge = new DataFusionBridge();
        BufferAllocator allocator = mock(BufferAllocator.class);
        VectorSchemaRoot mockRoot = mock(VectorSchemaRoot.class);
        when(mockRoot.getRowCount()).thenReturn(5);

        VectorSchemaRoot result = bridge.execute(new byte[] { 1, 2, 3 }).next();

        // Stub implementation returns input unchanged
        assertSame(mockRoot, result);
        assertEquals(5, result.getRowCount());
    }
}
