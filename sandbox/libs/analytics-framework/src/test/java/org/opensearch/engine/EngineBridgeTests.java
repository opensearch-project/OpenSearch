/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine;

import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.test.OpenSearchTestCase;

public class EngineBridgeTests extends OpenSearchTestCase {

    public void testBridgeCanBeImplementedAsLambda() {
        // NativeEngineBridge is a functional interface with one method: execute()
        EngineBridge bridge = (serializedPlan, input, allocator) -> input;
        assertNotNull(bridge);
    }

    public void testBridgeCanBeImplementedAsClass() {
        EngineBridge bridge = new EngineBridge() {
            @Override
            public org.apache.arrow.vector.VectorSchemaRoot execute(
                byte[] serializedPlan,
                org.apache.arrow.vector.VectorSchemaRoot input,
                org.apache.arrow.memory.BufferAllocator allocator
            ) {
                return input;
            }
        };
        assertNotNull(bridge);
    }
}
