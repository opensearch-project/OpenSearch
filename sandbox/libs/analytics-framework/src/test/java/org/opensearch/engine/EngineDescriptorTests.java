/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine;

import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.engine.exec.OperatorType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class EngineDescriptorTests extends OpenSearchTestCase {

    public void testEngineDescriptorContractCanBeImplemented() {
        EngineDescriptor descriptor = new EngineDescriptor() {
            @Override
            public String name() {
                return "test-engine";
            }

            @Override
            public Set<OperatorType> supportedOperators() {
                return Set.of(OperatorType.SCAN, OperatorType.FILTER);
            }

            @Override
            public EngineBridge bridge() {
                return null;
            }
        };

        assertEquals("test-engine", descriptor.name());
        assertEquals(Set.of(OperatorType.SCAN, OperatorType.FILTER), descriptor.supportedOperators());
        assertNull(descriptor.bridge());
    }

    public void testEngineDescriptorWithBridge() {
        EngineBridge mockBridge = (plan, input, alloc) -> input;

        EngineDescriptor descriptor = new EngineDescriptor() {
            @Override
            public String name() {
                return "bridge-engine";
            }

            @Override
            public Set<OperatorType> supportedOperators() {
                return Set.of(OperatorType.FILTER);
            }

            @Override
            public EngineBridge bridge() {
                return mockBridge;
            }
        };

        assertEquals("bridge-engine", descriptor.name());
        assertSame(mockBridge, descriptor.bridge());
    }
}
