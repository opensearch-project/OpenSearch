/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.engine.operator;

import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.engine.exec.ArrowBatch;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

public class NativeEngineOperatorTests extends OpenSearchTestCase {

    public void testDelegatesToBridgeAndReturnsResult() {
        byte[] planBytes = "test-plan".getBytes(StandardCharsets.UTF_8);

        // Track whether bridge was called
        boolean[] bridgeCalled = { false };
        EngineBridge bridge = (serializedPlan, input, allocator) -> {
            bridgeCalled[0] = true;
            assertArrayEquals(planBytes, serializedPlan);
            return input;  // pass-through
        };

        ArrowBatch childBatch = new ArrowBatch(null, 5);
        Operator childOp = new Operator() {
            private boolean done = false;

            @Override
            public void open() {}

            @Override
            public ArrowBatch next() {
                if (done) return null;
                done = true;
                return childBatch;
            }

            @Override
            public boolean isFinished() {
                return done;
            }

            @Override
            public void close() {}
        };

        NativeEngineOperator op = new NativeEngineOperator(childOp, planBytes, bridge, null);
        op.open();

        ArrowBatch result = op.next();
        assertTrue("Bridge should have been called", bridgeCalled[0]);
        assertNotNull(result);

        // Second call should return null (child exhausted)
        ArrowBatch secondResult = op.next();
        assertNull(secondResult);
        assertTrue(op.isFinished());

        op.close();
    }

    public void testIsFinishedReturnsTrueAfterChildExhausted() {
        EngineBridge bridge = (plan, input, alloc) -> input;

        Operator childOp = new Operator() {
            @Override
            public void open() {}

            @Override
            public ArrowBatch next() {
                return null;
            }

            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public void close() {}
        };

        NativeEngineOperator op = new NativeEngineOperator(childOp, new byte[] { 1 }, bridge, null);
        op.open();
        assertNull(op.next());
        assertTrue(op.isFinished());
        op.close();
    }

    public void testCloseClosesChild() {
        boolean[] childClosed = { false };
        Operator childOp = new Operator() {
            @Override
            public void open() {}

            @Override
            public ArrowBatch next() {
                return null;
            }

            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public void close() {
                childClosed[0] = true;
            }
        };

        EngineBridge bridge = (plan, input, alloc) -> input;
        NativeEngineOperator op = new NativeEngineOperator(childOp, new byte[] { 1 }, bridge, null);
        op.close();
        assertTrue("Child operator should be closed", childClosed[0]);
    }
}
