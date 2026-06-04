/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.worker;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link WorkerNode}
 */
public class WorkerNodeTests extends OpenSearchTestCase {

    public void testCreateWorkerNode() {
        WorkerNode worker = WorkerNode.createWorkerNode("1", "Worker1", "192.168.1.1");
        assertNotNull(worker);
        assertEquals("1", worker.getId());
        assertEquals("Worker1", worker.getName());
        assertEquals("192.168.1.1", worker.getIp());
    }

    public void testEquality() {
        WorkerNode worker1 = WorkerNode.createWorkerNode("5", "Worker5", "192.168.1.5");
        WorkerNode worker2 = WorkerNode.createWorkerNode("5", "Worker5", "192.168.1.5");
        WorkerNode worker3 = WorkerNode.createWorkerNode("6", "Worker6", "192.168.1.6");

        assertEquals(worker1, worker2);
        assertNotEquals(worker1, worker3);
        assertNotEquals(worker2, worker3);
    }

    public void testHashCode() {
        WorkerNode worker1 = WorkerNode.createWorkerNode("7", "Worker7", "192.168.1.7");
        WorkerNode worker2 = WorkerNode.createWorkerNode("7", "Worker7", "192.168.1.7");

        assertEquals(worker1.hashCode(), worker2.hashCode());
    }

    public void testNotEqualToNull() {
        WorkerNode worker = WorkerNode.createWorkerNode("8", "Worker8", "192.168.1.8");
        assertNotEquals(null, worker);
    }

    public void testNotEqualToDifferentClass() {
        WorkerNode worker = WorkerNode.createWorkerNode("9", "Worker9", "192.168.1.9");
        assertNotEquals(worker, new Object());
    }
}
