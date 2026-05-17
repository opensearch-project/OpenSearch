/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class AnalyticsShardTaskCancellationListenerTests extends OpenSearchTestCase {

    private AnalyticsShardTask createTask() {
        return new AnalyticsShardTask(1L, "type", "action", "description", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    public void testListenerFiresOnCancellation() {
        AnalyticsShardTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);

        task.cancel("test reason");

        assertEquals(1, callCount.get());
    }

    public void testListenerFiresImmediatelyIfAlreadyCancelled() {
        AnalyticsShardTask task = createTask();
        task.cancel("already cancelled");

        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);

        assertEquals(1, callCount.get());
    }

    public void testClearListenerPreventsCallback() {
        AnalyticsShardTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);
        task.clearCancellationListener();

        task.cancel("test reason");

        assertEquals(0, callCount.get());
    }

    public void testNullListenerDoesNotThrowOnCancellation() {
        AnalyticsShardTask task = createTask();
        task.cancel("test reason");
    }

    public void testSetListenerReplacesExisting() {
        AnalyticsShardTask task = createTask();
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();

        task.setCancellationListener(first::incrementAndGet);
        task.setCancellationListener(second::incrementAndGet);

        task.cancel("test reason");

        assertEquals(0, first.get());
        assertEquals(1, second.get());
    }
}
