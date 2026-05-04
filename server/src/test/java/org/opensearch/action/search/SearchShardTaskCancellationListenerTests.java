/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class SearchShardTaskCancellationListenerTests extends OpenSearchTestCase {

    private SearchShardTask createTask() {
        return new SearchShardTask(1L, "type", "action", "description", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    public void testListenerFiresOnCancellation() {
        SearchShardTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);

        task.cancel("test reason");

        assertEquals(1, callCount.get());
    }

    public void testListenerFiresImmediatelyIfAlreadyCancelled() {
        SearchShardTask task = createTask();
        task.cancel("already cancelled");

        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);

        assertEquals(1, callCount.get());
    }

    public void testClearListenerPreventsCallback() {
        SearchShardTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);
        task.clearCancellationListener();

        task.cancel("test reason");

        assertEquals(0, callCount.get());
    }

    public void testNullListenerDoesNotThrowOnCancellation() {
        SearchShardTask task = createTask();
        // No listener set — should not throw
        task.cancel("test reason");
    }

    public void testSetListenerReplacesExisting() {
        SearchShardTask task = createTask();
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();

        task.setCancellationListener(first::incrementAndGet);
        task.setCancellationListener(second::incrementAndGet);

        task.cancel("test reason");

        assertEquals(0, first.get());
        assertEquals(1, second.get());
    }
}
