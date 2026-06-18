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

public class AnalyticsQueryTaskCancellationCallbackTests extends OpenSearchTestCase {

    private AnalyticsQueryTask createTask() {
        return new AnalyticsQueryTask(1L, "type", "action", "queryId", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    public void testCallbackFiresOnCancellation() {
        AnalyticsQueryTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setOnCancelCallback(callCount::incrementAndGet);

        task.cancel("test reason");

        assertEquals(1, callCount.get());
    }

    public void testCallbackFiresImmediatelyIfAlreadyCancelled() {
        AnalyticsQueryTask task = createTask();
        task.cancel("cancel before install");

        AtomicInteger callCount = new AtomicInteger();
        task.setOnCancelCallback(callCount::incrementAndGet);

        assertEquals("callback must fire when installed after cancel", 1, callCount.get());
    }

    public void testCallbackFiresExactlyOnce() {
        AnalyticsQueryTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setOnCancelCallback(callCount::incrementAndGet);

        task.cancel("first");
        task.cancel("second");

        assertEquals(1, callCount.get());
    }

    public void testNullCallbackDoesNotThrowOnCancellation() {
        AnalyticsQueryTask task = createTask();
        task.cancel("no callback installed");
    }

    public void testSecondSetThrows() {
        AnalyticsQueryTask task = createTask();
        task.setOnCancelCallback(() -> {});
        expectThrows(IllegalStateException.class, () -> task.setOnCancelCallback(() -> {}));
    }
}
