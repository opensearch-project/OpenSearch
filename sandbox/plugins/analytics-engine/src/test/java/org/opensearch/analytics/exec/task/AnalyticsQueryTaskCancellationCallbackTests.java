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

    public void testSecondSetReplacesPreviousCallback() {
        // Replace-semantics is load-bearing: multi-phase drivers (UnifiedDispatch's broadcast-capture →
        // residual-dispatch handoff, QueryScheduler) install a temporary callback for phase 1, then replace
        // it when phase 2 begins so cancel routes to the active phase's walker. The second set must win and
        // the first must NOT fire on cancel.
        AnalyticsQueryTask task = createTask();
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        task.setOnCancelCallback(first::incrementAndGet);
        task.setOnCancelCallback(second::incrementAndGet);

        task.cancel("after replace");

        assertEquals("replaced (first) callback must not fire", 0, first.get());
        assertEquals("active (second) callback fires exactly once", 1, second.get());
    }
}
