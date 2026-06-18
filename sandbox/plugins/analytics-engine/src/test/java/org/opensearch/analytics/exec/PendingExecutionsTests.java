/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link PendingExecutions}' permit-based admission: at most {@code permits}
 * runnables execute concurrently; the rest queue and drain as permits are released. This is the
 * mechanism behind the per-node {@code analytics.query.max_concurrent_shard_requests_per_node}
 * throttle.
 */
public class PendingExecutionsTests extends OpenSearchTestCase {

    public void testRunsImmediatelyWhenPermitsAvailable() {
        PendingExecutions pending = new PendingExecutions(2);
        List<Integer> ran = new ArrayList<>();
        pending.tryRun(() -> ran.add(0));
        pending.tryRun(() -> ran.add(1));
        assertEquals("both runnables run while permits are available", List.of(0, 1), ran);
    }

    public void testQueuesBeyondLimitAndDrainsOnFinish() {
        PendingExecutions pending = new PendingExecutions(2);
        List<Integer> ran = new ArrayList<>();

        // Two permits; the runnables here do NOT call finishAndRunNext, so they hold their permits.
        pending.tryRun(() -> ran.add(0));
        pending.tryRun(() -> ran.add(1));
        // Third exceeds the limit → queued, not yet run.
        pending.tryRun(() -> ran.add(2));
        assertEquals("third runnable is queued, not run", List.of(0, 1), ran);

        // Release one permit → the queued runnable drains.
        pending.finishAndRunNext();
        assertEquals("queued runnable runs once a permit frees", List.of(0, 1, 2), ran);
    }

    public void testFinishWithEmptyQueueIsNoOp() {
        PendingExecutions pending = new PendingExecutions(1);
        List<Integer> ran = new ArrayList<>();
        pending.tryRun(() -> ran.add(0));
        // No work queued behind it — releasing the permit must not run anything or throw.
        pending.finishAndRunNext();
        assertEquals(List.of(0), ran);
        // A subsequent submission still runs (permit is available again).
        pending.tryRun(() -> ran.add(1));
        assertEquals(List.of(0, 1), ran);
    }

    public void testLimitOfOneSerializes() {
        PendingExecutions pending = new PendingExecutions(1);
        List<Integer> ran = new ArrayList<>();
        pending.tryRun(() -> ran.add(0));
        pending.tryRun(() -> ran.add(1)); // queued behind the held permit
        pending.tryRun(() -> ran.add(2)); // queued
        assertEquals("only the first runs at limit 1", List.of(0), ran);

        pending.finishAndRunNext();
        assertEquals(List.of(0, 1), ran);
        pending.finishAndRunNext();
        assertEquals(List.of(0, 1, 2), ran);
    }
}
