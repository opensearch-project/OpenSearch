/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.support.ListenerTimeouts;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.node.Node.NODE_NAME_SETTING;

public class RepositoryOperationGuardTests extends OpenSearchTestCase {

    private static final String REPO = "test-repo";

    private DeterministicTaskQueue taskQueue;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());
    }

    // Acquisitions succeed while under the limit and fail fast with no permit taken once the limit is reached.
    public void testTryAcquireSucceedsUnderLimitAndFailsFastAtLimit() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(2);
        guard.tryAcquire(REPO);
        assertEquals(1, guard.getOutstandingOps(REPO));
        guard.tryAcquire(REPO);
        assertEquals(2, guard.getOutstandingOps(REPO));

        final RepositoryException e = expectThrows(RepositoryException.class, () -> guard.tryAcquire(REPO));
        assertTrue(e.getMessage().contains("at the limit of"));
        assertEquals(2, guard.getOutstandingOps(REPO)); // rejected attempt must not increment the count
    }

    // A released permit frees up capacity for a subsequent acquisition.
    public void testReleaseOnResponseBalancesAcquire() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        guard.tryAcquire(REPO);
        assertEquals(1, guard.getOutstandingOps(REPO));
        guard.release(REPO);
        assertEquals(0, guard.getOutstandingOps(REPO));
        guard.tryAcquire(REPO);
        assertEquals(1, guard.getOutstandingOps(REPO));
    }

    // Releasing without a matching acquire is a caller bug: it trips the assertion under -ea
    // and never drives the count negative (which in prod would silently grant extra capacity)
    public void testReleaseWithoutAcquireIsGuarded() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        expectThrows(AssertionError.class, () -> guard.release(REPO));
        assertEquals(0, guard.getOutstandingOps(REPO));
    }

    // Release fires exactly once on response, even if the underlying listener is (incorrectly) notified twice.
    public void testReleaseRunsExactlyOnceOnResponseEvenWithLateDuplicateCompletion() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        guard.tryAcquire(REPO);
        final AtomicInteger releaseCount = new AtomicInteger(0);
        final ActionListener<String> business = ActionListener.wrap(r -> {}, e -> {});
        final ActionListener<String> released = ActionListener.runAfter(business, () -> {
            releaseCount.incrementAndGet();
            guard.release(REPO);
        });
        final ActionListener<String> timed = ListenerTimeouts.wrapWithTimeout(
            taskQueue.getThreadPool(),
            released,
            TimeValue.timeValueMinutes(1),
            ThreadPool.Names.GENERIC,
            "test"
        );

        timed.onResponse("ok");
        timed.onResponse("late-duplicate");
        timed.onFailure(new RuntimeException("late-duplicate"));

        assertEquals(1, releaseCount.get());
        assertEquals(0, guard.getOutstandingOps(REPO));

        // Pending timeout task must be a no-op now that the listener has already resolved.
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertEquals(1, releaseCount.get());
    }

    // Release fires exactly once on failure, even if a later response also reaches the listener.
    public void testReleaseRunsExactlyOnceOnFailureEvenWithLateDuplicateCompletion() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        guard.tryAcquire(REPO);
        final AtomicInteger releaseCount = new AtomicInteger(0);
        final ActionListener<String> business = ActionListener.wrap(r -> {}, e -> {});
        final ActionListener<String> released = ActionListener.runAfter(business, () -> {
            releaseCount.incrementAndGet();
            guard.release(REPO);
        });
        final ActionListener<String> timed = ListenerTimeouts.wrapWithTimeout(
            taskQueue.getThreadPool(),
            released,
            TimeValue.timeValueMinutes(1),
            ThreadPool.Names.GENERIC,
            "test"
        );

        timed.onFailure(new RuntimeException("boom"));
        timed.onResponse("late-duplicate");

        assertEquals(1, releaseCount.get());
        assertEquals(0, guard.getOutstandingOps(REPO));

        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertEquals(1, releaseCount.get());
    }

    // The sharpest correctness case: a timeout releases the permit, and the orphaned worker's late completion
    // must be dropped by ListenerTimeouts rather than double-releasing.
    public void testReleaseRunsExactlyOnceOnTimeoutAndOrphanedLateCompletionDoesNotDoubleRelease() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        guard.tryAcquire(REPO);
        final AtomicInteger releaseCount = new AtomicInteger(0);
        final AtomicReference<Exception> observedFailure = new AtomicReference<>();
        final ActionListener<String> business = ActionListener.wrap(r -> {}, observedFailure::set);
        final ActionListener<String> released = ActionListener.runAfter(business, () -> {
            releaseCount.incrementAndGet();
            guard.release(REPO);
        });
        final ActionListener<String> timed = ListenerTimeouts.wrapWithTimeout(
            taskQueue.getThreadPool(),
            released,
            TimeValue.timeValueMillis(10),
            ThreadPool.Names.GENERIC,
            "test"
        );

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertTrue(observedFailure.get() instanceof OpenSearchTimeoutException);
        assertEquals(1, releaseCount.get());
        assertEquals(0, guard.getOutstandingOps(REPO));

        // Simulates the orphaned worker thread finally completing after the timeout already fired.
        timed.onResponse("late-orphaned-completion");
        assertEquals(1, releaseCount.get());
        assertEquals(0, guard.getOutstandingOps(REPO));
    }

    // Raising or lowering the limit at runtime takes effect on the next acquisition attempt.
    public void testDynamicLimitUpdateAppliesToSubsequentAcquisitions() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        guard.tryAcquire(REPO);
        expectThrows(RepositoryException.class, () -> guard.tryAcquire(REPO));

        guard.setMaxOutstandingOps(2);
        assertEquals(2, guard.getMaxOutstandingOps());
        guard.tryAcquire(REPO);
        assertEquals(2, guard.getOutstandingOps(REPO));

        guard.setMaxOutstandingOps(1);
        expectThrows(RepositoryException.class, () -> guard.tryAcquire(REPO));
    }

    // Zero or negative limits are rejected both at construction and via a dynamic update.
    public void testRejectsNonPositiveLimit() {
        expectThrows(IllegalArgumentException.class, () -> new RepositoryOperationGuard(0));
        expectThrows(IllegalArgumentException.class, () -> new RepositoryOperationGuard(-1));
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(4);
        expectThrows(IllegalArgumentException.class, () -> guard.setMaxOutstandingOps(0));
        expectThrows(IllegalArgumentException.class, () -> guard.setMaxOutstandingOps(-1));
    }

    // The constructor argument is the initial limit exposed via the getter. 7 is arbitrary — chosen only to be
    // distinct from the production default (4) and other values used elsewhere in this suite.
    public void testConstructorSetsInitialLimit() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(7);
        assertEquals(7, guard.getMaxOutstandingOps());
    }

    // Each repository has its own independent limit and count; one repo hitting its limit does not affect another.
    public void testTracksMultipleRepositoriesIndependently() {
        final RepositoryOperationGuard guard = new RepositoryOperationGuard(1);
        guard.tryAcquire("repo-a");
        guard.tryAcquire("repo-b");
        assertEquals(1, guard.getOutstandingOps("repo-a"));
        assertEquals(1, guard.getOutstandingOps("repo-b"));
        expectThrows(RepositoryException.class, () -> guard.tryAcquire("repo-a"));

        guard.release("repo-a");
        assertEquals(0, guard.getOutstandingOps("repo-a"));
        assertEquals(1, guard.getOutstandingOps("repo-b"));
    }
}
