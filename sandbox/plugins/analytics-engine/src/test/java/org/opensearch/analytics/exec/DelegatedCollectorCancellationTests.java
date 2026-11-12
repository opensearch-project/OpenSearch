/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.HashMap;

/**
 * Regression tests for the observed sf=10 failure where a delegated-filter fragment on an
 * ALREADY-CANCELLED query reported a hard native error instead of a clean cancellation:
 *
 * <pre>
 * RuntimeException: Execution error: External error: delegated-backend
 * collector.collect_packed_u64_bitset(rg=0, [0, 25)): collectDocs(context_id=620800, key=1) failed: -1
 * </pre>
 *
 * <p>Root cause of the -1: the per-query {@code FilterDelegationHandle} binding is unregistered when
 * the fragment tears down, but tokio's abort is cooperative — a native scan task already inside a
 * row-group prefetch keeps running to its next yield point and can issue one more {@code collectDocs}
 * upcall first. Refusing that upcall is CORRECT (there is no handle left, and the query's results are
 * being discarded). The defect is that it was reported as a 500-class execution error rather than the
 * cancellation it actually is.
 *
 * <p>These tests pin {@link AnalyticsSearchService#asCancellationIfTornDown}: re-type only when the
 * task is genuinely cancelled AND the failure carries the delegated-collector marker.
 */
public class DelegatedCollectorCancellationTests extends OpenSearchTestCase {

    /** The exact native message observed on the cluster (context_id=620800, key=1, range [0, 25)). */
    private static final String OBSERVED_NATIVE_MESSAGE = "Execution error: External error: delegated-backend "
        + "collector.collect_packed_u64_bitset(rg=0, [0, 25)): collectDocs(context_id=620800, key=1) failed: -1";

    private static AnalyticsShardTask newTask() {
        return new AnalyticsShardTask(
            1L,
            "transport",
            "indices:data/read/analytics/fragment",
            "test-fragment",
            TaskId.EMPTY_TASK_ID,
            new HashMap<>()
        );
    }

    /**
     * THE BUG. A cancelled task whose fragment failed with the delegated-collector upcall error must
     * surface as a cancellation, not as the raw native execution error.
     */
    public void testCancelledTaskWithCollectorFailureBecomesCancellation() {
        AnalyticsShardTask task = newTask();
        task.cancel("cancelled by test");
        Exception nativeFailure = new RuntimeException(OBSERVED_NATIVE_MESSAGE);

        Exception result = AnalyticsSearchService.asCancellationIfTornDown(nativeFailure, task);

        assertTrue("expected a TaskCancelledException but got " + result.getClass().getName(), result instanceof TaskCancelledException);
        assertSame("original native failure must be preserved as the cause", nativeFailure, result.getCause());
    }

    /** The marker is found through a wrapped cause chain, as it arrives from the engine. */
    public void testCollectorFailureNestedInCauseChainIsDetected() {
        AnalyticsShardTask task = newTask();
        task.cancel("cancelled by test");
        Exception wrapped = new RuntimeException(
            "Failed to start streaming fragment on [idx][0]",
            new RuntimeException(OBSERVED_NATIVE_MESSAGE)
        );

        Exception result = AnalyticsSearchService.asCancellationIfTornDown(wrapped, task);

        assertTrue(result instanceof TaskCancelledException);
    }

    /**
     * The second observed instance (range [0, 1000000), context_id=620822) must be handled by the
     * same marker, confirming the match doesn't depend on the specific ids or range.
     */
    public void testSecondObservedInstanceIsAlsoRetyped() {
        AnalyticsShardTask task = newTask();
        task.cancel("cancelled by test");
        Exception nativeFailure = new RuntimeException(
            "Execution error: External error: delegated-backend "
                + "collector.collect_packed_u64_bitset(rg=0, [0, 1000000)): collectDocs(context_id=620822, key=1) failed: -1"
        );

        assertTrue(AnalyticsSearchService.asCancellationIfTornDown(nativeFailure, task) instanceof TaskCancelledException);
    }

    /**
     * Guard against over-reach: the SAME native error on a LIVE (uncancelled) task is a real
     * delegation defect and must keep its identity so it is not silently downgraded.
     */
    public void testLiveTaskWithCollectorFailureIsUnchanged() {
        AnalyticsShardTask task = newTask();
        assertFalse(task.isCancelled());
        Exception nativeFailure = new RuntimeException(OBSERVED_NATIVE_MESSAGE);

        assertSame(nativeFailure, AnalyticsSearchService.asCancellationIfTornDown(nativeFailure, task));
    }

    /**
     * Guard against over-reach the other way: a cancelled task whose failure is unrelated to the
     * delegated collector (e.g. a real breaker trip) must NOT be relabelled as a cancellation —
     * that would mask the actionable cause.
     */
    public void testCancelledTaskWithUnrelatedFailureIsUnchanged() {
        AnalyticsShardTask task = newTask();
        task.cancel("cancelled by test");
        Exception unrelated = new IllegalStateException("Failed to allocate 1024 bytes");

        assertSame(unrelated, AnalyticsSearchService.asCancellationIfTornDown(unrelated, task));
    }

    /** A null task (no tracked task) must be a no-op rather than an NPE. */
    public void testNullTaskIsUnchanged() {
        Exception nativeFailure = new RuntimeException(OBSERVED_NATIVE_MESSAGE);
        assertSame(nativeFailure, AnalyticsSearchService.asCancellationIfTornDown(nativeFailure, null));
    }

    /**
     * End-to-end value of the re-typing: once it is a {@link TaskCancelledException}, the existing
     * transport mapping tags it {@code CANCELLED} across Flight, so the coordinator rebuilds a
     * cancellation instead of a generic retryable 500. This is the whole point of the fix.
     */
    public void testRetypedCancellationCrossesTransportAsCancelled() {
        AnalyticsShardTask task = newTask();
        task.cancel("cancelled by test");
        Exception result = AnalyticsSearchService.asCancellationIfTornDown(new RuntimeException(OBSERVED_NATIVE_MESSAGE), task);

        Exception wire = AnalyticsTransportErrors.toWireError(result);

        assertTrue("expected a StreamException but got " + wire.getClass().getName(), wire instanceof StreamException);
        assertEquals(StreamErrorCode.CANCELLED, ((StreamException) wire).getErrorCode());

        // And the coordinator rebuilds it as a cancellation, not a 500.
        assertTrue(AnalyticsTransportErrors.fromWireError(wire) instanceof TaskCancelledException);
    }
}
