/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.stage.coordinator.LocalStageTask;
import org.opensearch.analytics.exec.stage.shard.ShardStageTask;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests the sealed {@link StageTask} hierarchy: a shared state machine plus per-variant payload.
 * {@link ShardStageTask} carries an {@link ExecutionTarget}; {@link LocalStageTask} carries a body
 * {@link java.util.function.Consumer} for coordinator-local execution.
 */
public class StageTaskTests extends OpenSearchTestCase {

    public void testShardStageTaskCarriesExecutionTarget() {
        ExecutionTarget target = newShardTarget();
        ShardStageTask task = new ShardStageTask(new StageTaskId(7, 3), target);
        assertSame(target, task.target());
        assertEquals(new StageTaskId(7, 3), task.id());
        assertEquals(StageTaskState.CREATED, task.state());
    }

    public void testLocalStageTaskCarriesListenerConsumerBody() {
        AtomicBoolean ran = new AtomicBoolean(false);
        java.util.function.Consumer<org.opensearch.core.action.ActionListener<Void>> body = listener -> {
            ran.set(true);
            listener.onResponse(null);
        };
        LocalStageTask task = new LocalStageTask(new StageTaskId(9, 0), body);
        assertSame(body, task.body());
        assertEquals(new StageTaskId(9, 0), task.id());
        assertEquals(StageTaskState.CREATED, task.state());
        // body is not invoked just by holding the task — runner's job
        assertFalse(ran.get());
    }

    public void testStateMachineSharedAcrossVariants() {
        for (StageTask task : new StageTask[] {
            new ShardStageTask(new StageTaskId(0, 0), newShardTarget()),
            new LocalStageTask(new StageTaskId(0, 0), l -> l.onResponse(null)) }) {
            assertTrue(task.transitionTo(StageTaskState.RUNNING));
            assertTrue("startedAtMs stamped on RUNNING", task.startedAtMs() > 0);
            assertEquals(0L, task.finishedAtMs());

            assertTrue(task.transitionTo(StageTaskState.FINISHED));
            assertTrue("finishedAtMs stamped on terminal", task.finishedAtMs() > 0);

            assertFalse("terminal is sticky", task.transitionTo(StageTaskState.FAILED));
            assertEquals(StageTaskState.FINISHED, task.state());
        }
    }

    // ── SKIPPED: a terminal that means "never sent, and that was correct" ──

    /**
     * {@code SKIPPED} must be terminal and reachable straight from CREATED — a skipped task is never
     * dispatched, so a non-terminal SKIPPED would leave the stage RUNNING forever.
     */
    public void testSkippedIsTerminalFromCreated() {
        StageTask task = new ShardStageTask(new StageTaskId(0, 0), newShardTarget());

        assertTrue(StageTaskState.SKIPPED.isTerminal());
        assertTrue(task.transitionTo(StageTaskState.SKIPPED));
        assertEquals(StageTaskState.SKIPPED, task.state());
        assertTrue("finishedAtMs stamped like any other terminal", task.finishedAtMs() > 0);
    }

    /**
     * Only the first terminal claims the task; later attempts return false so the caller doesn't
     * decrement the stage counter twice. The real race is a cancel sweep against a dispatch-side skip.
     */
    public void testOnlyTheFirstTerminalClaimsTheTask() {
        StageTask skippedFirst = new ShardStageTask(new StageTaskId(0, 0), newShardTarget());
        assertTrue(skippedFirst.transitionTo(StageTaskState.SKIPPED));
        assertFalse("no transition back out", skippedFirst.transitionTo(StageTaskState.RUNNING));
        assertFalse("nor to another terminal", skippedFirst.transitionTo(StageTaskState.CANCELLED));
        assertFalse("nor a repeat of the same one", skippedFirst.transitionTo(StageTaskState.SKIPPED));
        assertEquals(StageTaskState.SKIPPED, skippedFirst.state());

        StageTask cancelledFirst = new ShardStageTask(new StageTaskId(0, 1), newShardTarget());
        assertTrue(cancelledFirst.transitionTo(StageTaskState.CANCELLED));
        assertFalse("cancel already settled it — the skip must not settle it again", cancelledFirst.transitionTo(StageTaskState.SKIPPED));
        assertEquals(StageTaskState.CANCELLED, cancelledFirst.state());
    }

    private static ExecutionTarget newShardTarget() {
        return new ExecutionTarget(null) {
        };
    }
}
