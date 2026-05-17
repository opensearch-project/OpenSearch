/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests the sealed {@link StageTask} hierarchy: shared state-machine + per-variant payload.
 * {@link ShardStageTask} carries an {@link ExecutionTarget} (routing key for Flight dispatch);
 * {@link LocalStageTask} carries a {@link Runnable} body for coordinator-local execution.
 * Neither variant has a "fake" target for the other's payload.
 */
public class StageTaskTests extends OpenSearchTestCase {

    public void testShardStageTaskCarriesExecutionTarget() {
        ExecutionTarget target = newShardTarget();
        ShardStageTask task = new ShardStageTask(new StageTaskId(7, 3), target);
        assertSame(target, task.target());
        assertEquals(new StageTaskId(7, 3), task.id());
        assertEquals(StageTaskState.CREATED, task.state());
    }

    public void testLocalStageTaskCarriesRunnableBody() {
        AtomicBoolean ran = new AtomicBoolean(false);
        Runnable body = () -> ran.set(true);
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
            new LocalStageTask(new StageTaskId(0, 0), () -> {}) }) {
            assertTrue(task.transitionTo(StageTaskState.RUNNING));
            assertTrue("startedAtMs stamped on RUNNING", task.startedAtMs() > 0);
            assertEquals(0L, task.finishedAtMs());

            assertTrue(task.transitionTo(StageTaskState.FINISHED));
            assertTrue("finishedAtMs stamped on terminal", task.finishedAtMs() > 0);

            assertFalse("terminal is sticky", task.transitionTo(StageTaskState.FAILED));
            assertEquals(StageTaskState.FINISHED, task.state());
        }
    }

    private static ExecutionTarget newShardTarget() {
        return new ExecutionTarget(null) {
        };
    }
}
