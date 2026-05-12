/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class TaskTrackerTests extends OpenSearchTestCase {

    public void testAllTasksTerminalForStageReturnsTrueWhenEmpty() {
        TaskTracker tracker = new TaskTracker();
        // No tasks registered for the stage — vacuously true. Scheduler uses this when
        // a stage resolves to zero targets (empty SearchShardsResponse).
        assertTrue(tracker.allTasksTerminalForStage(0));
    }

    public void testAllTasksTerminalForStageFalseWhileAnyRunning() {
        TaskTracker tracker = new TaskTracker();
        tracker.register(task(0, 0));
        StageTask t1 = task(0, 1);
        tracker.register(t1);
        t1.transitionTo(StageTaskState.RUNNING);

        assertFalse(tracker.allTasksTerminalForStage(0));
    }

    public void testAllTasksTerminalForStageTrueWhenEveryTaskFinished() {
        TaskTracker tracker = new TaskTracker();
        StageTask t0 = task(0, 0);
        StageTask t1 = task(0, 1);
        tracker.register(t0);
        tracker.register(t1);
        t0.transitionTo(StageTaskState.RUNNING);
        t1.transitionTo(StageTaskState.RUNNING);
        t0.transitionTo(StageTaskState.FINISHED);
        t1.transitionTo(StageTaskState.FINISHED);

        assertTrue(tracker.allTasksTerminalForStage(0));
    }

    public void testAllTasksTerminalForStageTrueWithMixedTerminals() {
        // Stage is considered terminal as soon as every task is in SOME terminal state —
        // mixed FINISHED/FAILED/CANCELLED all count. Scheduler needs this to drive
        // stage-state derivation: the stage itself will then decide success vs failure.
        TaskTracker tracker = new TaskTracker();
        StageTask t0 = task(0, 0);
        StageTask t1 = task(0, 1);
        StageTask t2 = task(0, 2);
        tracker.register(t0);
        tracker.register(t1);
        tracker.register(t2);
        t0.transitionTo(StageTaskState.RUNNING);
        t0.transitionTo(StageTaskState.FINISHED);
        t1.transitionTo(StageTaskState.RUNNING);
        t1.transitionTo(StageTaskState.FAILED);
        t2.transitionTo(StageTaskState.CANCELLED);

        assertTrue(tracker.allTasksTerminalForStage(0));
    }

    public void testTasksForStageOnlyReturnsThatStage() {
        TaskTracker tracker = new TaskTracker();
        tracker.register(task(0, 0));
        tracker.register(task(0, 1));
        tracker.register(task(1, 0));

        assertEquals(2, tracker.tasksForStage(0).size());
        assertEquals(1, tracker.tasksForStage(1).size());
    }

    public void testStageTaskTransitionToTerminalIsFinal() {
        StageTask t = task(0, 0);
        assertTrue(t.transitionTo(StageTaskState.RUNNING));
        assertTrue(t.transitionTo(StageTaskState.FINISHED));
        assertFalse("terminal state must not be overwritten", t.transitionTo(StageTaskState.FAILED));
        assertEquals(StageTaskState.FINISHED, t.state());
    }

    public void testStageTaskStampsStartAndEndTimesOnTransition() {
        StageTask t = task(0, 0);
        assertEquals("start not yet stamped before transition to RUNNING", 0L, t.startedAtMs());
        assertEquals("end not yet stamped before terminal transition", 0L, t.finishedAtMs());

        t.transitionTo(StageTaskState.RUNNING);
        long start = t.startedAtMs();
        assertTrue("start stamped on RUNNING", start > 0);

        t.transitionTo(StageTaskState.FINISHED);
        long end = t.finishedAtMs();
        assertTrue("end stamped on terminal", end > 0);
        assertTrue("end must be >= start", end >= start);
    }

    public void testStageTaskDoubleTerminalKeepsFirstEndTime() {
        // Late onFailure after a successful isLast=true must not rewrite the end stamp.
        StageTask t = task(0, 0);
        t.transitionTo(StageTaskState.RUNNING);
        t.transitionTo(StageTaskState.FINISHED);
        long firstEnd = t.finishedAtMs();
        // Spin briefly so System.currentTimeMillis() would advance.
        try { Thread.sleep(2); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        assertFalse(t.transitionTo(StageTaskState.FAILED));
        assertEquals("end stamp must not rewrite on rejected transition", firstEnd, t.finishedAtMs());
    }

    private static StageTask task(int stageId, int partitionId) {
        return new StageTask(new StageTaskId(stageId, partitionId), new TestTarget());
    }

    private static final class TestTarget extends ExecutionTarget {
        TestTarget() {
            super(mock(DiscoveryNode.class));
        }
    }
}
