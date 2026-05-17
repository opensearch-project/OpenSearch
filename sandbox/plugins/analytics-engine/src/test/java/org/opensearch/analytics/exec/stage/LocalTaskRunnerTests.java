/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link LocalTaskRunner}. Body-runs-on-executor + signals
 * lifecycle via {@link ActionListener<Void>}.
 */
public class LocalTaskRunnerTests extends OpenSearchTestCase {

    public void testHappyPathRunsBodyAndSignalsCompleted() {
        AtomicBoolean ran = new AtomicBoolean();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();

        LocalTaskRunner dispatcher = new LocalTaskRunner(Runnable::run);
        LocalStageTask task = new LocalStageTask(new StageTaskId(0, 0), () -> ran.set(true));

        dispatcher.run(task, handle(completed, failure));

        assertTrue("body ran", ran.get());
        assertTrue("onCompleted fired", completed.get());
        assertNull("onFailed must not fire on happy path", failure.get());
    }

    public void testBodyThrowSignalsFailedWithCause() {
        RuntimeException boom = new RuntimeException("boom");
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();

        LocalTaskRunner dispatcher = new LocalTaskRunner(Runnable::run);
        LocalStageTask task = new LocalStageTask(new StageTaskId(0, 0), () -> { throw boom; });

        dispatcher.run(task, handle(completed, failure));

        assertFalse("onCompleted must not fire when body throws", completed.get());
        assertSame("onFailed receives the original exception", boom, failure.get());
    }

    public void testDispatchSubmitsToExecutor() {
        // Verify the dispatcher actually calls executor.execute — not running inline
        // when the executor is async. We use a capturing executor: instead of running
        // the runnable, we store it and run it explicitly after dispatch returns.
        AtomicReference<Runnable> submitted = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();

        LocalTaskRunner dispatcher = new LocalTaskRunner(submitted::set);
        LocalStageTask task = new LocalStageTask(new StageTaskId(0, 0), () -> {});

        dispatcher.run(task, handle(completed, failure));

        assertNotNull("body must be submitted to the executor", submitted.get());
        assertFalse("handle should not fire before the executor runs the body", completed.get());

        submitted.get().run();
        assertTrue("onCompleted fires once the executor runs the body", completed.get());
    }

    private static ActionListener<Void> handle(AtomicBoolean completed, AtomicReference<Exception> failure) {
        return new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                completed.set(true);
            }

            @Override
            public void onFailure(Exception cause) {
                failure.set(cause);
            }
        };
    }
}
