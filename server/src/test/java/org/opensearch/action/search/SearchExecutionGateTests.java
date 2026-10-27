/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class SearchExecutionGateTests extends OpenSearchTestCase {

    private final SearchTask task = new SearchTask(
        1,
        "transport",
        SearchAction.NAME,
        () -> "test",
        TaskId.EMPTY_TASK_ID,
        Collections.emptyMap()
    );
    private final SearchRequest request = new SearchRequest("index");

    public void testInactiveGateRunsTheRequestDirectly() {
        SearchAdmission inactive = new SearchAdmission() {
            @Override
            public boolean isActive() {
                return false;
            }

            @Override
            public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
                throw new AssertionError("an inactive admission must not be consulted");
            }
        };
        ActionListener<String> listener = ActionListener.wrap(r -> {}, e -> {});
        AtomicReference<ActionListener<String>> received = new AtomicReference<>();
        execute(inactive, task, request, listener, received::set);
        assertSame("a request run directly gets the caller's listener", listener, received.get());

        // Run directly, the request's exceptions reach the caller as if there were no gate.
        IllegalStateException boom = new IllegalStateException("boom");
        assertSame(boom, expectThrows(IllegalStateException.class, () -> execute(inactive, task, request, listener, l -> { throw boom; })));
    }

    public void testNoopGateRunsEveryRequestDirectly() {
        ActionListener<String> listener = ActionListener.wrap(r -> {}, e -> {});
        AtomicReference<ActionListener<String>> received = new AtomicReference<>();
        SearchExecutionGate.NOOP.execute(task, request, listener, received::set);
        assertSame("a request run directly gets the caller's listener", listener, received.get());
    }

    public void testRequiresAnAdmission() {
        expectThrows(NullPointerException.class, () -> new SearchExecutionGate(null));
    }

    public void testReleasesPermitBeforeNotifyingSuccess() {
        AtomicInteger releases = new AtomicInteger();
        AtomicInteger releasesWhenNotified = new AtomicInteger(-1);
        AtomicReference<String> response = new AtomicReference<>();
        execute(grantingCountedPermit(releases), task, request, ActionListener.<String>wrap(r -> {
            releasesWhenNotified.set(releases.get());
            response.set(r);
        }, e -> fail("must not fail")), l -> {
            assertEquals("the permit is held while the request runs", 0, releases.get());
            l.onResponse("done");
        });
        assertEquals("done", response.get());
        assertEquals("the permit must be released before the response is delivered", 1, releasesWhenNotified.get());
        assertEquals("the permit must be released exactly once", 1, releases.get());
    }

    public void testReleasesPermitBeforeNotifyingFailure() {
        AtomicInteger releases = new AtomicInteger();
        AtomicInteger releasesWhenNotified = new AtomicInteger(-1);
        IllegalStateException boom = new IllegalStateException("boom");
        AtomicReference<Exception> failure = new AtomicReference<>();
        execute(grantingCountedPermit(releases), task, request, ActionListener.<String>wrap(r -> fail("must not succeed"), e -> {
            releasesWhenNotified.set(releases.get());
            failure.set(e);
        }), l -> l.onFailure(boom));
        assertSame(boom, failure.get());
        assertEquals("the permit must be released before the failure is delivered", 1, releasesWhenNotified.get());
        assertEquals("the permit must be released exactly once", 1, releases.get());
    }

    public void testRefusedAdmissionFailsWithoutRunningTheRequest() {
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("throttled");
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        execute(
            (t, r, l) -> l.onFailure(rejection),
            task,
            request,
            ActionListener.<String>wrap(
                r -> fail("must not succeed"),
                e -> assertTrue("notified more than once", failure.compareAndSet(null, e))
            ),
            l -> ran.set(true)
        );
        assertFalse("a refused request must not run", ran.get());
        assertSame("the refusal reaches the caller unchanged", rejection, failure.get());
    }

    public void testRunsWhenAsynchronousAdmissionCompletes() {
        AtomicReference<ActionListener<Releasable>> pendingAdmission = new AtomicReference<>();
        AtomicReference<ActionListener<String>> admittedListener = new AtomicReference<>();
        AtomicReference<String> response = new AtomicReference<>();
        execute((t, r, l) -> pendingAdmission.set(l), task, request, ActionListener.<String>wrap(response::set, e -> {
            throw new AssertionError("unexpected failure", e);
        }), admittedListener::set);
        assertNull("the request must wait for admission", admittedListener.get());

        AtomicInteger releases = new AtomicInteger();
        pendingAdmission.get().onResponse(releases::incrementAndGet);
        assertNotNull(admittedListener.get());
        assertEquals(0, releases.get());
        admittedListener.get().onResponse("done");
        assertEquals("done", response.get());
        assertEquals(1, releases.get());
    }

    public void testReleasesAndFailsWhenTheRequestThrows() {
        AtomicInteger releases = new AtomicInteger();
        IllegalStateException boom = new IllegalStateException("boom");
        AtomicInteger releasesWhenNotified = new AtomicInteger(-1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        execute(grantingCountedPermit(releases), task, request, ActionListener.<String>wrap(r -> fail("must not succeed"), e -> {
            assertTrue("notified more than once", failure.compareAndSet(null, e));
            releasesWhenNotified.set(releases.get());
        }), l -> { throw boom; });
        assertSame(boom, failure.get());
        assertEquals("the permit must be released before the failure is delivered", 1, releasesWhenNotified.get());
        assertEquals("the permit must be released exactly once", 1, releases.get());
    }

    public void testNotifiesAndReleasesOnceWhenTheRequestThrowsAfterCompleting() {
        AtomicInteger releases = new AtomicInteger();
        AtomicInteger responses = new AtomicInteger();
        execute(
            grantingCountedPermit(releases),
            task,
            request,
            ActionListener.<String>wrap(r -> responses.incrementAndGet(), e -> fail("the completed request must not also fail")),
            l -> {
                l.onResponse("done");
                throw new IllegalStateException("boom");
            }
        );
        assertEquals(1, responses.get());
        assertEquals("the permit must be released exactly once", 1, releases.get());
    }

    public void testDeliversUnexpectedAdmissionException() {
        IllegalStateException boom = new IllegalStateException("boom");
        AtomicBoolean ran = new AtomicBoolean();
        AtomicReference<Exception> failure = new AtomicReference<>();
        execute(
            (t, r, l) -> { throw boom; },
            task,
            request,
            ActionListener.<String>wrap(
                r -> fail("must not succeed"),
                e -> assertTrue("notified more than once", failure.compareAndSet(null, e))
            ),
            l -> ran.set(true)
        );
        assertFalse(ran.get());
        assertSame(boom, failure.get());
    }

    public void testFailedReleaseDoesNotFailTheRequest() {
        AtomicReference<String> response = new AtomicReference<>();
        execute(
            (t, r, l) -> l.onResponse(() -> { throw new IllegalStateException("release failed"); }),
            task,
            request,
            ActionListener.<String>wrap(response::set, e -> {
                throw new AssertionError("unexpected failure", e);
            }),
            l -> l.onResponse("done")
        );
        assertEquals("done", response.get());
    }

    // An admission that admits inline with a permit counting its releases.
    private static SearchAdmission grantingCountedPermit(AtomicInteger releases) {
        return (t, r, l) -> l.onResponse(releases::incrementAndGet);
    }

    // Runs body through a gate built on admission.
    private static <T> void execute(
        SearchAdmission admission,
        Task task,
        ActionRequest request,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> body
    ) {
        new SearchExecutionGate(admission).execute(task, request, listener, body);
    }
}
