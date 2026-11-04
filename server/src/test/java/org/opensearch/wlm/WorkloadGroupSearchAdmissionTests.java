/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchAdmission;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class WorkloadGroupSearchAdmissionTests extends OpenSearchTestCase {

    private static final long PARENT_ID = 7;
    private static final String WORKLOAD_GROUP_ID = "wg-1";

    private WorkloadGroupService workloadGroupService;
    private TaskManager taskManager;
    private WorkloadGroupSearchAdmission admission;
    // The arguments of the last acquireThrottlePermit call.
    private final AtomicReference<BooleanSupplier> parentAlreadyCounted = new AtomicReference<>();
    private final AtomicReference<ActionListener<Releasable>> serviceListener = new AtomicReference<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        workloadGroupService = mock(WorkloadGroupService.class);
        doAnswer(invocation -> {
            parentAlreadyCounted.set(invocation.getArgument(1));
            serviceListener.set(invocation.getArgument(2));
            return null;
        }).when(workloadGroupService).acquireThrottlePermit(any(), any(), any());
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(
            new DiscoveryNode("local", buildNewFakeTransportAddress(), Collections.emptyMap(), Collections.emptySet(), Version.CURRENT)
        );
        taskManager = mock(TaskManager.class);
        // Both policies active and applying to the group unless a test says otherwise.
        when(workloadGroupService.isResourceLimitCheckActive()).thenReturn(true);
        when(workloadGroupService.isThrottlingActive()).thenReturn(true);
        when(workloadGroupService.enforcesResourceLimits(any())).thenReturn(true);
        when(workloadGroupService.configuresThrottling(any())).thenReturn(true);
        admission = new WorkloadGroupSearchAdmission(workloadGroupService, clusterService, taskManager);
    }

    public void testIsActiveWhileAnyPolicyIsActive() {
        assertTrue(admission.isActive());
        when(workloadGroupService.isThrottlingActive()).thenReturn(false);
        assertTrue("a group without throttling can still reject on resource limits", admission.isActive());
        when(workloadGroupService.isResourceLimitCheckActive()).thenReturn(false);
        assertFalse(admission.isActive());
        when(workloadGroupService.isThrottlingActive()).thenReturn(true);
        assertTrue(admission.isActive());
    }

    public void testInactiveThrottleIsSkipped() {
        when(workloadGroupService.isThrottlingActive()).thenReturn(false);
        assertSame(SearchAdmission.NOOP_PERMIT, admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest()).get());
        verify(workloadGroupService).rejectIfNeeded(WORKLOAD_GROUP_ID);
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
    }

    public void testInactiveResourceLimitCheckIsSkipped() {
        when(workloadGroupService.isResourceLimitCheckActive()).thenReturn(false);
        admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest());
        verify(workloadGroupService, never()).rejectIfNeeded(any());
        verify(workloadGroupService).acquireThrottlePermit(any(), any(), any());
    }

    public void testPoliciesSkipGroupsTheyDoNotApplyTo() {
        when(workloadGroupService.enforcesResourceLimits(WORKLOAD_GROUP_ID)).thenReturn(false);
        when(workloadGroupService.configuresThrottling(WORKLOAD_GROUP_ID)).thenReturn(false);
        assertSame(SearchAdmission.NOOP_PERMIT, admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest()).get());
        verify(workloadGroupService, never()).rejectIfNeeded(any());
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
    }

    public void testNestedSearchUnderACountedParentIsStillCheckedAgainstResourceLimits() {
        SearchTask parent = searchTask(TaskId.EMPTY_TASK_ID);
        parent.setThrottleCounted(true);
        when(taskManager.getTask(PARENT_ID)).thenReturn(parent);
        doThrow(new OpenSearchRejectedExecutionException("contended")).when(workloadGroupService).rejectIfNeeded(WORKLOAD_GROUP_ID);
        WorkloadGroupTask nested = searchTask(new TaskId("local", PARENT_ID));
        AtomicReference<Exception> failure = new AtomicReference<>();
        admission.admit(nested, new SearchRequest(), ActionListener.wrap(p -> fail("must not admit"), failure::set));
        // Inheriting the parent's throttle charge does not exempt a nested search from the resource-limit check.
        assertThat(failure.get(), instanceOf(OpenSearchRejectedExecutionException.class));
        assertTrue(nested.isAdmissionRejected());
    }

    public void testAsyncGrantThenRefusalReleasesThePermitAndFailsOnce() {
        AtomicReference<ActionListener<Releasable>> firstPolicyListener = new AtomicReference<>();
        WorkloadGroupAdmissionPolicy grantsLater = policy((task, request, listener) -> firstPolicyListener.set(listener));
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("refused");
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicInteger failures = new AtomicInteger();
        new WorkloadGroupSearchAdmission(List.of(grantsLater, refusingPolicy(rejection))).admit(
            task,
            new SearchRequest(),
            ActionListener.wrap(p -> fail("must not admit"), e -> failures.incrementAndGet())
        );
        assertEquals("nothing completes until the first policy answers", 0, failures.get());

        AtomicBoolean released = new AtomicBoolean();
        firstPolicyListener.get().onResponse(() -> released.set(true));
        assertTrue("the permit granted before the refusal must be released", released.get());
        assertEquals(1, failures.get());
        assertTrue(task.isAdmissionRejected());
    }

    public void testPolicyThatThrowsBeforeCompletingRefusesTheRequest() {
        AtomicBoolean released = new AtomicBoolean();
        IllegalStateException boom = new IllegalStateException("boom");
        WorkloadGroupAdmissionPolicy throwing = policy((task, request, listener) -> { throw boom; });
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Exception> failure = new AtomicReference<>();
        new WorkloadGroupSearchAdmission(List.of(grantingPolicy(() -> released.set(true)), throwing)).admit(
            task,
            new SearchRequest(),
            ActionListener.wrap(p -> fail("must not admit"), failure::set)
        );
        assertSame(boom, failure.get());
        assertTrue(released.get());
        assertTrue(task.isAdmissionRejected());
    }

    public void testThrowFromTheAdmittedRequestIsNotARefusal() {
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        IllegalStateException boom = new IllegalStateException("boom");
        WorkloadGroupSearchAdmission onePolicy = new WorkloadGroupSearchAdmission(List.of(grantingPolicy(() -> {})));
        expectThrows(IllegalStateException.class, () -> onePolicy.admit(task, new SearchRequest(), new ActionListener<>() {
            @Override
            public void onResponse(Releasable permit) {
                throw boom;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("an admitted request must not be failed by admission", e);
            }
        }));
        assertFalse(task.isAdmissionRejected());
    }

    public void testPermitsFromSeveralPoliciesAreReleasedTogether() {
        AtomicInteger released = new AtomicInteger();
        WorkloadGroupSearchAdmission twoPolicies = new WorkloadGroupSearchAdmission(
            List.of(grantingPolicy(released::incrementAndGet), grantingPolicy(released::incrementAndGet))
        );
        AtomicReference<Releasable> permit = new AtomicReference<>();
        twoPolicies.admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest(), ActionListener.wrap(permit::set, e -> fail()));
        assertEquals("nothing is released until the request completes", 0, released.get());
        permit.get().close();
        assertEquals(2, released.get());
    }

    public void testFailureReportedAfterAdmittingIsIgnored() {
        // Like a policy whose own wrapping reports a throw from the admitted request's listener as a failure.
        WorkloadGroupAdmissionPolicy failsAfterResponding = policy((task, request, listener) -> {
            try {
                listener.onResponse(SearchAdmission.NOOP_PERMIT);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicBoolean failed = new AtomicBoolean();
        new WorkloadGroupSearchAdmission(List.of(failsAfterResponding)).admit(task, new SearchRequest(), new ActionListener<>() {
            @Override
            public void onResponse(Releasable permit) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void onFailure(Exception e) {
                failed.set(true);
            }
        });
        assertFalse("an admitted request must not also be failed", failed.get());
        assertFalse(task.isAdmissionRejected());
    }

    public void testPolicyThatFailsToDecideIsSkipped() {
        WorkloadGroupAdmissionPolicy failsToDecide = new WorkloadGroupAdmissionPolicy() {
            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public boolean appliesTo(WorkloadGroupTask task, ActionRequest request) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void admit(WorkloadGroupTask task, ActionRequest request, ActionListener<Releasable> listener) {
                throw new AssertionError("a policy that failed to decide must not run");
            }
        };
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Releasable> permit = new AtomicReference<>();
        new WorkloadGroupSearchAdmission(List.of(failsToDecide)).admit(
            task,
            new SearchRequest(),
            ActionListener.wrap(permit::set, e -> fail())
        );
        assertSame(SearchAdmission.NOOP_PERMIT, permit.get());
        assertFalse(task.isAdmissionRejected());
    }

    public void testPolicyThatDoesNotApplyIsSkipped() {
        AtomicBoolean admitted = new AtomicBoolean();
        WorkloadGroupAdmissionPolicy notApplying = policy(false, (task, request, listener) -> {
            admitted.set(true);
            listener.onResponse(SearchAdmission.NOOP_PERMIT);
        });
        AtomicReference<Releasable> permit = new AtomicReference<>();
        new WorkloadGroupSearchAdmission(List.of(notApplying)).admit(
            searchTask(TaskId.EMPTY_TASK_ID),
            new SearchRequest(),
            ActionListener.wrap(permit::set, e -> fail())
        );
        assertFalse(admitted.get());
        assertSame(SearchAdmission.NOOP_PERMIT, permit.get());
    }

    public void testResourceLimitRejectionFailsBeforeTheThrottle() {
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("contended");
        doThrow(rejection).when(workloadGroupService).rejectIfNeeded(WORKLOAD_GROUP_ID);
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Exception> failure = new AtomicReference<>();
        admission.admit(task, new SearchRequest(), ActionListener.wrap(p -> fail("must not admit"), failure::set));
        assertSame(rejection, failure.get());
        // A rejected request must not take a throttle permit, and must not later count as a completion.
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
        assertTrue(task.isAdmissionRejected());
    }

    public void testAnUnexpectedResourceLimitErrorFailsOpen() {
        doThrow(new IllegalStateException("boom")).when(workloadGroupService).rejectIfNeeded(WORKLOAD_GROUP_ID);
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Releasable> permit = admit(task, new SearchRequest());
        serviceListener.get().onResponse(null);
        assertSame(SearchAdmission.NOOP_PERMIT, permit.get());
        assertFalse(task.isAdmissionRejected());
    }

    public void testScrollPageSkipsTheResourceLimitCheck() {
        doThrow(new OpenSearchRejectedExecutionException("contended")).when(workloadGroupService).rejectIfNeeded(any());
        admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchScrollRequest("scroll-id"));
        verify(workloadGroupService, never()).rejectIfNeeded(any());
        verify(workloadGroupService).acquireThrottlePermit(any(), any(), any());
    }

    public void testAFailingAdmittedListenerDoesNotMarkTheTaskRejected() {
        WorkloadGroupTask task = searchTask(TaskId.EMPTY_TASK_ID);
        IllegalStateException boom = new IllegalStateException("boom");
        // Not ActionListener.wrap, which would turn the throw into an onFailure of its own.
        admission.admit(task, new SearchRequest(), new ActionListener<>() {
            @Override
            public void onResponse(Releasable permit) {
                throw boom;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("an admitted request must not be failed by admission", e);
            }
        });
        expectThrows(IllegalStateException.class, () -> serviceListener.get().onResponse(null));
        assertFalse("an admitted request that later fails is a completion, not a rejection", task.isAdmissionRejected());
    }

    public void testAdmitsATaskOutsideWorkloadManagementWithoutACharge() {
        Task task = new Task(1, "transport", SearchAction.NAME, "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        assertSame(SearchAdmission.NOOP_PERMIT, admit(task, new SearchRequest()).get());
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
    }

    public void testCreatePitIsAdmittedWithoutACharge() {
        // CreatePitController runs its shard fan-out on a SearchTask that carries the create point-in-time action.
        SearchTask createPit = new SearchTask(1, "transport", CreatePitAction.NAME, () -> "test", TaskId.EMPTY_TASK_ID, Map.of());
        assertSame(SearchAdmission.NOOP_PERMIT, admit(createPit, new SearchRequest()).get());
        verify(workloadGroupService, never()).rejectIfNeeded(any());
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
    }

    public void testNoChargeBecomesTheNoopPermit() {
        AtomicReference<Releasable> permit = admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest());
        serviceListener.get().onResponse(null);
        assertSame(SearchAdmission.NOOP_PERMIT, permit.get());
    }

    public void testPassesThroughAPermitAndARejection() {
        Releasable charged = () -> {};
        WorkloadGroupTask admitted = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Releasable> permit = admit(admitted, new SearchRequest());
        serviceListener.get().onResponse(charged);
        assertSame(charged, permit.get());
        assertFalse(admitted.isAdmissionRejected());

        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("throttled");
        AtomicReference<Exception> failure = new AtomicReference<>();
        WorkloadGroupTask throttled = searchTask(TaskId.EMPTY_TASK_ID);
        admission.admit(throttled, new SearchRequest(), ActionListener.wrap(p -> fail("must not admit"), failure::set));
        serviceListener.get().onFailure(rejection);
        assertSame(rejection, failure.get());
        // A throttled request never ran, so it must not count as a completion.
        assertTrue(throttled.isAdmissionRejected());
    }

    public void testSearchInheritsACountedLocalParentsCharge() {
        SearchTask parent = searchTask(TaskId.EMPTY_TASK_ID);
        when(taskManager.getTask(PARENT_ID)).thenReturn(parent);
        admit(searchTask(new TaskId("local", PARENT_ID)), new SearchRequest());
        assertFalse("an uncounted parent has no charge to inherit", parentAlreadyCounted.get().getAsBoolean());

        parent.setThrottleCounted(true);
        assertTrue(parentAlreadyCounted.get().getAsBoolean());
    }

    public void testSearchInheritsNothingFromAMissingOrRemoteParent() {
        SearchTask parent = searchTask(TaskId.EMPTY_TASK_ID);
        parent.setThrottleCounted(true);
        when(taskManager.getTask(PARENT_ID)).thenReturn(parent);

        admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest());
        assertFalse(parentAlreadyCounted.get().getAsBoolean());
        admit(searchTask(new TaskId("remote", PARENT_ID)), new SearchRequest());
        assertFalse("a remote parent's charge is not visible here", parentAlreadyCounted.get().getAsBoolean());
        admit(searchTask(new TaskId("local", PARENT_ID + 1)), new SearchRequest());
        assertFalse("a parent no longer registered has no charge", parentAlreadyCounted.get().getAsBoolean());
        // Each of these still reaches the service, so a parent that cannot be counted never skips the throttle.
        verify(workloadGroupService, times(3)).acquireThrottlePermit(any(), any(), any());
    }

    public void testSearchUnderANonSearchParentInheritsNothing() {
        // An _msearch parent is a plain cancellable task, never counted, so each sub-search is charged on its own.
        Task msearch = new MultiSearchRequest().createTask(PARENT_ID, "transport", MultiSearchAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
        when(taskManager.getTask(PARENT_ID)).thenReturn(msearch);
        admit(searchTask(new TaskId("local", PARENT_ID)), new SearchRequest());
        assertFalse(parentAlreadyCounted.get().getAsBoolean());
    }

    public void testLooksUpTheParentOnlyWhenAsked() {
        admit(searchTask(new TaskId("local", PARENT_ID)), new SearchRequest());
        // The service asks only once a throttle applies; until then the admission must not touch the task manager.
        verifyNoInteractions(taskManager);
        parentAlreadyCounted.get().getAsBoolean();
        verify(taskManager).getTask(PARENT_ID);
    }

    public void testScrollPageNeverInheritsAParentsCharge() {
        SearchTask parent = searchTask(TaskId.EMPTY_TASK_ID);
        parent.setThrottleCounted(true);
        when(taskManager.getTask(PARENT_ID)).thenReturn(parent);
        admit(searchTask(new TaskId("local", PARENT_ID)), new SearchScrollRequest("scroll-id"));
        assertFalse(parentAlreadyCounted.get().getAsBoolean());
        verifyNoInteractions(taskManager);
    }

    private AtomicReference<Releasable> admit(Task task, ActionRequest request) {
        // Clear the previous call's arguments, so an assertion never reads them after an admit that skipped the service.
        parentAlreadyCounted.set(null);
        serviceListener.set(null);
        AtomicReference<Releasable> permit = new AtomicReference<>();
        admission.admit(task, request, ActionListener.wrap(permit::set, e -> { throw new AssertionError("unexpected failure", e); }));
        return permit;
    }

    @FunctionalInterface
    private interface Admit {
        void admit(WorkloadGroupTask task, ActionRequest request, ActionListener<Releasable> listener);
    }

    private static WorkloadGroupAdmissionPolicy policy(Admit admit) {
        return policy(true, admit);
    }

    /** An active policy that applies to every request when {@code applies}, and to none otherwise. */
    private static WorkloadGroupAdmissionPolicy policy(boolean applies, Admit admit) {
        return new WorkloadGroupAdmissionPolicy() {
            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public boolean appliesTo(WorkloadGroupTask task, ActionRequest request) {
                return applies;
            }

            @Override
            public void admit(WorkloadGroupTask task, ActionRequest request, ActionListener<Releasable> listener) {
                admit.admit(task, request, listener);
            }
        };
    }

    private static WorkloadGroupAdmissionPolicy grantingPolicy(Releasable permit) {
        return policy((task, request, listener) -> listener.onResponse(permit));
    }

    private static WorkloadGroupAdmissionPolicy refusingPolicy(Exception rejection) {
        return policy((task, request, listener) -> listener.onFailure(rejection));
    }

    /** A search task tagged with {@link #WORKLOAD_GROUP_ID}, as {@code TransportSearchAction} tags it before admission. */
    private static SearchTask searchTask(TaskId parentTaskId) {
        SearchTask task = new SearchTask(1, "transport", SearchAction.NAME, () -> "test", parentTaskId, Collections.emptyMap());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, WORKLOAD_GROUP_ID);
        task.setWorkloadGroupId(threadContext);
        return task;
    }
}
