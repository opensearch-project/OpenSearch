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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

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
        admission = new WorkloadGroupSearchAdmission(workloadGroupService, clusterService, taskManager);
    }

    public void testIsActiveFollowsWhetherSearchAdmissionIsActive() {
        when(workloadGroupService.isSearchAdmissionActive()).thenReturn(true);
        assertTrue(admission.isActive());
        when(workloadGroupService.isSearchAdmissionActive()).thenReturn(false);
        assertFalse(admission.isActive());
    }

    public void testResourceLimitRejectionFailsBeforeTheThrottle() {
        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("contended");
        doThrow(rejection).when(workloadGroupService).rejectIfNeeded(any());
        SearchTask task = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Exception> failure = new AtomicReference<>();
        admission.admit(task, new SearchRequest(), ActionListener.wrap(p -> fail("must not admit"), failure::set));
        assertSame(rejection, failure.get());
        // A rejected request must not take a throttle permit, and must not later count as a completion.
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
        assertTrue(task.isAdmissionRejected());
    }

    public void testAnUnexpectedResourceLimitErrorFailsOpen() {
        doThrow(new IllegalStateException("boom")).when(workloadGroupService).rejectIfNeeded(any());
        SearchTask task = searchTask(TaskId.EMPTY_TASK_ID);
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

    public void testCreatePitSkipsTheResourceLimitCheck() {
        SearchTask createPit = new SearchTask(1, "transport", CreatePitAction.NAME, () -> "test", TaskId.EMPTY_TASK_ID, Map.of());
        admit(createPit, new SearchRequest());
        verify(workloadGroupService, never()).rejectIfNeeded(any());
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
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
    }

    public void testNoChargeBecomesTheNoopPermit() {
        AtomicReference<Releasable> permit = admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest());
        serviceListener.get().onResponse(null);
        assertSame(SearchAdmission.NOOP_PERMIT, permit.get());
    }

    public void testPassesThroughAPermitAndARejection() {
        Releasable charged = () -> {};
        SearchTask admitted = searchTask(TaskId.EMPTY_TASK_ID);
        AtomicReference<Releasable> permit = admit(admitted, new SearchRequest());
        serviceListener.get().onResponse(charged);
        assertSame(charged, permit.get());
        assertFalse(admitted.isAdmissionRejected());

        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("throttled");
        AtomicReference<Exception> failure = new AtomicReference<>();
        SearchTask throttled = searchTask(TaskId.EMPTY_TASK_ID);
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

    private static SearchTask searchTask(TaskId parentTaskId) {
        return new SearchTask(1, "transport", SearchAction.NAME, () -> "test", parentTaskId, Collections.emptyMap());
    }
}
