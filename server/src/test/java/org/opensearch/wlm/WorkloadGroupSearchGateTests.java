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
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchExecutionGate;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class WorkloadGroupSearchGateTests extends OpenSearchTestCase {

    private static final long PARENT_ID = 7;

    private WorkloadGroupService workloadGroupService;
    private TaskManager taskManager;
    private WorkloadGroupSearchGate gate;
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
        gate = new WorkloadGroupSearchGate(workloadGroupService, clusterService, taskManager);
    }

    public void testIsActiveFollowsWhetherThrottlingIsActive() {
        when(workloadGroupService.isThrottlingActive()).thenReturn(true);
        assertTrue(gate.isActive());
        when(workloadGroupService.isThrottlingActive()).thenReturn(false);
        assertFalse(gate.isActive());
    }

    public void testAdmitsATaskOutsideWorkloadManagementWithoutACharge() {
        Task task = new Task(1, "transport", SearchAction.NAME, "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        assertSame(SearchExecutionGate.NOOP_PERMIT, admit(task, new SearchRequest()).get());
        verify(workloadGroupService, never()).acquireThrottlePermit(any(), any(), any());
    }

    public void testNoChargeBecomesTheNoopPermit() {
        AtomicReference<Releasable> permit = admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest());
        serviceListener.get().onResponse(null);
        assertSame(SearchExecutionGate.NOOP_PERMIT, permit.get());
    }

    public void testPassesThroughAPermitAndARejection() {
        Releasable charged = () -> {};
        AtomicReference<Releasable> permit = admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest());
        serviceListener.get().onResponse(charged);
        assertSame(charged, permit.get());

        OpenSearchRejectedExecutionException rejection = new OpenSearchRejectedExecutionException("throttled");
        AtomicReference<Exception> failure = new AtomicReference<>();
        gate.admit(searchTask(TaskId.EMPTY_TASK_ID), new SearchRequest(), ActionListener.wrap(p -> fail("must not admit"), failure::set));
        serviceListener.get().onFailure(rejection);
        assertSame(rejection, failure.get());
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
        // The service asks only once a throttle applies; until then the gate must not touch the task manager.
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
        gate.admit(task, request, ActionListener.wrap(permit::set, e -> { throw new AssertionError("unexpected failure", e); }));
        return permit;
    }

    private static SearchTask searchTask(TaskId parentTaskId) {
        return new SearchTask(1, "transport", SearchAction.NAME, () -> "test", parentTaskId, Collections.emptyMap());
    }
}
