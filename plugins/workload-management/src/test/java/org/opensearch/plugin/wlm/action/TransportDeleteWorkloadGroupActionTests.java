/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteWorkloadGroupActionTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private ThreadPool threadPool;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private WorkloadGroupPersistenceService workloadGroupPersistenceService;
    private IndexStoredRulePersistenceService rulePersistenceService;
    private WorkloadGroupFeatureType featureType;
    private TransportDeleteWorkloadGroupAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        transportService = mock(TransportService.class);
        actionFilters = mock(ActionFilters.class);
        threadPool = mock(ThreadPool.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        workloadGroupPersistenceService = mock(WorkloadGroupPersistenceService.class);
        rulePersistenceService = mock(IndexStoredRulePersistenceService.class);
        featureType = mock(WorkloadGroupFeatureType.class);

        action = new TransportDeleteWorkloadGroupAction(
            clusterService,
            transportService,
            actionFilters,
            threadPool,
            indexNameExpressionResolver,
            workloadGroupPersistenceService,
            rulePersistenceService,
            featureType
        );
    }

    /**
     * Test case to validate the construction for TransportDeleteWorkloadGroupAction
     */
    public void testConstruction() {
        assertNotNull(action);
        assertEquals(ThreadPool.Names.GET, action.executor());
    }

    /**
     * Test case to validate successful workload group deletion
     */
    public void testClusterManagerOperationSuccess() throws Exception {
        String workloadGroupName = "testGroup";
        String workloadGroupId = "test-id-123";
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(workloadGroupName);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Create mock workload group
        WorkloadGroup mockWorkloadGroup = createMockWorkloadGroup(workloadGroupName, workloadGroupId);

        // Create mock cluster state with workload group
        ClusterState clusterState = createMockClusterStateWithWorkloadGroup(mockWorkloadGroup);

        // Mock empty rules response
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        when(getRuleResponse.getRules()).thenReturn(Collections.emptyList());

        // Mock executor service
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(threadPool.executor(ThreadPool.Names.GET)).thenReturn(mockExecutor);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutor).submit(any(Runnable.class));

        // Mock rule persistence service responses
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetRuleResponse> ruleListener = invocation.getArgument(1);
            ruleListener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any());

        action.clusterManagerOperation(request, clusterState, listener);

        verify(workloadGroupPersistenceService).deleteInClusterStateMetadata(eq(request), eq(listener));
        verify(mockExecutor).submit(any(Runnable.class));
    }

    /**
     * Test case to validate ResourceNotFoundException when workload group doesn't exist
     */
    public void testClusterManagerOperationWorkloadGroupNotFound() throws Exception {
        String workloadGroupName = "nonExistentGroup";
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(workloadGroupName);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Create empty cluster state
        ClusterState clusterState = createEmptyClusterState();

        // Mock executor service
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(threadPool.executor(ThreadPool.Names.GET)).thenReturn(mockExecutor);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(mockExecutor).submit(any(Runnable.class));

        action.clusterManagerOperation(request, clusterState, listener);

        verify(listener).onFailure(any(ResourceNotFoundException.class));
        verify(workloadGroupPersistenceService, never()).deleteInClusterStateMetadata(any(), any());
    }

    /**
     * Test case to validate that deletion is prevented when rules exist for the workload group
     */
    public void testRuleDeletionWithExistingRules() throws Exception {
        String workloadGroupName = "testGroup";
        String workloadGroupId = "test-id-123";
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(workloadGroupName);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        WorkloadGroup mockWorkloadGroup = createMockWorkloadGroup(workloadGroupName, workloadGroupId);
        ClusterState clusterState = createMockClusterStateWithWorkloadGroup(mockWorkloadGroup);

        // Mock rules that reference this workload group
        Rule mockRule = createMockRule("rule-1", workloadGroupId);
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        when(getRuleResponse.getRules()).thenReturn(List.of(mockRule));

        // Mock executor to immediately execute the runnable
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(threadPool.executor(ThreadPool.Names.GET)).thenReturn(mockExecutor);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(mockExecutor).submit(any(Runnable.class));

        // Mock rule persistence service responses
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetRuleResponse> ruleListener = invocation.getArgument(1);
            ruleListener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any());

        action.clusterManagerOperation(request, clusterState, listener);

        verify(rulePersistenceService).getRule(any(GetRuleRequest.class), any());
        verify(listener).onFailure(any(IllegalStateException.class));
        verify(workloadGroupPersistenceService, never()).deleteInClusterStateMetadata(any(), any());
    }

    /**
     * Test case to validate successful deletion when no rules exist for the workload group
     */
    public void testSuccessfulDeletionWithNoRules() throws Exception {
        String workloadGroupName = "testGroup";
        String workloadGroupId = "test-id-123";
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(workloadGroupName);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        WorkloadGroup mockWorkloadGroup = createMockWorkloadGroup(workloadGroupName, workloadGroupId);
        ClusterState clusterState = createMockClusterStateWithWorkloadGroup(mockWorkloadGroup);

        // Mock empty rules response
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        when(getRuleResponse.getRules()).thenReturn(Collections.emptyList());

        // Mock executor to immediately execute the runnable
        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(threadPool.executor(ThreadPool.Names.GET)).thenReturn(mockExecutor);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(mockExecutor).submit(any(Runnable.class));

        // Mock rule persistence service responses
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetRuleResponse> ruleListener = invocation.getArgument(1);
            ruleListener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any());

        action.clusterManagerOperation(request, clusterState, listener);

        verify(rulePersistenceService).getRule(any(GetRuleRequest.class), any());
        verify(workloadGroupPersistenceService).deleteInClusterStateMetadata(eq(request), eq(listener));
    }

    /**
     * Test case to validate rule deletion handles exceptions gracefully
     */
    public void testRuleDeletionWithException() throws Exception {
        String workloadGroupName = "testGroup";
        String workloadGroupId = "test-id-123";
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(workloadGroupName);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        WorkloadGroup mockWorkloadGroup = createMockWorkloadGroup(workloadGroupName, workloadGroupId);
        ClusterState clusterState = createMockClusterStateWithWorkloadGroup(mockWorkloadGroup);

        ExecutorService mockExecutor = mock(ExecutorService.class);
        when(threadPool.executor(ThreadPool.Names.GET)).thenReturn(mockExecutor);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(mockExecutor).submit(any(Runnable.class));

        // Mock rule persistence service to throw exception
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetRuleResponse> ruleListener = invocation.getArgument(1);
            ruleListener.onFailure(new RuntimeException("Rule service error"));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any());

        // Should not throw exception, just log and continue
        action.clusterManagerOperation(request, clusterState, listener);

        verify(rulePersistenceService).getRule(any(GetRuleRequest.class), any());
        verify(listener).onFailure(any(RuntimeException.class));
    }

    /**
     * Test case to validate cluster block check
     */
    public void testCheckBlock() {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest("testGroup");
        ClusterState clusterState = mock(ClusterState.class);
        ClusterBlocks clusterBlocks = mock(ClusterBlocks.class);

        when(clusterState.blocks()).thenReturn(clusterBlocks);

        action.checkBlock(request, clusterState);

        verify(clusterBlocks).globalBlockedException(any());
    }

    /**
     * Test case to validate stream input reading
     */
    public void testRead() throws IOException {
        AcknowledgedResponse originalResponse = new AcknowledgedResponse(true);

        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        AcknowledgedResponse readResponse = action.read(out.bytes().streamInput());

        assertEquals(originalResponse.isAcknowledged(), readResponse.isAcknowledged());
    }

    private WorkloadGroup createMockWorkloadGroup(String name, String id) {
        Map<ResourceType, Double> resourceLimits = Map.of(ResourceType.CPU, 0.5);
        MutableWorkloadGroupFragment fragment = new MutableWorkloadGroupFragment(
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
            resourceLimits
        );
        return new WorkloadGroup(name, id, fragment, System.currentTimeMillis());
    }

    private ClusterState createMockClusterStateWithWorkloadGroup(WorkloadGroup workloadGroup) {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        Map<String, WorkloadGroup> workloadGroups = Map.of(workloadGroup.get_id(), workloadGroup);

        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(workloadGroups);

        return clusterState;
    }

    private ClusterState createEmptyClusterState() {
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        Map<String, WorkloadGroup> workloadGroups = Collections.emptyMap();

        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(workloadGroups);

        return clusterState;
    }

    private Rule createMockRule(String ruleId, String workloadGroupId) {
        Rule mockRule = mock(Rule.class);
        when(mockRule.getId()).thenReturn(ruleId);
        when(mockRule.getFeatureValue()).thenReturn(workloadGroupId);
        return mockRule;
    }
}
