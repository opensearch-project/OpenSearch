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
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Transport action for delete WorkloadGroup
 *
 * @opensearch.experimental
 */
public class TransportDeleteWorkloadGroupAction extends TransportClusterManagerNodeAction<
    DeleteWorkloadGroupRequest,
    AcknowledgedResponse> {

    private final WorkloadGroupPersistenceService workloadGroupPersistenceService;
    private final RulePersistenceService rulePersistenceService;
    private final FeatureType featureType;

    /**
     * Constructor for TransportDeleteWorkloadGroupAction
     *
     * @param clusterService - a {@link ClusterService} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param indexNameExpressionResolver - a {@link IndexNameExpressionResolver} object
     * @param workloadGroupPersistenceService - a {@link WorkloadGroupPersistenceService} object
     * @param persistenceService - a {@link IndexStoredRulePersistenceService} instance
     * @param featureType - workloadManagement feature type
     */
    @Inject
    public TransportDeleteWorkloadGroupAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        WorkloadGroupPersistenceService workloadGroupPersistenceService,
        IndexStoredRulePersistenceService persistenceService,
        WorkloadGroupFeatureType featureType
    ) {
        super(
            DeleteWorkloadGroupAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteWorkloadGroupRequest::new,
            indexNameExpressionResolver
        );
        this.workloadGroupPersistenceService = workloadGroupPersistenceService;
        this.rulePersistenceService = persistenceService;
        this.featureType = featureType;
    }

    @Override
    protected void clusterManagerOperation(
        DeleteWorkloadGroupRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        threadPool.executor(executor()).submit(() -> {
            try {
                checkNoAssociatedRulesExist(request, listener, state);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GET;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void checkNoAssociatedRulesExist(
        DeleteWorkloadGroupRequest request,
        ActionListener<AcknowledgedResponse> listener,
        ClusterState state
    ) {
        Collection<WorkloadGroup> workloadGroups = WorkloadGroupPersistenceService.getFromClusterStateMetadata(request.getName(), state);
        if (workloadGroups.isEmpty()) {
            throw new ResourceNotFoundException("No WorkloadGroup exists with the provided name: " + request.getName());
        }

        WorkloadGroup workloadGroup = workloadGroups.iterator().next();
        rulePersistenceService.getRule(
            new GetRuleRequest(null, Collections.emptyMap(), null, featureType),
            new ActionListener<GetRuleResponse>() {
                @Override
                public void onResponse(GetRuleResponse getRuleResponse) {
                    List<Rule> associatedRules = getRuleResponse.getRules()
                        .stream()
                        .filter(rule -> rule.getFeatureValue().equals(workloadGroup.get_id()))
                        .toList();

                    if (!associatedRules.isEmpty()) {
                        listener.onFailure(
                            new IllegalStateException(
                                workloadGroup.getName()
                                    + " workload group has rules with ids: "
                                    + associatedRules
                                    + " ."
                                    + "Please delete them first otherwise system will be an inconsistent state."
                            )
                        );
                        return;
                    }
                    workloadGroupPersistenceService.deleteInClusterStateMetadata(request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }
}
