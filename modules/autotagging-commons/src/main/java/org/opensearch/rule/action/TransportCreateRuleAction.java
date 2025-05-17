/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.rule.CreateRuleRequest;
import org.opensearch.rule.CreateRuleResponse;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to create Rules
 * @opensearch.experimental
 */
public class TransportCreateRuleAction extends TransportClusterManagerNodeAction<CreateRuleRequest, CreateRuleResponse> {

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportCreateRuleAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param transportService - a {@link TransportService} object
     * @param clusterService - a {@link ClusterService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param indexNameExpressionResolver - {@link IndexNameExpressionResolver} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     */
    @Inject
    public TransportCreateRuleAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry
    ) {
        super(
            CreateRuleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateRuleRequest::new,
            indexNameExpressionResolver
        );
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
    }

    @Override
    protected String executor() {
        return SAME;
    }

    @Override
    protected CreateRuleResponse read(StreamInput in) throws IOException {
        return new CreateRuleResponse(in);
    }

    @Override
    protected void clusterManagerOperation(CreateRuleRequest request, ClusterState state, ActionListener<CreateRuleResponse> listener)
        throws Exception {
        final RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
            request.getRule().getFeatureType()
        );
        rulePersistenceService.createRule(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateRuleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }
}
