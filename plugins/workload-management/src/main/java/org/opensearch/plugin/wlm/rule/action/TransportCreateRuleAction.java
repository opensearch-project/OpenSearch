/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.admin.cluster.remote.RemoteInfoAction;
import org.opensearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.opensearch.action.admin.cluster.remote.RemoteInfoResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.rule.service.RulePersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static java.util.stream.Collectors.toList;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to create Rule
 * @opensearch.experimental
 */
public class TransportCreateRuleAction extends HandledTransportAction<CreateRuleRequest, CreateRuleResponse> {

    private final RulePersistenceService rulePersistenceService;

    /**
     * Constructor for TransportCreateRuleAction
     *
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceService - a {@link RulePersistenceService} object
     */
    @Inject
    public TransportCreateRuleAction(
        TransportService transportService,
        ActionFilters actionFilters,
        RulePersistenceService rulePersistenceService
    ) {
        super(CreateRuleAction.NAME, transportService, actionFilters, CreateRuleRequest::new);
        this.rulePersistenceService = rulePersistenceService;
    }

    @Override
    protected void doExecute(Task task, CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        rulePersistenceService.createRule(request.getRule(), listener);
    }
}
