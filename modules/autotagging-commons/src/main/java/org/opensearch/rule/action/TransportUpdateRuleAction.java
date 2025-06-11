/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.RuleRoutingServiceRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.rule.RuleFrameworkPlugin.RULE_THREAD_POOL_NAME;

/**
 * Transport action to update Rules
 * @opensearch.experimental
 */
public class TransportUpdateRuleAction extends TransportAction<UpdateRuleRequest, UpdateRuleResponse> {
    private final ThreadPool threadPool;
    private final RuleRoutingServiceRegistry ruleRoutingServiceRegistry;
    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;

    /**
     * Constructor for TransportUpdateRuleAction
     * @param transportService - a {@link TransportService} object
     * @param  threadPool - a {@link ThreadPool} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     * @param ruleRoutingServiceRegistry - a {@link RuleRoutingServiceRegistry} object
     */
    @Inject
    public TransportUpdateRuleAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry,
        RuleRoutingServiceRegistry ruleRoutingServiceRegistry
    ) {
        super(UpdateRuleAction.NAME, actionFilters, transportService.getTaskManager());
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
        this.ruleRoutingServiceRegistry = ruleRoutingServiceRegistry;
        this.threadPool = threadPool;

        transportService.registerRequestHandler(
            UpdateRuleAction.NAME,
            ThreadPool.Names.SAME,
            UpdateRuleRequest::new,
            new TransportRequestHandler<UpdateRuleRequest>() {
                @Override
                public void messageReceived(UpdateRuleRequest request, TransportChannel channel, Task task) {
                    executeLocally(request, ActionListener.wrap(response -> {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Failed to send UpdateRuleResponse to transport channel", e);
                            throw new TransportException("Fail to send", e);
                        }
                    }, exception -> {
                        try {
                            channel.sendResponse(exception);
                        } catch (IOException e) {
                            logger.error("Failed to send exception response to transport channel", e);
                            throw new TransportException("Fail to send", e);
                        }
                    }));
                }
            }
        );
    }

    @Override
    protected void doExecute(Task task, UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        ruleRoutingServiceRegistry.getRuleRoutingService(request.getFeatureType()).handleUpdateRuleRequest(request, listener);
    }

    /**
     * Executes the update rule operation locally on the dedicated rule thread pool.
     * @param request the UpdateRuleRequest
     * @param listener listener to handle response or failure
     */
    private void executeLocally(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
        threadPool.executor(RULE_THREAD_POOL_NAME).execute(() -> {
            final RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
                request.getFeatureType()
            );
            rulePersistenceService.updateRule(request, listener);
        });
    }
}
