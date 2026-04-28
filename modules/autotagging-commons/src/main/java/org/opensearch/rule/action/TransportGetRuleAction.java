/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get Rules
 * @opensearch.experimental
 */
public class TransportGetRuleAction extends HandledTransportAction<GetRuleRequest, GetRuleResponse> {

    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;
    private final ThreadPool threadPool;

    /**
     * Constructor for TransportGetRuleAction
     * @param transportService - a {@link TransportService} object
     * @param threadPool - a {@link ThreadPool} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     */
    @Inject
    public TransportGetRuleAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry
    ) {
        super(GetRuleAction.NAME, transportService, actionFilters, GetRuleRequest::new);
        this.threadPool = threadPool;
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;
    }

    @Override
    protected void doExecute(Task task, GetRuleRequest request, ActionListener<GetRuleResponse> listener) {
        threadPool.executor(ThreadPool.Names.GET).execute(() -> {
            final RulePersistenceService rulePersistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
                request.getFeatureType()
            );
            rulePersistenceService.getRule(request, listener);
        });
    }
}
