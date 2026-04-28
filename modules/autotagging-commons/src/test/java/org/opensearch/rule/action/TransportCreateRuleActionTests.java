/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.RuleRoutingService;
import org.opensearch.rule.RuleRoutingServiceRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TransportCreateRuleActionTests extends OpenSearchTestCase {
    private TransportService transportService;
    private ThreadPool threadPool;
    private ActionFilters actionFilters;
    private TransportCreateRuleAction action;
    private FeatureType mockFeatureType;
    private RuleRoutingServiceRegistry routingRegistry;
    private RulePersistenceServiceRegistry persistenceRegistry;
    private RuleRoutingService mockRoutingService;

    private final String testIndexName = "test-index";

    public void setUp() throws Exception {
        super.setUp();
        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        actionFilters = mock(ActionFilters.class);
        mockFeatureType = mock(FeatureType.class);
        routingRegistry = mock(RuleRoutingServiceRegistry.class);
        persistenceRegistry = mock(RulePersistenceServiceRegistry.class);
        when(mockFeatureType.getName()).thenReturn("test_feature");
        mockRoutingService = mock(RuleRoutingService.class);
        routingRegistry.register(mockFeatureType, mockRoutingService);

        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any());
        when(threadPool.executor(any())).thenReturn(executorService);
        action = new TransportCreateRuleAction(transportService, threadPool, actionFilters, persistenceRegistry, routingRegistry);
    }

    public void testExecution() {
        IndexStoredRulePersistenceService persistenceService = mock(IndexStoredRulePersistenceService.class);
        when(persistenceRegistry.getRulePersistenceService(mockFeatureType)).thenReturn(persistenceService);
        when(routingRegistry.getRuleRoutingService(mockFeatureType)).thenReturn(mockRoutingService);
        Rule rule = mock(Rule.class);
        when(rule.getFeatureType()).thenReturn(mockFeatureType);
        CreateRuleRequest request = new CreateRuleRequest(rule);
        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        action.doExecute(null, request, listener);
        verify(routingRegistry, times(1)).getRuleRoutingService(request.getRule().getFeatureType());
    }
}
