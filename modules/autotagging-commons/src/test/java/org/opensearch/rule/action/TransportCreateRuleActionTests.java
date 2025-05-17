/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.rule.CreateRuleRequest;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateRuleActionTests extends OpenSearchTestCase {
    TransportCreateRuleAction sut;

    public void testExecute() throws Exception {
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry = mock(RulePersistenceServiceRegistry.class);
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        RulePersistenceService rulePersistenceService = mock(RulePersistenceService.class);
        CreateRuleRequest createRuleRequest = mock(CreateRuleRequest.class);
        Rule rule = mock(Rule.class);
        when(createRuleRequest.getRule()).thenReturn(rule);
        when(createRuleRequest.getRule().getFeatureType()).thenReturn(null);
        ThreadPool threadPool = mock(ThreadPool.class);
        ClusterService clusterService = mock(ClusterService.class);
        IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);

        when(rulePersistenceServiceRegistry.getRulePersistenceService(any())).thenReturn(rulePersistenceService);
        doNothing().when(rulePersistenceService).getRule(any(), any());
        sut = new TransportCreateRuleAction(
            threadPool,
            transportService,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            rulePersistenceServiceRegistry
        );
        sut.clusterManagerOperation(createRuleRequest, null, null);
        verify(rulePersistenceService, times(1)).createRule(any(), any());
    }
}
