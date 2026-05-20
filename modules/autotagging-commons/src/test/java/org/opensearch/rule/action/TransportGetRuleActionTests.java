/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.apache.lucene.util.SameThreadExecutorService;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetRuleActionTests extends OpenSearchTestCase {
    TransportGetRuleAction sut;

    public void testExecute() {
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry = mock(RulePersistenceServiceRegistry.class);
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        RulePersistenceService rulePersistenceService = mock(RulePersistenceService.class);
        GetRuleRequest getRuleRequest = mock(GetRuleRequest.class);
        when(getRuleRequest.getFeatureType()).thenReturn(null);

        when(rulePersistenceServiceRegistry.getRulePersistenceService(any())).thenReturn(rulePersistenceService);
        doNothing().when(rulePersistenceService).getRule(any(), any());
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(new SameThreadExecutorService());
        sut = new TransportGetRuleAction(transportService, threadPool, actionFilters, rulePersistenceServiceRegistry);
        sut.doExecute(null, getRuleRequest, null);
        verify(rulePersistenceService, times(1)).getRule(any(), any());
    }
}
