/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.rule.DeleteRuleRequest;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteRuleActionTests extends OpenSearchTestCase {
    TransportDeleteRuleAction sut;

    public void testExecute() {
        // Mocks
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry = mock(RulePersistenceServiceRegistry.class);
        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        RulePersistenceService rulePersistenceService = mock(RulePersistenceService.class);
        DeleteRuleRequest deleteRuleRequest = mock(DeleteRuleRequest.class);

        // Behavior
        when(deleteRuleRequest.getFeatureType()).thenReturn(FeatureType.from("feature_x"));
        when(rulePersistenceServiceRegistry.getRulePersistenceService(any())).thenReturn(rulePersistenceService);
        doNothing().when(rulePersistenceService).deleteRule(any(), any());

        // Test execution
        sut = new TransportDeleteRuleAction(transportService, actionFilters, rulePersistenceServiceRegistry);
        sut.doExecute(null, deleteRuleRequest, null);

        // Assertion
        verify(rulePersistenceService, times(1)).deleteRule(any(), any());
    }
}
