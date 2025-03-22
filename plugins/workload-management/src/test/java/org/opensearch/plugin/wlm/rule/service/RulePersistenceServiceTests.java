/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.get.GetRequestBuilder;
import org.opensearch.action.get.GetResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.opensearch.plugin.wlm.RuleTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.RuleTestUtils.assertEqualRules;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;
import static org.opensearch.plugin.wlm.RuleTestUtils.setUpRulePersistenceService;
import static org.opensearch.plugin.wlm.rule.service.RulePersistenceService.RULES_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class RulePersistenceServiceTests extends OpenSearchTestCase {
    public void testGetRuleById() throws IOException {
        String ruleSource = ruleOne.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).toString();
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsString()).thenReturn(ruleSource);
        when(client.prepareGet(eq(RULES_INDEX), eq(_ID_ONE))).thenReturn(getRequestBuilder);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(0);
            actionListener.onResponse(getResponse);
            return null;
        }).when(getRequestBuilder).execute(any(ActionListener.class));

        rulePersistenceService.getRule(_ID_ONE, new HashMap<>(), null, listener);

        ArgumentCaptor<GetRuleResponse> captor = ArgumentCaptor.forClass(GetRuleResponse.class);
        verify(listener).onResponse(captor.capture());
        GetRuleResponse response = captor.getValue();
        assertNotNull(response);
        assertEqualRules(Map.of(_ID_ONE, ruleOne), response.getRules(), false);
        clearInvocations(client, getRequestBuilder, getResponse, listener);
    }

    public void testGetRuleByIdNotFound() {
        String nonExistentRuleId = "non-existent-rule";
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);

        when(client.prepareGet(RULES_INDEX, nonExistentRuleId)).thenReturn(getRequestBuilder);
        when(getResponse.isExists()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(0);
            actionListener.onResponse(getResponse);
            return null;
        }).when(getRequestBuilder).execute(any(ActionListener.class));

        rulePersistenceService.getRule(nonExistentRuleId, new HashMap<>(), null, listener);

        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(captor.capture());
        Exception exception = captor.getValue();
        assertTrue(exception instanceof ResourceNotFoundException);
        clearInvocations(client, getRequestBuilder, getResponse, listener);
    }
}
