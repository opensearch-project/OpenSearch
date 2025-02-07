/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.get.GetRequestBuilder;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.client.FilterClient;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.Rule;

import java.io.IOException;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.opensearch.plugin.wlm.RuleTestUtils.*;
import static org.opensearch.plugin.wlm.rule.service.RulePersistenceService.RULE_INDEX;
@SuppressWarnings("unchecked")
public class RulePersistenceServiceTests extends OpenSearchTestCase {

    /**
     * Test case to validate the creation logic of a Rule
     */
    public void testCreateRule() throws IOException {
        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        Client client = mock(Client.class);
        RulePersistenceService rulePersistenceService = new RulePersistenceService(client);
        IndexResponse indexResponse = new IndexResponse(new ShardId(RULE_INDEX, "uuid", 0), "id", 1, 1, 1, true);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));

        rulePersistenceService.createRule(ruleOne, listener);
        verify(client).index(any(IndexRequest.class), any(ActionListener.class));
        ArgumentCaptor<CreateRuleResponse> responseCaptor = ArgumentCaptor.forClass(CreateRuleResponse.class);
        verify(listener).onResponse(responseCaptor.capture());

        CreateRuleResponse createRuleResponse = responseCaptor.getValue();
        assertNotNull(createRuleResponse);
        Rule rule = createRuleResponse.getRule();
        assertEquals(rule.getFeature(), ruleOne.getFeature());
        assertEquals(rule.getLabel(), ruleOne.getLabel());
        assertEquals(rule.getAttributeMap(), ruleOne.getAttributeMap());
        clearInvocations(client, listener);
    }

    public void testGetRuleById() throws IOException {
        String ruleSource = ruleOne.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).toString();
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        Client client = mock(Client.class);
        RulePersistenceService rulePersistenceService = new RulePersistenceService(client);
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);

        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSourceAsString()).thenReturn(ruleSource);
        when(client.prepareGet(eq(RULE_INDEX), eq(_ID_ONE))).thenReturn(getRequestBuilder);
        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(0);
            actionListener.onResponse(getResponse);
            return null;
        }).when(getRequestBuilder).execute(any(ActionListener.class));

        rulePersistenceService.getRule(_ID_ONE, listener);

        ArgumentCaptor<GetRuleResponse> captor = ArgumentCaptor.forClass(GetRuleResponse.class);
        verify(listener).onResponse(captor.capture());
        GetRuleResponse response = captor.getValue();
        assertNotNull(response);
        assertEqualRules(Map.of(_ID_ONE, ruleOne), response.getRules(), false);
        clearInvocations(client, getRequestBuilder, getResponse, listener);
    }

    public void testGetRuleByIdNotFound() {
        String nonExistentRuleId = "non-existent-rule";
        Client client = mock(Client.class);
        RulePersistenceService rulePersistenceService = new RulePersistenceService(client);
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        GetResponse getResponse = mock(GetResponse.class);
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);

        when(client.prepareGet(RULE_INDEX, nonExistentRuleId)).thenReturn(getRequestBuilder);
        when(getResponse.isExists()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<GetResponse> actionListener = invocation.getArgument(0);
            actionListener.onResponse(getResponse);
            return null;
        }).when(getRequestBuilder).execute(any(ActionListener.class));

        rulePersistenceService.getRule(nonExistentRuleId, listener);

        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(captor.capture());
        Exception exception = captor.getValue();
        assertTrue(exception instanceof ResourceNotFoundException);
        clearInvocations(client, getRequestBuilder, getResponse, listener);
    }
}
