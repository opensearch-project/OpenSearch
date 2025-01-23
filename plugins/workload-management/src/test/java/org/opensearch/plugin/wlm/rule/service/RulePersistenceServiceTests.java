/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.Rule;

import java.io.IOException;

import org.mockito.ArgumentCaptor;

import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;
import static org.opensearch.plugin.wlm.RuleTestUtils.setUpRulePersistenceService;
import static org.opensearch.plugin.wlm.rule.service.RulePersistenceService.RULE_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class RulePersistenceServiceTests extends OpenSearchTestCase {

    public void testCreateRuleIndexIfAbsent() {
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService();
        Client client = rulePersistenceService.getClient();
        ClusterService clusterService = rulePersistenceService.getClusterService();
        Metadata metadata = mock(Metadata.class);
        ClusterState clusterState = mock(ClusterState.class);
        ActionListener<Boolean> listener = mock(ActionListener.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.hasIndex(RulePersistenceService.RULE_INDEX)).thenReturn(true);

        rulePersistenceService.createRuleIndexIfAbsent(listener);
        verify(listener).onResponse(true);
        verify(client, never()).admin();
    }

    /**
     * Test case to validate the creation logic of a Rule
     */
    public void testPersistRule() throws IOException {
        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService();
        Client client = rulePersistenceService.getClient();
        IndexResponse indexResponse = new IndexResponse(new ShardId(RULE_INDEX, "uuid", 0), "id", 1, 1, 1, true);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));

        rulePersistenceService.persistRule(ruleOne, listener);
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
}
