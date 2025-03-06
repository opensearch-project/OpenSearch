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
import org.opensearch.autotagging.Rule;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import org.mockito.ArgumentCaptor;

import static org.opensearch.plugin.wlm.RuleTestUtils.assertEqualRule;
import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;
import static org.opensearch.plugin.wlm.RuleTestUtils.setUpRulePersistenceService;
import static org.opensearch.plugin.wlm.rule.service.RulePersistenceService.RULE_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class RulePersistenceServiceTests extends OpenSearchTestCase {

    /**
     * Test case to validate the creation logic of a Rule
     */
    public void testCreateRule() throws IOException {
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
        Rule<QueryGroupFeatureType> rule = createRuleResponse.getRule();
        assertEqualRule(ruleOne, rule, false);
        clearInvocations(client, listener);
    }
}
