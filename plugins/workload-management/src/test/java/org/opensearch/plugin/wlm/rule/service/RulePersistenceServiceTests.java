/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.HashMap;

import static org.opensearch.autotagging.Rule._ID_STRING;
import static org.opensearch.plugin.wlm.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.plugin.wlm.RuleTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.RuleTestUtils._ID_TWO;
import static org.opensearch.plugin.wlm.RuleTestUtils.setUpRulePersistenceService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class RulePersistenceServiceTests extends OpenSearchTestCase {
    public void testBuildGetRuleQuery_WithId() {
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
        BoolQueryBuilder query = rulePersistenceService.buildGetRuleQuery(_ID_ONE, new HashMap<>());
        assertTrue(query.hasClauses());
        assertEquals(QueryBuilders.termQuery(_ID_STRING, _ID_ONE).toString(), query.must().get(0).toString());
    }

    public void testBuildGetRuleQuery_WithFilters() {
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
        BoolQueryBuilder query = rulePersistenceService.buildGetRuleQuery(null, ATTRIBUTE_MAP);
        assertTrue(query.hasClauses());
        assertEquals(1, query.must().size());
        assertTrue(query.filter().contains(QueryBuilders.existsQuery(QueryGroupFeatureType.NAME)));
    }

    public void testGetRule_WithId() {
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SetupMocksForGetRule(client, searchRequestBuilder);

        rulePersistenceService.getRule(_ID_ONE, new HashMap<>(), null, listener);
        verify(client).prepareSearch(RulePersistenceService.RULES_INDEX);
        verify(searchRequestBuilder).setQuery(any());
        verify(searchRequestBuilder).execute(any());
    }

    public void testGetRule_WithSearchAfter() {
        RulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        SetupMocksForGetRule(client, searchRequestBuilder);
        when(searchRequestBuilder.addSort(anyString(), any(SortOrder.class))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.searchAfter(any())).thenReturn(searchRequestBuilder);

        rulePersistenceService.getRule(null, new HashMap<>(), _ID_TWO, listener);
        verify(searchRequestBuilder).addSort(_ID_STRING, SortOrder.ASC);
        verify(searchRequestBuilder).searchAfter(new Object[] { _ID_TWO });
    }

    public void SetupMocksForGetRule(Client client, SearchRequestBuilder searchRequestBuilder) {
        when(client.prepareSearch(anyString())).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setQuery(any())).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setSize(anyInt())).thenReturn(searchRequestBuilder);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> searchListener = invocation.getArgument(0);
            searchListener.onResponse(mock(SearchResponse.class));
            return null;
        }).when(searchRequestBuilder).execute(any());
    }
}
