/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("unchecked")
public class WlmRulePersistenceServiceTests extends OpenSearchTestCase {
    // public void testBuildGetRuleQuery_WithId() {
    // WlmRulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
    // BoolQueryBuilder query = rulePersistenceService.buildGetRuleQuery(_ID_ONE, new HashMap<>());
    // assertTrue(query.hasClauses());
    // assertEquals(QueryBuilders.termQuery(_ID_STRING, _ID_ONE).toString(), query.must().get(0).toString());
    // }
    //
    // public void testBuildGetRuleQuery_WithFilters() {
    // WlmRulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
    // BoolQueryBuilder query = rulePersistenceService.buildGetRuleQuery(null, ATTRIBUTE_MAP);
    // assertTrue(query.hasClauses());
    // assertEquals(1, query.must().size());
    // assertTrue(query.filter().contains(QueryBuilders.existsQuery(QueryGroupFeatureType.NAME)));
    // }
    //
    // public void testGetRule_WithId() {
    // WlmRulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
    // Client client = rulePersistenceService.getClient();
    // ActionListener<SearchResponse> listener = mock(ActionListener.class);
    // SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
    // SetupMocksForGetRule(client, searchRequestBuilder);
    //
    // rulePersistenceService.getRule(_ID_ONE, new HashMap<>(), null, listener);
    // verify(client).prepareSearch(WlmRulePersistenceService.RULES_INDEX);
    // verify(searchRequestBuilder).setQuery(any());
    // verify(searchRequestBuilder).execute(any());
    // }
    //
    // public void testGetRule_WithSearchAfter() {
    // WlmRulePersistenceService rulePersistenceService = setUpRulePersistenceService(new HashMap<>());
    // Client client = rulePersistenceService.getClient();
    // ActionListener<SearchResponse> listener = mock(ActionListener.class);
    // SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
    // SetupMocksForGetRule(client, searchRequestBuilder);
    // when(searchRequestBuilder.addSort(anyString(), any(SortOrder.class))).thenReturn(searchRequestBuilder);
    // when(searchRequestBuilder.searchAfter(any())).thenReturn(searchRequestBuilder);
    //
    // rulePersistenceService.getRule(null, new HashMap<>(), _ID_TWO, listener);
    // verify(searchRequestBuilder).addSort(_ID_STRING, SortOrder.ASC);
    // verify(searchRequestBuilder).searchAfter(new Object[] { _ID_TWO });
    // }
    //
    // public void SetupMocksForGetRule(Client client, SearchRequestBuilder searchRequestBuilder) {
    // when(client.prepareSearch(anyString())).thenReturn(searchRequestBuilder);
    // when(searchRequestBuilder.setQuery(any())).thenReturn(searchRequestBuilder);
    // when(searchRequestBuilder.setSize(anyInt())).thenReturn(searchRequestBuilder);
    // doAnswer(invocation -> {
    // ActionListener<SearchResponse> searchListener = invocation.getArgument(0);
    // searchListener.onResponse(mock(SearchResponse.class));
    // return null;
    // }).when(searchRequestBuilder).execute(any());
    // }
}
