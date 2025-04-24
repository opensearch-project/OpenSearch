/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.apache.lucene.search.TotalHits;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.GetRuleRequest;
import org.opensearch.rule.GetRuleResponse;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleQueryMapper;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.HashMap;

import org.mockito.ArgumentCaptor;

import static org.opensearch.rule.XContentRuleParserTests.VALID_JSON;
import static org.opensearch.rule.utils.RuleTestUtils.TEST_INDEX_NAME;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class IndexStoredRulePersistenceServiceTests extends OpenSearchTestCase {

    public static final int MAX_VALUES_PER_PAGE = 50;

    public void testGetRuleByIdSuccess() {
        GetRuleRequest getRuleRequest = mock(GetRuleRequest.class);
        when(getRuleRequest.getId()).thenReturn(_ID_ONE);
        when(getRuleRequest.getAttributeFilters()).thenReturn(new HashMap<>());
        QueryBuilder queryBuilder = mock(QueryBuilder.class);
        RuleQueryMapper<QueryBuilder> mockRuleQueryMapper = mock(RuleQueryMapper.class);
        RuleEntityParser mockRuleEntityParser = mock(RuleEntityParser.class);
        Rule mockRule = mock(Rule.class);

        when(mockRuleEntityParser.parse(anyString())).thenReturn(mockRule);
        when(mockRuleQueryMapper.from(getRuleRequest)).thenReturn(queryBuilder);
        when(queryBuilder.filter(any())).thenReturn(queryBuilder);

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        Client client = setUpMockClient(searchRequestBuilder);

        RulePersistenceService rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            MAX_VALUES_PER_PAGE,
            mockRuleEntityParser,
            mockRuleQueryMapper
        );

        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);
        SearchHit hit = searchHits.getHits()[0];
        hit.sourceRef(new BytesArray(VALID_JSON));

        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);

        doAnswer((invocation) -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(0);
            actionListener.onResponse(searchResponse);
            return null;
        }).when(searchRequestBuilder).execute(any(ActionListener.class));

        when(getRuleRequest.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);
        rulePersistenceService.getRule(getRuleRequest, listener);

        ArgumentCaptor<GetRuleResponse> responseCaptor = ArgumentCaptor.forClass(GetRuleResponse.class);
        verify(listener).onResponse(responseCaptor.capture());
        GetRuleResponse response = responseCaptor.getValue();
        assertEquals(response.getRules().size(), 1);
    }

    public void testGetRuleByIdNotFound() {
        GetRuleRequest getRuleRequest = mock(GetRuleRequest.class);
        when(getRuleRequest.getId()).thenReturn(_ID_ONE);
        QueryBuilder queryBuilder = mock(QueryBuilder.class);
        RuleQueryMapper<QueryBuilder> mockRuleQueryMapper = mock(RuleQueryMapper.class);
        RuleEntityParser mockRuleEntityParser = mock(RuleEntityParser.class);
        Rule mockRule = mock(Rule.class);

        when(mockRuleEntityParser.parse(anyString())).thenReturn(mockRule);
        when(mockRuleQueryMapper.from(getRuleRequest)).thenReturn(queryBuilder);
        when(queryBuilder.filter(any())).thenReturn(queryBuilder);

        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        Client client = setUpMockClient(searchRequestBuilder);

        RulePersistenceService rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            MAX_VALUES_PER_PAGE,
            mockRuleEntityParser,
            mockRuleQueryMapper
        );

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[] {}, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f));

        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);

        doAnswer(invocationOnMock -> {
            ActionListener<SearchResponse> actionListener = invocationOnMock.getArgument(0);
            actionListener.onResponse(searchResponse);
            return null;
        }).when(searchRequestBuilder).execute(any(ActionListener.class));

        when(getRuleRequest.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);
        rulePersistenceService.getRule(getRuleRequest, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception exception = exceptionCaptor.getValue();
        assertTrue(exception instanceof ResourceNotFoundException);
    }

    private Client setUpMockClient(SearchRequestBuilder searchRequestBuilder) {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        ThreadPool threadPool = mock(ThreadPool.class);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        when(client.prepareSearch(TEST_INDEX_NAME)).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setSize(anyInt())).thenReturn(searchRequestBuilder);

        return client;
    }
}
