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
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.DeleteRuleRequest;
import org.opensearch.rule.GetRuleRequest;
import org.opensearch.rule.GetRuleResponse;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleQueryMapper;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.HashMap;

import org.mockito.ArgumentCaptor;

import static org.opensearch.rule.XContentRuleParserTests.VALID_JSON;
import static org.opensearch.rule.RuleTestUtils.TEST_INDEX_NAME;
import static org.opensearch.rule.RuleTestUtils._ID_ONE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.opensearch.rule.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.RuleTestUtils.TEST_INDEX_NAME;
import static org.opensearch.rule.RuleTestUtils._ID_ONE;
import static org.opensearch.rule.RuleTestUtils.setUpIndexStoredRulePersistenceService;
import static org.opensearch.rule.utils.IndexStoredRuleParserTests.VALID_JSON;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class IndexStoredRulePersistenceServiceTests extends OpenSearchTestCase {

    public void testGetRuleByIdSuccess() {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);
        SearchHit hit = searchHits.getHits()[0];
        hit.sourceRef(new BytesArray(VALID_JSON));

        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        rulePersistenceService.handleGetRuleResponse(_ID_ONE, searchResponse, listener);

        ArgumentCaptor<GetRuleResponse> responseCaptor = ArgumentCaptor.forClass(GetRuleResponse.class);
        verify(listener).onResponse(responseCaptor.capture());
        GetRuleResponse response = responseCaptor.getValue();
        assertEquals(response.getRules().size(), 1);
        assertEquals(RestStatus.OK, response.getRestStatus());
    }

    public void testGetRuleByIdNotFound() {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[] {}, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f));

        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        rulePersistenceService.handleGetRuleResponse(_ID_ONE, searchResponse, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception exception = exceptionCaptor.getValue();
        assertTrue(exception instanceof ResourceNotFoundException);
    }

    public void testGetRuleWithAttributes() {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        SearchRequestBuilder searchRequestBuilder = mock(SearchRequestBuilder.class);
        Client client = rulePersistenceService.getClient();
        when(client.prepareSearch(TEST_INDEX_NAME)).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setQuery(any(QueryBuilder.class))).thenReturn(searchRequestBuilder);
        when(searchRequestBuilder.setSize(anyInt())).thenReturn(searchRequestBuilder);
        rulePersistenceService.getRuleFromIndex(null, ATTRIBUTE_MAP, null, listener);
        verify(client).prepareSearch(TEST_INDEX_NAME);
        verify(searchRequestBuilder).setQuery(any());
    }

    public void testDeleteRule_successful() {
        String ruleId = "test-rule-id";
        DeleteRuleRequest request = new DeleteRuleRequest(ruleId, RuleTestUtils.MockRuleFeatureType.INSTANCE);

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        RulePersistenceService rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            MAX_VALUES_PER_PAGE,
            mock(RuleEntityParser.class),
            mock(RuleQueryMapper.class)
        );

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<ActionListener<org.opensearch.action.delete.DeleteResponse>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        rulePersistenceService.deleteRule(request, listener);

        verify(client).delete(requestCaptor.capture(), listenerCaptor.capture());
        assertEquals(ruleId, requestCaptor.getValue().id());

        org.opensearch.action.delete.DeleteResponse deleteResponse = mock(org.opensearch.action.delete.DeleteResponse.class);
        when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);

        listenerCaptor.getValue().onResponse(deleteResponse);

        verify(listener).onResponse(argThat(AcknowledgedResponse::isAcknowledged));
    }

    public void testDeleteRule_notFound() {
        String ruleId = "missing-rule-id";
        DeleteRuleRequest request = new DeleteRuleRequest(ruleId, RuleTestUtils.MockRuleFeatureType.INSTANCE);

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        RulePersistenceService rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            MAX_VALUES_PER_PAGE,
            mock(RuleEntityParser.class),
            mock(RuleQueryMapper.class)
        );

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<ActionListener<org.opensearch.action.delete.DeleteResponse>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        rulePersistenceService.deleteRule(request, listener);

        verify(client).delete(requestCaptor.capture(), listenerCaptor.capture());
        assertEquals(ruleId, requestCaptor.getValue().id());

        listenerCaptor.getValue().onFailure(new DocumentMissingException(new ShardId(TEST_INDEX_NAME, "_na_", 0), ruleId));

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }
}
