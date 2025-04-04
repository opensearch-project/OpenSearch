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
import org.opensearch.rule.RuleTestUtils;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import java.io.IOException;
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
import static org.opensearch.rule.RuleTestUtils.ruleOne;
import static org.opensearch.rule.RuleTestUtils.setUpIndexStoredRulePersistenceService;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class IndexStoredRulePersistenceServiceTests extends OpenSearchTestCase {

    public void testCreateIndexIfAbsent_IndexExists() {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        ClusterService clusterService = rulePersistenceService.getClusterService();
        when(clusterService.state().metadata().hasIndex(TEST_INDEX_NAME)).thenReturn(true);
        ActionListener<Boolean> listener = mock(ActionListener.class);
        rulePersistenceService.createIndexIfAbsent(listener);
        verify(listener).onResponse(true);
        verifyNoMoreInteractions(listener);
    }

    public void testCreateIndexIfAbsent() {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(mock(AdminClient.class));
        when(client.admin().indices()).thenReturn(indicesAdminClient);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(new CreateIndexResponse(true, true, TEST_INDEX_NAME)); // Assuming the index creation was successful
            return null;
        }).when(indicesAdminClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
        rulePersistenceService.createIndexIfAbsent(new ActionListener<>() {
            @Override
            public void onResponse(Boolean indexCreated) {
                assertTrue(indexCreated);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Index creation failed: " + e.getMessage());
            }
        });
        verify(indicesAdminClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
    }

    public void testPersistRuleSuccess() {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        IndexResponse indexResponse = new IndexResponse(new ShardId(TEST_INDEX_NAME, "uuid", 0), "id", 1, 1, 1, true);
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
        assertEquals(ruleOne, createRuleResponse.getRule());
    }

    public void testPersistRuleFailure() throws IOException {
        IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
        Client client = rulePersistenceService.getClient();
        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("Indexing failed"));
            return null;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));

        rulePersistenceService.persistRule(ruleOne, listener);
        verify(client).index(any(IndexRequest.class), any(ActionListener.class));
        verify(listener).onFailure(any(RuntimeException.class));
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

    // public void testCreateIndexIfAbsent_IndexExists() {
    // IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
    // ClusterService clusterService = rulePersistenceService.getClusterService();
    // when(clusterService.state().metadata().hasIndex(TEST_INDEX_NAME)).thenReturn(true);
    // ActionListener<Boolean> listener = mock(ActionListener.class);
    // rulePersistenceService.createIndexIfAbsent(listener);
    // verify(listener).onResponse(true);
    // verifyNoMoreInteractions(listener);
    // }
    //
    // public void testCreateIndexIfAbsent() {
    // IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
    // Client client = rulePersistenceService.getClient();
    // IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
    // when(client.admin()).thenReturn(mock(AdminClient.class));
    // when(client.admin().indices()).thenReturn(indicesAdminClient);
    // doAnswer(invocation -> {
    // ActionListener<CreateIndexResponse> listener = invocation.getArgument(1);
    // listener.onResponse(new CreateIndexResponse(true, true, TEST_INDEX_NAME)); // Assuming the index creation was successful
    // return null;
    // }).when(indicesAdminClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
    // rulePersistenceService.createIndexIfAbsent(new ActionListener<>() {
    // @Override
    // public void onResponse(Boolean indexCreated) {
    // assertTrue(indexCreated);
    // }
    //
    // @Override
    // public void onFailure(Exception e) {
    // fail("Index creation failed: " + e.getMessage());
    // }
    // });
    // verify(indicesAdminClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
    // }
    //
    // public void testPersistRuleSuccess() {
    // IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
    // Client client = rulePersistenceService.getClient();
    // ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
    // IndexResponse indexResponse = new IndexResponse(new ShardId(TEST_INDEX_NAME, "uuid", 0), "id", 1, 1, 1, true);
    // doAnswer(invocation -> {
    // ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
    // actionListener.onResponse(indexResponse);
    // return null;
    // }).when(client).index(any(IndexRequest.class), any(ActionListener.class));
    //
    // rulePersistenceService.persistRule(ruleOne, listener);
    // verify(client).index(any(IndexRequest.class), any(ActionListener.class));
    // ArgumentCaptor<CreateRuleResponse> responseCaptor = ArgumentCaptor.forClass(CreateRuleResponse.class);
    // verify(listener).onResponse(responseCaptor.capture());
    //
    // CreateRuleResponse createRuleResponse = responseCaptor.getValue();
    // assertNotNull(createRuleResponse);
    // assertEquals(ruleOne, createRuleResponse.getRule());
    // }
    //
    // public void testPersistRuleFailure() throws IOException {
    // IndexStoredRulePersistenceService rulePersistenceService = setUpIndexStoredRulePersistenceService(new HashMap<>());
    // Client client = rulePersistenceService.getClient();
    // ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
    // doAnswer(invocation -> {
    // ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
    // actionListener.onFailure(new RuntimeException("Indexing failed"));
    // return null;
    // }).when(client).index(any(IndexRequest.class), any(ActionListener.class));
    //
    // rulePersistenceService.persistRule(ruleOne, listener);
    // verify(client).index(any(IndexRequest.class), any(ActionListener.class));
    // verify(listener).onFailure(any(RuntimeException.class));
    // }
}
