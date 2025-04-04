/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("unchecked")
public class IndexStoredRulePersistenceServiceTests extends OpenSearchTestCase {

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
