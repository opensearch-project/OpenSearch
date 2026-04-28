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
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleQueryMapper;
import org.opensearch.rule.RuleUtils;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.opensearch.rule.XContentRuleParserTests.VALID_JSON;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_TWO;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.TEST_INDEX_NAME;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class IndexStoredRulePersistenceServiceTests extends OpenSearchTestCase {

    private static final int MAX_VALUES_PER_PAGE = 50;
    private Client client;
    private ClusterService clusterService;
    private RuleQueryMapper<QueryBuilder> ruleQueryMapper;
    private RuleEntityParser ruleEntityParser;
    private SearchRequestBuilder searchRequestBuilder;
    private RulePersistenceService rulePersistenceService;
    private QueryBuilder queryBuilder;
    private Rule rule;

    public void setUp() throws Exception {
        super.setUp();
        searchRequestBuilder = mock(SearchRequestBuilder.class);
        client = setUpMockClient(searchRequestBuilder);
        rule = mock(Rule.class);
        clusterService = mock(ClusterService.class);
        Settings testSettings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(testSettings, new HashSet<>());
        when(clusterService.getSettings()).thenReturn(testSettings);
        clusterSettings.registerSetting(IndexStoredRulePersistenceService.MAX_WLM_RULES_SETTING);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.hasIndex(TEST_INDEX_NAME)).thenReturn(true);
        ruleQueryMapper = mock(RuleQueryMapper.class);
        ruleEntityParser = mock(RuleEntityParser.class);
        queryBuilder = mock(QueryBuilder.class);
        when(queryBuilder.filter(any())).thenReturn(queryBuilder);
        when(ruleQueryMapper.from(any(GetRuleRequest.class))).thenReturn(queryBuilder);
        when(ruleQueryMapper.getCardinalityQuery()).thenReturn(mock(QueryBuilder.class));
        when(ruleEntityParser.parse(anyString())).thenReturn(rule);

        rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            clusterService,
            MAX_VALUES_PER_PAGE,
            ruleEntityParser,
            ruleQueryMapper
        );
    }

    public void testCreateRuleOnExistingIndex() throws Exception {
        CreateRuleRequest createRuleRequest = mock(CreateRuleRequest.class);
        when(createRuleRequest.getRule()).thenReturn(rule);
        when(rule.toXContent(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[] {}, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f));
        when(searchRequestBuilder.get()).thenReturn(searchResponse);

        IndexResponse indexResponse = mock(IndexResponse.class);
        when(indexResponse.getId()).thenReturn(_ID_ONE);
        ActionFuture<IndexResponse> future = mock(ActionFuture.class);
        when(future.get()).thenReturn(indexResponse);
        when(client.index(any(IndexRequest.class))).thenReturn(future);

        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        rulePersistenceService.createRule(createRuleRequest, listener);

        ArgumentCaptor<CreateRuleResponse> responseCaptor = ArgumentCaptor.forClass(CreateRuleResponse.class);
        verify(listener).onResponse(responseCaptor.capture());
        assertNotNull(responseCaptor.getValue().getRule());
    }

    public void testCardinalityCheckBasedFailure() throws Exception {
        CreateRuleRequest createRuleRequest = mock(CreateRuleRequest.class);
        when(createRuleRequest.getRule()).thenReturn(rule);
        when(rule.toXContent(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(
            new SearchHits(new SearchHit[] {}, new TotalHits(10000, TotalHits.Relation.EQUAL_TO), 1.0f)
        );
        when(searchRequestBuilder.get()).thenReturn(searchResponse);

        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        rulePersistenceService.createRule(createRuleRequest, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(OpenSearchRejectedExecutionException.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertNotNull(exceptionCaptor.getValue());
    }

    public void testConcurrentCreateDuplicateRules() throws InterruptedException {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        Set<String> storedAttributeMaps = ConcurrentHashMap.newKeySet();

        CreateRuleRequest createRuleRequest = mock(CreateRuleRequest.class);
        when(rule.getAttributeMap()).thenReturn(ATTRIBUTE_MAP);
        when(rule.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);
        when(createRuleRequest.getRule()).thenReturn(rule);

        RulePersistenceService rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            clusterService,
            MAX_VALUES_PER_PAGE,
            ruleEntityParser,
            ruleQueryMapper
        ) {
            @Override
            public void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
                singleThreadExecutor.execute(() -> {
                    Rule rule = request.getRule();
                    validateNoDuplicateRule(rule, new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {
                            synchronized (storedAttributeMaps) {
                                storedAttributeMaps.add(MOCK_RULE_ATTRIBUTE_ONE.getName());
                            }
                            listener.onResponse(new CreateRuleResponse(rule));
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                            latch.countDown();
                        }
                    });
                });
            }

            public void validateNoDuplicateRule(Rule rule, ActionListener<Void> listener) {
                synchronized (storedAttributeMaps) {
                    if (storedAttributeMaps.contains(MOCK_RULE_ATTRIBUTE_ONE.getName())) {
                        listener.onFailure(new IllegalArgumentException("Duplicate rule exists with attribute map"));
                    } else {
                        listener.onResponse(null);
                    }
                }
            }
        };

        class TestListener implements ActionListener<CreateRuleResponse> {
            final AtomicInteger successCount = new AtomicInteger();
            final AtomicInteger failureCount = new AtomicInteger();
            final List<Exception> failures = Collections.synchronizedList(new ArrayList<>());

            @Override
            public void onResponse(CreateRuleResponse response) {
                successCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                failures.add(e);
            }
        }
        TestListener testListener = new TestListener();

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> rulePersistenceService.createRule(createRuleRequest, testListener)).start();
        }
        boolean completed = latch.await(10, TimeUnit.SECONDS);
        singleThreadExecutor.shutdown();
        assertTrue("All create calls should complete", completed);
        assertEquals(1, testListener.successCount.get());
        assertEquals(threadCount - 1, testListener.failureCount.get());
        for (Exception e : testListener.failures) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("Duplicate rule"));
        }
    }

    public void testCreateDuplicateRule() {
        CreateRuleRequest createRuleRequest = mock(CreateRuleRequest.class);
        when(createRuleRequest.getRule()).thenReturn(rule);
        when(rule.getAttributeMap()).thenReturn(Map.of(MOCK_RULE_ATTRIBUTE_ONE, Set.of(ATTRIBUTE_VALUE_ONE)));
        when(rule.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);

        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHit hit = new SearchHit(1);
        hit.sourceRef(new BytesArray(VALID_JSON));
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchRequestBuilder.get()).thenReturn(searchResponse);

        ActionListener<CreateRuleResponse> listener = mock(ActionListener.class);
        when(ruleEntityParser.parse(any(String.class))).thenReturn(rule);
        rulePersistenceService.createRule(createRuleRequest, listener);
        ArgumentCaptor<Exception> failureCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(failureCaptor.capture());
    }

    public void testGetRuleByIdSuccess() {
        GetRuleRequest getRuleRequest = mock(GetRuleRequest.class);
        when(getRuleRequest.getId()).thenReturn(_ID_ONE);
        when(getRuleRequest.getAttributeFilters()).thenReturn(new HashMap<>());
        when(getRuleRequest.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);

        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHit searchHit = new SearchHit(1);
        searchHit.sourceRef(new BytesArray(VALID_JSON));
        SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchRequestBuilder.get()).thenReturn(searchResponse);

        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);
        ArgumentCaptor<GetRuleResponse> responseCaptor = ArgumentCaptor.forClass(GetRuleResponse.class);
        rulePersistenceService.getRule(getRuleRequest, listener);
        verify(listener).onResponse(responseCaptor.capture());
        GetRuleResponse response = responseCaptor.getValue();
        assertEquals(1, response.getRules().size());
    }

    public void testGetRuleByIdNotFound() {
        GetRuleRequest getRuleRequest = mock(GetRuleRequest.class);
        when(getRuleRequest.getId()).thenReturn(_ID_ONE);
        when(getRuleRequest.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchRequestBuilder.get()).thenReturn(searchResponse);
        when(searchResponse.getHits()).thenReturn(new SearchHits(new SearchHit[] {}, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f));
        ActionListener<GetRuleResponse> listener = mock(ActionListener.class);

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

    public void testDeleteRule_successful() {
        String ruleId = "test-rule-id";
        DeleteRuleRequest request = new DeleteRuleRequest(ruleId, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<ActionListener<org.opensearch.action.delete.DeleteResponse>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );

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
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        ArgumentCaptor<ActionListener<org.opensearch.action.delete.DeleteResponse>> listenerCaptor = ArgumentCaptor.forClass(
            ActionListener.class
        );

        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        rulePersistenceService.deleteRule(request, listener);

        verify(client).delete(requestCaptor.capture(), listenerCaptor.capture());
        assertEquals(ruleId, requestCaptor.getValue().id());

        listenerCaptor.getValue().onFailure(new DocumentMissingException(new ShardId(TEST_INDEX_NAME, "_na_", 0), ruleId));

        verify(listener).onFailure(any(ResourceNotFoundException.class));
    }

    public void testConcurrentUpdateDuplicateRules() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        Set<String> storedAttributeMaps = ConcurrentHashMap.newKeySet();

        UpdateRuleRequest updateRuleRequest = mock(UpdateRuleRequest.class);
        when(updateRuleRequest.getFeatureValue()).thenReturn(FEATURE_VALUE_ONE);
        when(updateRuleRequest.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);
        when(updateRuleRequest.getAttributeMap()).thenReturn(ATTRIBUTE_MAP);
        when(updateRuleRequest.getDescription()).thenReturn(DESCRIPTION_TWO);

        Rule originalRule = mock(Rule.class);
        when(originalRule.getId()).thenReturn(_ID_ONE);
        when(originalRule.getAttributeMap()).thenReturn(ATTRIBUTE_MAP);
        when(originalRule.getFeatureType()).thenReturn(RuleTestUtils.MockRuleFeatureType.INSTANCE);
        when(originalRule.getFeatureValue()).thenReturn(FEATURE_VALUE_ONE);
        when(originalRule.getDescription()).thenReturn(DESCRIPTION_ONE);

        RulePersistenceService rulePersistenceService = new IndexStoredRulePersistenceService(
            TEST_INDEX_NAME,
            client,
            clusterService,
            MAX_VALUES_PER_PAGE,
            ruleEntityParser,
            ruleQueryMapper
        ) {
            @Override
            public void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
                singleThreadExecutor.execute(() -> {
                    Rule updatedRule = RuleUtils.composeUpdatedRule(originalRule, request, request.getFeatureType());
                    validateNoDuplicateRule(updatedRule, new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void unused) {
                            listener.onResponse(new UpdateRuleResponse(updatedRule));
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                            latch.countDown();
                        }
                    });
                });
            }

            public void validateNoDuplicateRule(Rule rule, ActionListener<Void> listener) {
                synchronized (storedAttributeMaps) {
                    String key = rule.getAttributeMap().toString();
                    if (storedAttributeMaps.contains(key)) {
                        listener.onFailure(new IllegalArgumentException("Duplicate rule exists with attribute map"));
                    } else {
                        storedAttributeMaps.add(key);
                        listener.onResponse(null);
                    }
                }
            }
        };

        class TestListener implements ActionListener<UpdateRuleResponse> {
            final AtomicInteger successCount = new AtomicInteger();
            final AtomicInteger failureCount = new AtomicInteger();
            final List<Exception> failures = Collections.synchronizedList(new ArrayList<>());

            @Override
            public void onResponse(UpdateRuleResponse response) {
                successCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                failures.add(e);
            }
        }

        TestListener testListener = new TestListener();

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> rulePersistenceService.updateRule(updateRuleRequest, testListener)).start();
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        singleThreadExecutor.shutdown();

        assertTrue("All update calls should complete", completed);
        assertEquals(1, testListener.successCount.get());
        assertEquals(threadCount - 1, testListener.failureCount.get());

        for (Exception e : testListener.failures) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("Duplicate rule"));
        }
    }

    public void testUpdateRule_RuleNotFound() {
        UpdateRuleRequest request = mock(UpdateRuleRequest.class);
        when(request.getId()).thenReturn(_ID_ONE);

        SearchHits emptyHits = new SearchHits(new SearchHit[] {}, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(emptyHits);
        when(searchRequestBuilder.get()).thenReturn(searchResponse);

        AtomicReference<Exception> failure = new AtomicReference<>();
        rulePersistenceService.updateRule(request, new ActionListener<>() {
            @Override
            public void onResponse(UpdateRuleResponse updateRuleResponse) {
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });

        assertTrue(failure.get() instanceof ResourceNotFoundException);
        assertTrue(failure.get().getMessage().contains(_ID_ONE));
    }

    public void testUpdateRule_ParseFailure() {
        UpdateRuleRequest request = mock(UpdateRuleRequest.class);
        when(request.getId()).thenReturn(_ID_ONE);

        SearchHit searchHit = new SearchHit(1, _ID_ONE, null, null);
        searchHit.sourceRef(new BytesArray(VALID_JSON));
        SearchHits searchHits = new SearchHits(new SearchHit[] { searchHit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchRequestBuilder.get()).thenReturn(searchResponse);
        when(ruleEntityParser.parse(anyString())).thenThrow(new RuntimeException("Invalid JSON"));

        AtomicReference<Exception> failure = new AtomicReference<>();
        rulePersistenceService.updateRule(request, new ActionListener<>() {
            @Override
            public void onResponse(UpdateRuleResponse updateRuleResponse) {
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        });

        assertNotNull(failure.get());
        assertTrue(failure.get().getMessage().contains("Invalid JSON"));
    }
}
