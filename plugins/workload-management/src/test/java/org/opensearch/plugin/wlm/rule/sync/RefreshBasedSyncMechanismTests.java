/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.sync;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.plugin.wlm.WorkloadManagementPlugin;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.rule.storage.XContentRuleParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RefreshBasedSyncMechanismTests extends OpenSearchTestCase {
    public static final String VALID_JSON = String.format(Locale.ROOT, """
        {
            "description": "%s",
            "workload_group": "feature value",
            "index_pattern": ["attribute_value_one", "attribute_value_two"],
            "updated_at": "%s"
        }
        """, "test description", Instant.now().toString());

    RefreshBasedSyncMechanism sut;

    Client mockClient;
    InMemoryRuleProcessingService ruleProcessingService;
    FeatureType featureType;
    AttributeValueStoreFactory attributeValueStoreFactory;
    ThreadPool mockThreadPool;
    Scheduler.Cancellable scheduledFuture;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            Settings settings = Settings.builder().put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000).build();
            ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(plugin.getSettings()));
            mockThreadPool = mock(ThreadPool.class);
            attributeValueStoreFactory = new AttributeValueStoreFactory(WorkloadGroupFeatureType.INSTANCE, DefaultAttributeValueStore::new);
            RuleEntityParser parser = new XContentRuleParser(WorkloadGroupFeatureType.INSTANCE);
            ruleProcessingService = mock(InMemoryRuleProcessingService.class);
            mockClient = mock(Client.class);
            when(ruleProcessingService.getAttributeValueStoreFactory()).thenReturn(attributeValueStoreFactory);
            scheduledFuture = mock(Scheduler.Cancellable.class);
            when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(scheduledFuture);

            sut = new RefreshBasedSyncMechanism(
                mockClient,
                mockThreadPool,
                settings,
                clusterSettings,
                parser,
                ruleProcessingService,
                WorkloadGroupFeatureType.INSTANCE
            );
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testDoStart() {
        sut.doStart();
        verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(), any(), any());
    }

    public void testDoStop() {
        sut.doStart();
        sut.doStop();
        verify(scheduledFuture, times(1)).cancel();
    }

    public void testDoClose() throws IOException {
        sut.doStart();
        sut.doClose();
        verify(scheduledFuture, times(1)).cancel();
    }

    /**
     * Tests the behavior of doRun when the search operation fails.
     * This test verifies that the method handles the failure case correctly
     * by logging the failure without throwing an exception.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunSearchFailure() {
        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        when(mockClient.prepareSearch(eq(WorkloadManagementPlugin.INDEX_NAME))).thenReturn(requestBuilder);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException("Search failed"));
            return null;
        }).when(requestBuilder).execute(any(ActionListener.class));

        sut.doRun();

        verify(mockClient).prepareSearch(eq(WorkloadManagementPlugin.INDEX_NAME));
        verify(ruleProcessingService, times(1)).getAttributeValueStoreFactory();
    }

    /**
     * Test case for the doRun() method.
     * This test verifies that the doRun() method clears rules from the in-memory service
     * and attempts to refresh rules by executing a search request.
     */
    @SuppressWarnings("unchecked")
    public void test_doRun_clearsAndRefreshesRules() {
        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        SearchResponse searchResponse = mock(SearchResponse.class);

        SearchHits searchHits = new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchHit hit = searchHits.getHits()[0];
        hit.sourceRef(new BytesArray(VALID_JSON));
        when(searchResponse.getHits()).thenReturn(searchHits);

        when(mockClient.prepareSearch(eq(WorkloadManagementPlugin.INDEX_NAME))).thenReturn(requestBuilder);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(0);
            listener.onResponse(searchResponse);
            return null;
        }).when(requestBuilder).execute(any(ActionListener.class));

        sut.doRun();

        verify(ruleProcessingService, times(1)).getAttributeValueStoreFactory();
        verify(ruleProcessingService, times(1)).add(any());
        verify(mockClient, times(1)).prepareSearch(WorkloadManagementPlugin.INDEX_NAME);
    }
}
