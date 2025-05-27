/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.sync;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.AutoTaggingActionFilterTests;
import org.opensearch.plugin.wlm.WorkloadManagementPlugin;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.rule.sync.detect.AddRuleEvent;
import org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.opensearch.rule.GetRuleRequest;
import org.opensearch.rule.GetRuleResponse;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.rule.storage.XContentRuleParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RefreshBasedSyncMechanismTests extends OpenSearchTestCase {
    RefreshBasedSyncMechanism sut;

    Client mockClient;
    InMemoryRuleProcessingService ruleProcessingService;
    RulePersistenceService rulePersistenceService;
    AttributeValueStoreFactory attributeValueStoreFactory;
    ThreadPool mockThreadPool;
    Scheduler.Cancellable scheduledFuture;
    RuleEventClassifier ruleEventClassifier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            Settings settings = Settings.builder().put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000).build();
            ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(plugin.getSettings()));
            mockThreadPool = mock(ThreadPool.class);
            rulePersistenceService = mock(RulePersistenceService.class);
            ruleEventClassifier = mock(RuleEventClassifier.class);
            attributeValueStoreFactory = new AttributeValueStoreFactory(WorkloadGroupFeatureType.INSTANCE, DefaultAttributeValueStore::new);
            RuleEntityParser parser = new XContentRuleParser(WorkloadGroupFeatureType.INSTANCE);
            ruleProcessingService = mock(InMemoryRuleProcessingService.class);
            mockClient = mock(Client.class);
            when(ruleProcessingService.getAttributeValueStoreFactory()).thenReturn(attributeValueStoreFactory);
            scheduledFuture = mock(Scheduler.Cancellable.class);
            when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(scheduledFuture);

            sut = new RefreshBasedSyncMechanism(
                mockThreadPool,
                settings,
                clusterSettings,
                parser,
                ruleProcessingService,
                WorkloadGroupFeatureType.INSTANCE,
                rulePersistenceService,
                ruleEventClassifier
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
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Search failed"));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(rulePersistenceService, times(1)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(0)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * Test case for the doRun() method.
     * This test verifies that the doRun() method clears rules from the in-memory service
     * and attempts to refresh rules by executing a search request.
     */
    @SuppressWarnings("unchecked")
    public void test_doRun_RefreshesRules() {
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        Map<Attribute, Set<String>> attributeSetMap = Map.of(AutoTaggingActionFilterTests.TestAttribute.TEST_ATTRIBUTE, Set.of("test"));
        Rule rule = Rule.builder()
            .description("test description")
            .attributeMap(attributeSetMap)
            .featureType(AutoTaggingActionFilterTests.WLMFeatureType.WLM)
            .featureValue("test_value")
            .updatedAt("2025-05-27T08:58:57.558Z")
            .id("test_id")
            .build();

        when(getRuleResponse.getRules()).thenReturn(Map.of("test_id", rule));
        when(ruleEventClassifier.getRuleEvents(anySet())).thenReturn(List.of(new AddRuleEvent(rule)));
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(ruleProcessingService, times(1)).add(rule);
    }
}
