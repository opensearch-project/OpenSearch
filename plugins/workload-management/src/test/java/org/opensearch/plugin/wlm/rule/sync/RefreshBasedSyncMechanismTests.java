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
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.plugin.wlm.AutoTaggingActionFilterTests;
import org.opensearch.plugin.wlm.WlmClusterSettingValuesProvider;
import org.opensearch.plugin.wlm.WorkloadManagementPlugin;
import org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.ArgumentCaptor;

import static org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifierTests.getRandomRule;
import static org.mockito.ArgumentMatchers.any;
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
    FeatureType featureType;
    WlmClusterSettingValuesProvider nonPluginSettingValuesProvider;
    ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            Settings settings = Settings.builder()
                .put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000)
                .put(WorkloadManagementSettings.WLM_MODE_SETTING_NAME, "enabled")
                .build();
            clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(plugin.getSettings()));
            clusterSettings.registerSetting(WorkloadManagementSettings.WLM_MODE_SETTING);
            featureType = mock(FeatureType.class);
            mockThreadPool = mock(ThreadPool.class);
            ruleProcessingService = mock(InMemoryRuleProcessingService.class);
            rulePersistenceService = mock(RulePersistenceService.class);
            ruleEventClassifier = new RuleEventClassifier(Collections.emptySet(), ruleProcessingService);
            attributeValueStoreFactory = new AttributeValueStoreFactory(featureType, DefaultAttributeValueStore::new);
            nonPluginSettingValuesProvider = new WlmClusterSettingValuesProvider(settings, clusterSettings);
            mockClient = mock(Client.class);
            scheduledFuture = mock(Scheduler.Cancellable.class);
            when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(scheduledFuture);

            sut = new RefreshBasedSyncMechanism(
                mockThreadPool,
                settings,
                featureType,
                rulePersistenceService,
                ruleEventClassifier,
                nonPluginSettingValuesProvider
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
     * Tests the behavior of doRun when the WLM mode is disabled.
     * This test verifies that the method does not perform any operations
     * when the WLM mode is set to DISABLED.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunWhenWLM_isDisabled() {
        Settings disabledSettings = Settings.builder()
            .put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000)
            .put(WorkloadManagementSettings.WLM_MODE_SETTING_NAME, "disabled")
            .build();
        WlmClusterSettingValuesProvider disabledWlmModeProvider = new WlmClusterSettingValuesProvider(disabledSettings, clusterSettings);
        sut = new RefreshBasedSyncMechanism(
            mockThreadPool,
            disabledSettings,
            featureType,
            rulePersistenceService,
            ruleEventClassifier,
            disabledWlmModeProvider
        );
        sut.doRun();
        verify(rulePersistenceService, times(0)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
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

        when(getRuleResponse.getRules()).thenReturn(List.of(rule));
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(ruleProcessingService, times(1)).add(rule);
    }

    @SuppressWarnings("unchecked")
    public void test_doRun_RefreshesRulesAndCheckInMemoryView() {
        GetRuleResponse getRuleResponse = mock(GetRuleResponse.class);
        List<Rule> existingRules = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final String randomRuleId = randomAlphaOfLength(5);
            existingRules.add(getRandomRule(randomRuleId));
        }

        when(getRuleResponse.getRules()).thenReturn(existingRules);
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        // marks the first run of service
        sut.doRun();

        Set<Rule> previousRules = new HashSet<>(existingRules);

        Set<Rule> newRules = new HashSet<>();

        int deletionEventCount = 10;
        // Mark some deletions
        for (Rule rule : existingRules) {
            if (randomBoolean()) {
                deletionEventCount--;
                newRules.add(rule);
            }
        }

        // add new rule
        newRules.add(getRandomRule("10"));

        int updateEventCount = 0;
        // Update some rules
        for (Rule rule : previousRules) {
            if (randomBoolean() && !newRules.contains(rule)) {
                updateEventCount++;
                // since we are updating a new existing rule but we have marked it for deletion above hence decrement it
                deletionEventCount--;
                newRules.add(getRandomRule(rule.getId()));
            }
        }

        when(getRuleResponse.getRules()).thenReturn(new ArrayList<>(newRules));
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(getRuleResponse);
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(ruleProcessingService, times(deletionEventCount + updateEventCount)).remove(any(Rule.class));
        // Here 1 is due to add in the second run and 10 for adding 10 rules as part of first run
        verify(ruleProcessingService, times(updateEventCount + 1 + 10)).add(any(Rule.class));
    }

    @SuppressWarnings("unchecked")
    public void testDoRunIgnoresIndexNotFoundException() {
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException("rules index not found"));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        sut.doRun();
        verify(rulePersistenceService, times(1)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(0)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * Tests that doRun follows search_after pagination and applies the rules from all pages,
     * not just the first page.
     */
    @SuppressWarnings("unchecked")
    public void test_doRun_FollowsPaginationAndAppliesAllPages() {
        List<Rule> firstPageRules = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            firstPageRules.add(ruleWithId("rule-" + i));
        }
        List<Rule> secondPageRules = new ArrayList<>();
        for (int i = 50; i < 60; i++) {
            secondPageRules.add(ruleWithId("rule-" + i));
        }

        AtomicInteger callCount = new AtomicInteger();
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            if (callCount.getAndIncrement() == 0) {
                listener.onResponse(new GetRuleResponse(firstPageRules, "rule-49"));
            } else {
                listener.onResponse(new GetRuleResponse(secondPageRules, null));
            }
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        ArgumentCaptor<GetRuleRequest> requestCaptor = ArgumentCaptor.forClass(GetRuleRequest.class);
        verify(rulePersistenceService, times(2)).getRule(requestCaptor.capture(), any(ActionListener.class));
        assertNull(requestCaptor.getAllValues().get(0).getSearchAfter());
        assertEquals("rule-49", requestCaptor.getAllValues().get(1).getSearchAfter());
        verify(ruleProcessingService, times(60)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * Tests that doRun applies nothing when a later page fails, instead of applying
     * the partial rule set from the earlier pages.
     */
    @SuppressWarnings("unchecked")
    public void test_doRun_SkipsRunWhenLaterPageFails() {
        List<Rule> firstPageRules = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            firstPageRules.add(ruleWithId("rule-" + i));
        }

        AtomicInteger callCount = new AtomicInteger();
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            if (callCount.getAndIncrement() == 0) {
                listener.onResponse(new GetRuleResponse(firstPageRules, "rule-49"));
            } else {
                listener.onFailure(new RuntimeException("Search failed"));
            }
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(rulePersistenceService, times(2)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(0)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * Tests that doRun stops paging when a page returns the same search_after token it was
     * called with, instead of fetching forever. Against the old recursive implementation
     * this test recurses until StackOverflowError.
     */
    @SuppressWarnings("unchecked")
    public void test_doRun_StopsWhenSearchAfterDoesNotAdvance() {
        List<Rule> firstPageRules = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            firstPageRules.add(ruleWithId("rule-" + i));
        }
        List<Rule> secondPageRules = new ArrayList<>();
        for (int i = 50; i < 60; i++) {
            secondPageRules.add(ruleWithId("rule-" + i));
        }

        AtomicInteger callCount = new AtomicInteger();
        doAnswer(invocation -> {
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            if (callCount.getAndIncrement() == 0) {
                listener.onResponse(new GetRuleResponse(firstPageRules, "rule-49"));
            } else {
                // Backend returns the same token again: no progress.
                listener.onResponse(new GetRuleResponse(secondPageRules, "rule-49"));
            }
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        // Only two pages are fetched; the stalled token does not trigger a third call.
        verify(rulePersistenceService, times(2)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        // The rules from both fetched pages are still applied.
        verify(ruleProcessingService, times(60)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    private static Rule ruleWithId(String id) {
        return Rule.builder()
            .id(id)
            .description("description for " + id)
            .attributeMap(Map.of(AutoTaggingActionFilterTests.TestAttribute.TEST_ATTRIBUTE, Set.of("value for " + id)))
            .featureType(AutoTaggingActionFilterTests.WLMFeatureType.WLM)
            .featureValue("feature value for " + id)
            .updatedAt("2025-05-27T08:58:57.558Z")
            .build();
    }
}
