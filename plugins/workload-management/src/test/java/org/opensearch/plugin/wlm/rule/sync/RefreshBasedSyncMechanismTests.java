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

import static org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifierTests.getRandomRule;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
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
            // getAllRules is the interface's paging loop over getRule; run it for real so tests stub getRule pages.
            doCallRealMethod().when(rulePersistenceService).getAllRules(any(), any());
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
     * Rules spread over several pages are all applied, by following search_after until it is null.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunAppliesRulesFromAllPages() {
        Rule r1 = getRandomRule("id1");
        Rule r2 = getRandomRule("id2");
        Rule r3 = getRandomRule("id3");
        Map<String, GetRuleResponse> pages = Map.of(
            "",
            new GetRuleResponse(List.of(r1, r2), "id2"),
            "id2",
            new GetRuleResponse(List.of(r3), null)
        );
        List<String> requestedCursors = new ArrayList<>();
        doAnswer(invocation -> {
            GetRuleRequest request = invocation.getArgument(0);
            String cursor = request.getSearchAfter() == null ? "" : request.getSearchAfter();
            requestedCursors.add(cursor);
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(pages.get(cursor));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        assertEquals(List.of("", "id2"), requestedCursors);
        verify(ruleProcessingService, times(1)).add(r1);
        verify(ruleProcessingService, times(1)).add(r2);
        verify(ruleProcessingService, times(1)).add(r3);
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * If a later page fails, nothing from that run is applied, so rules on unread pages are not treated as deleted.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunAppliesNothingWhenALaterPageFails() {
        Rule r1 = getRandomRule("id1");
        doAnswer(invocation -> {
            GetRuleRequest request = invocation.getArgument(0);
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            if (request.getSearchAfter() == null) {
                listener.onResponse(new GetRuleResponse(List.of(r1), "id1"));
            } else {
                listener.onFailure(new RuntimeException("search failed"));
            }
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));

        sut.doRun();

        verify(rulePersistenceService, times(2)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(0)).add(any(Rule.class));
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * When the rule count is an exact multiple of the page size, the last full page still returns a cursor and the next
     * page is empty; every rule is applied.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunHandlesEmptyLastPage() {
        Rule r1 = getRandomRule("id1");
        Rule r2 = getRandomRule("id2");
        stubPages(Map.of("", new GetRuleResponse(List.of(r1, r2), "id2"), "id2", new GetRuleResponse(List.of(), null)));

        sut.doRun();

        verify(rulePersistenceService, times(2)).getRule(any(GetRuleRequest.class), any(ActionListener.class));
        verify(ruleProcessingService, times(1)).add(r1);
        verify(ruleProcessingService, times(1)).add(r2);
    }

    /**
     * A rule deleted from a later page is removed on the next run, and rules on other pages are left alone.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunRemovesRuleDeletedFromLaterPage() {
        Rule r1 = getRandomRule("id1");
        Rule r2 = getRandomRule("id2");
        Rule r3 = getRandomRule("id3");
        stubPages(Map.of("", new GetRuleResponse(List.of(r1, r2), "id2"), "id2", new GetRuleResponse(List.of(r3), null)));
        sut.doRun();

        stubPages(Map.of("", new GetRuleResponse(List.of(r1, r2), "id2"), "id2", new GetRuleResponse(List.of(), null)));
        sut.doRun();

        verify(ruleProcessingService, times(1)).remove(r3);
        verify(ruleProcessingService, times(0)).remove(r1);
        verify(ruleProcessingService, times(0)).remove(r2);
    }

    /**
     * If paging stops making progress, the previously applied rules are kept rather than treated as deleted.
     */
    @SuppressWarnings("unchecked")
    public void testDoRunKeepsRulesWhenPagingStopsMakingProgress() {
        Rule r1 = getRandomRule("id1");
        Rule r2 = getRandomRule("id2");
        stubPages(Map.of("", new GetRuleResponse(List.of(r1), "id1"), "id1", new GetRuleResponse(List.of(r2), null)));
        sut.doRun();

        // Now the second page repeats the cursor without adding anything new.
        stubPages(Map.of("", new GetRuleResponse(List.of(r1), "id1"), "id1", new GetRuleResponse(List.of(r1), "id1")));
        sut.doRun();

        verify(ruleProcessingService, times(1)).add(r1);
        verify(ruleProcessingService, times(1)).add(r2);
        verify(ruleProcessingService, times(0)).remove(any(Rule.class));
    }

    /**
     * Stubs getRule to return the page for each request's search_after ("" for the first page).
     */
    @SuppressWarnings("unchecked")
    private void stubPages(Map<String, GetRuleResponse> pages) {
        doAnswer(invocation -> {
            GetRuleRequest request = invocation.getArgument(0);
            ActionListener<GetRuleResponse> listener = invocation.getArgument(1);
            listener.onResponse(pages.get(request.getSearchAfter() == null ? "" : request.getSearchAfter()));
            return null;
        }).when(rulePersistenceService).getRule(any(GetRuleRequest.class), any(ActionListener.class));
    }
}
