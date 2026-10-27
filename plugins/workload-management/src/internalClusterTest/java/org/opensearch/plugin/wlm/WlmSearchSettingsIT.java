/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.TermsLookup;
import org.opensearch.plugin.wlm.WlmNodeThrottlingIT.ScriptedBlockPlugin;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.action.search.RestMultiSearchAction;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.RuleFrameworkPlugin;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.RuleRoutingServiceRegistry;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupSearchSettings;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * End-to-end integration test for the search settings a workload group applies to the searches assigned to it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class WlmSearchSettingsIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String PUT = "PUT";
    private static final String DELETE = "DELETE";
    private static final String WORKLOAD_GROUP_ID = "wlm_cancel_after_group";
    private static final String OTHER_WORKLOAD_GROUP_ID = "wlm_other_group";
    private static final String INDEX_NAME = "cancel_after_index";
    private static final String OTHER_INDEX_NAME = "other_index";
    private static final String LOOKUP_INDEX_NAME = "lookup_index";
    private static final String CANCEL_AFTER_TIME_INTERVAL = WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey();
    private static final String OVERRIDE_REQUEST_VALUES = WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES.getKey();
    private static final TimeValue GROUP_CANCEL_AFTER = TimeValue.timeValueSeconds(1);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(WlmAutoTaggingIT.TestWorkloadManagementPlugin.class);
        plugins.add(RuleFrameworkPlugin.class);
        plugins.add(ScriptedBlockPlugin.class);
        return plugins;
    }

    @Before
    public void registerFeatureTypeIfMissingOnAllNodes() {
        // The registry is JVM-static but each test gets a new cluster, so re-register this cluster's feature type.
        AutoTaggingRegistry.featureTypesRegistryMap.remove(WorkloadGroupFeatureType.NAME);
        FeatureType featureType = WlmAutoTaggingIT.TestWorkloadManagementPlugin.featureType;
        AutoTaggingRegistry.registerFeatureType(featureType);

        for (String node : internalCluster().getNodeNames()) {
            RulePersistenceServiceRegistry persistenceRegistry = internalCluster().getInstance(RulePersistenceServiceRegistry.class, node);
            RuleRoutingServiceRegistry routingRegistry = internalCluster().getInstance(RuleRoutingServiceRegistry.class, node);
            try {
                routingRegistry.getRuleRoutingService(featureType);
            } catch (IllegalArgumentException ex) {
                persistenceRegistry.register(featureType, WlmAutoTaggingIT.TestWorkloadManagementPlugin.rulePersistenceService);
                routingRegistry.register(featureType, WlmAutoTaggingIT.TestWorkloadManagementPlugin.ruleRoutingService);
            }
        }
    }

    @After
    public void clearWlmModeSetting() {
        Settings.Builder builder = Settings.builder().putNull(WorkloadManagementSettings.WLM_MODE_SETTING.getKey());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder).get());
    }

    public void testGroupCancelAfterTimeIntervalCancelsSearch() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        // Neither the request nor the cluster sets search.cancel_after_time_interval, so only the group's value can cancel it.
        assertCancelledByGroup(client().prepareSearch(INDEX_NAME));
    }

    public void testRequestThatTurnsOffCancellationIsNotCancelledWithoutOverride() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        // Without override_request_values the request's own value wins, and -1 turns off its cancellation.
        assertNotCancelledByGroup(client().prepareSearch(INDEX_NAME).setCancelAfterTimeInterval(SearchService.NO_TIMEOUT));
    }

    public void testGroupThatOverridesRequestValuesCancelsSearchWhateverTheRequestSet() throws Exception {
        createWorkloadGroupForIndex(
            Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).put(OVERRIDE_REQUEST_VALUES, true).build()
        );
        assertCancelledByGroup(client().prepareSearch(INDEX_NAME).setCancelAfterTimeInterval(TimeValue.timeValueHours(1)));
        assertCancelledByGroup(client().prepareSearch(INDEX_NAME).setCancelAfterTimeInterval(SearchService.NO_TIMEOUT));
    }

    public void testEachSubSearchOfAMultiSearchGetsItsOwnTimer() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        // Each sub-search consults the resolver in its own doExecute. Without override_request_values a sub-search's own interval
        // wins, so the sub-search that turns its cancellation off outlives the group's interval, and only the other one is cancelled.
        ActionFuture<MultiSearchResponse> response = client().prepareMultiSearch()
            .setMaxConcurrentSearchRequests(2)
            .add(client().prepareSearch(INDEX_NAME).setCancelAfterTimeInterval(SearchService.NO_TIMEOUT).setQuery(blockingQuery()))
            .add(client().prepareSearch(INDEX_NAME).setQuery(blockingQuery()))
            .execute();
        try {
            List<BlockedSearch> blocked = awaitBlockedSearches(INDEX_NAME, WORKLOAD_GROUP_ID, 2);
            awaitCancelled(withTaskTimeout(blocked, null));
            assertOutlivesTheGroupInterval(withTaskTimeout(blocked, SearchService.NO_TIMEOUT), response);
        } finally {
            disableBlocks(plugins);
        }
        MultiSearchResponse.Item[] items = response.actionGet(TIMEOUT).getResponses();
        assertSucceeded(items[0]);
        assertFailedAsCancelled(items[1].getFailure());
    }

    public void testGroupThatOverridesRequestValuesCancelsEverySubSearch() throws Exception {
        createWorkloadGroupForIndex(
            Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).put(OVERRIDE_REQUEST_VALUES, true).build()
        );
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        ActionFuture<MultiSearchResponse> response = client().prepareMultiSearch()
            .setMaxConcurrentSearchRequests(2)
            .add(client().prepareSearch(INDEX_NAME).setCancelAfterTimeInterval(SearchService.NO_TIMEOUT).setQuery(blockingQuery()))
            .add(client().prepareSearch(INDEX_NAME).setQuery(blockingQuery()))
            .execute();
        try {
            for (BlockedSearch blocked : awaitBlockedSearches(INDEX_NAME, WORKLOAD_GROUP_ID, 2)) {
                awaitCancelled(blocked);
            }
        } finally {
            disableBlocks(plugins);
        }
        for (MultiSearchResponse.Item item : response.actionGet(TIMEOUT).getResponses()) {
            assertFailedAsCancelled(item.getFailure());
        }
    }

    public void testMultiSearchLevelIntervalCountsAsEachSubSearchsOwn() throws Exception {
        // Guards existing behavior: without the resolver no group interval reaches the coordinator's timer at all. With it, the
        // msearch-level interval must keep beating a group without override_request_values.
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        String body = "{\"index\":\"" + INDEX_NAME + "\"}\n{\"query\":" + Strings.toString(MediaTypeRegistry.JSON, blockingQuery()) + "}\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(body),
            MediaTypeRegistry.JSON
        ).withParams(Map.of("cancel_after_time_interval", "-1")).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        // RestMultiSearchAction gives the msearch-level interval to each sub-search that sets none, before its task is created.
        assertEquals(SearchService.NO_TIMEOUT, request.requests().get(0).getCancelAfterTimeInterval());
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        ActionFuture<MultiSearchResponse> response = client().multiSearch(request);
        try {
            assertOutlivesTheGroupInterval(awaitBlockedSearch(INDEX_NAME, WORKLOAD_GROUP_ID), response);
        } finally {
            disableBlocks(plugins);
        }
        assertSucceeded(response.actionGet(TIMEOUT).getResponses()[0]);
    }

    public void testSubSearchesOfAMultiSearchRunInTheGroupsOfTheirIndices() throws Exception {
        // Each sub-search is tagged by its own indices only because TransportAction#execute restores the msearch thread's context
        // after starting each one, which it does while task resource tracking is enabled and supported.
        TaskResourceTrackingService taskResourceTrackingService = internalCluster().getInstance(TaskResourceTrackingService.class);
        assumeTrue(
            "the msearch thread's context is restored only while task resource tracking is enabled and supported",
            taskResourceTrackingService.isTaskResourceTrackingEnabled() && taskResourceTrackingService.isTaskResourceTrackingSupported()
        );
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        createWorkloadGroup(OTHER_WORKLOAD_GROUP_ID, Settings.EMPTY);
        indexDocument(OTHER_INDEX_NAME);
        createRuleAndAwaitTagging("wlm_other_rule", OTHER_INDEX_NAME, OTHER_WORKLOAD_GROUP_ID);
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        ActionFuture<MultiSearchResponse> response = client().prepareMultiSearch()
            .setMaxConcurrentSearchRequests(2)
            .add(client().prepareSearch(INDEX_NAME).setQuery(blockingQuery()))
            .add(client().prepareSearch(OTHER_INDEX_NAME).setQuery(blockingQuery()))
            .execute();
        try {
            BlockedSearch inGroup = awaitBlockedSearch(INDEX_NAME, WORKLOAD_GROUP_ID);
            BlockedSearch inOtherGroup = awaitBlockedSearch(OTHER_INDEX_NAME, OTHER_WORKLOAD_GROUP_ID);
            awaitCancelled(inGroup);
            assertOutlivesTheGroupInterval(inOtherGroup, response);
        } finally {
            disableBlocks(plugins);
        }
        MultiSearchResponse.Item[] items = response.actionGet(TIMEOUT).getResponses();
        assertFailedAsCancelled(items[0].getFailure());
        assertSucceeded(items[1]);
    }

    public void testGroupIntervalCancelsAScrollOnlyOnItsInitialSearch() throws Exception {
        createWorkloadGroupForIndex(
            Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).put(OVERRIDE_REQUEST_VALUES, true).build()
        );
        // The initial search of a scroll is a search like any other.
        assertCancelledByGroup(client().prepareSearch(INDEX_NAME).setScroll(TIMEOUT));

        // Guards existing behavior: a scroll page is tagged to the group through the scroll's indices, but TransportSearchScrollAction
        // arms no timer, so the group's interval can't cancel it even with override_request_values. More documents leave the page
        // something to query.
        for (int i = 2; i <= 5; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource(Map.of("field", "value")).get();
        }
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        // The block is released, so the initial search runs through.
        String scrollId = client().prepareSearch(INDEX_NAME).setScroll(TIMEOUT).setSize(1).setQuery(blockingQuery()).get().getScrollId();
        try {
            List<ScriptedBlockPlugin> plugins = enableBlocks();
            ActionFuture<SearchResponse> page = client().prepareSearchScroll(scrollId).setScroll(TIMEOUT).execute();
            try {
                BlockedSearch blocked = awaitBlockedScrollPage(scrollId);
                assertEquals("the scroll page must run in the workload group", WORKLOAD_GROUP_ID, blocked.task().getWorkloadGroupId());
                assertOutlivesTheGroupInterval(blocked, page);
            } finally {
                disableBlocks(plugins);
            }
            SearchResponse searchResponse = page.actionGet(TIMEOUT);
            assertEquals(0, searchResponse.getFailedShards());
            scrollId = searchResponse.getScrollId();
        } finally {
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    public void testGroupIntervalCancelsAPointInTimeSearch() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        // Rules tag a search by its indices, and a point-in-time search names none, so it reaches the group through the header.
        Client groupClient = groupClient(WORKLOAD_GROUP_ID);
        String pitId = groupClient.execute(CreatePitAction.INSTANCE, new CreatePitRequest(TIMEOUT, false, INDEX_NAME))
            .actionGet(TIMEOUT)
            .getId();
        try {
            // A point-in-time search goes through doExecute like any other search.
            assertCancelledByGroup(groupClient.prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)));
        } finally {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pitId)).actionGet(TIMEOUT);
        }
    }

    public void testNestedRewriteSearchIsCancelledByItsOwnTimer() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        // Matches no rule, so the nested search runs in the outer search's group, whose header it inherits.
        indexDocument(LOOKUP_INDEX_NAME);
        // A terms lookup with a subquery runs a nested search while the outer search rewrites its query. This group throttles
        // nothing, so the nested search is not a child of the outer one, and only its own timer bounds it.
        TermsLookup lookup = new TermsLookup(LOOKUP_INDEX_NAME, null, "field", blockingQuery());
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        ActionFuture<SearchResponse> response = client().prepareSearch(INDEX_NAME)
            .setQuery(QueryBuilders.termsLookupQuery("field", lookup))
            .execute();
        try {
            BlockedSearch nested = awaitBlockedSearch(LOOKUP_INDEX_NAME, WORKLOAD_GROUP_ID);
            assertFalse("the nested search must not be a child of the outer one", nested.task().getParentTaskId().isSet());
            awaitCancelled(nested);
        } finally {
            disableBlocks(plugins);
        }
        assertFailedAsCancelled(expectThrows(Exception.class, () -> response.actionGet(TIMEOUT)));
    }

    public void testHeaderAssignsTheGroupWithoutARule() throws Exception {
        setWlmMode("enabled");
        createWorkloadGroup(WORKLOAD_GROUP_ID, Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        indexDocument(INDEX_NAME);
        // The header a client sends, as RestController copies it into the thread context.
        assertCancelledByGroup(groupClient(WORKLOAD_GROUP_ID).prepareSearch(INDEX_NAME));
    }

    public void testHeaderWinsOverARuleForAnotherGroup() throws Exception {
        setWlmMode("enabled");
        createWorkloadGroup(WORKLOAD_GROUP_ID, Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        createWorkloadGroup(OTHER_WORKLOAD_GROUP_ID, Settings.EMPTY);
        indexDocument(INDEX_NAME);
        createRuleAndAwaitTagging("wlm_other_rule", INDEX_NAME, OTHER_WORKLOAD_GROUP_ID);
        // AutoTaggingActionFilter never overwrites a header, so the search runs in the header's group, with its interval.
        assertCancelledByGroup(groupClient(WORKLOAD_GROUP_ID).prepareSearch(INDEX_NAME));
    }

    public void testDisabledModeAppliesTheGroupIntervalOnlyThroughTheHeader() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        setWlmMode("disabled");
        // Guards existing behavior: rules tag nothing in this mode, so the search runs in the default group, which sets no interval.
        assertNotCancelledByGroup(
            client().prepareSearch(INDEX_NAME),
            INDEX_NAME,
            WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get()
        );
        // Neither the resolver nor the listener checks the mode, so a header still applies the group's interval.
        assertCancelledByGroup(groupClient(WORKLOAD_GROUP_ID).prepareSearch(INDEX_NAME));
    }

    public void testMonitorOnlyModeAppliesTheGroupInterval() throws Exception {
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        setWlmMode("monitor_only");
        assertCancelledByGroup(client().prepareSearch(INDEX_NAME));
    }

    public void testSearchTaggedToADeletedGroupKeepsItsOwnInterval() throws Exception {
        // Guards existing behavior: no timer could take a group's interval before the resolver. With it, a group that is gone
        // from the cluster state must not apply its interval.
        createWorkloadGroupForIndex(Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build());
        updateWorkloadGroupInClusterState(
            workloadGroup(WORKLOAD_GROUP_ID, Settings.builder().put(CANCEL_AFTER_TIME_INTERVAL, GROUP_CANCEL_AFTER).build()),
            DELETE
        );
        // The rule still points at the deleted group, so the search is tagged to an id that no longer resolves to a group.
        assertNotCancelledByGroup(client().prepareSearch(INDEX_NAME));
    }

    // Helpers

    /**
     * Creates the workload group with {@code searchSettings} and a rule that assigns searches on {@link #INDEX_NAME} to it, indexes
     * a document, and waits until searches are tagged to the group.
     */
    private void createWorkloadGroupForIndex(Settings searchSettings) throws Exception {
        setWlmMode("enabled");
        createWorkloadGroup(WORKLOAD_GROUP_ID, searchSettings);
        indexDocument(INDEX_NAME);
        createRuleAndAwaitTagging("wlm_cancel_after_rule", INDEX_NAME, WORKLOAD_GROUP_ID);
    }

    private void createWorkloadGroup(String workloadGroupId, Settings searchSettings) throws InterruptedException {
        updateWorkloadGroupInClusterState(workloadGroup(workloadGroupId, searchSettings), PUT);
    }

    private static WorkloadGroup workloadGroup(String workloadGroupId, Settings searchSettings) {
        return new WorkloadGroup(
            workloadGroupId + "_name",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.CPU, 0.4, ResourceType.MEMORY, 0.4),
                searchSettings
            ),
            Instant.now().getMillis()
        );
    }

    /** Creates a rule that assigns searches on {@code indexName} to the workload group, and waits until searches are tagged to it. */
    private void createRuleAndAwaitTagging(String ruleId, String indexName, String workloadGroupId) throws Exception {
        createRule(ruleId, indexName, workloadGroupId);
        // Rule propagation is async; wait until searches are tagged to the group.
        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);
    }

    /** A client whose requests carry the workloadGroupId header for {@code workloadGroupId}. */
    private Client groupClient(String workloadGroupId) {
        return client().filterWithHeader(Map.of(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId));
    }

    /** Runs {@code search} with a query that blocks on its shard, and asserts that the group's interval cancels it. */
    private void assertCancelledByGroup(SearchRequestBuilder search) throws Exception {
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        ActionFuture<SearchResponse> response = search.setQuery(blockingQuery()).execute();
        try {
            awaitCancelled(awaitBlockedSearch(INDEX_NAME, WORKLOAD_GROUP_ID));
        } finally {
            disableBlocks(plugins);
        }
        assertFailedAsCancelled(expectThrows(Exception.class, () -> response.actionGet(TIMEOUT)));
    }

    /** Runs {@code search} with a query that blocks on its shard, and asserts that it outlives the group's interval. */
    private void assertNotCancelledByGroup(SearchRequestBuilder search) throws Exception {
        assertNotCancelledByGroup(search, INDEX_NAME, WORKLOAD_GROUP_ID);
    }

    /**
     * Runs {@code search} with a query that blocks on its shard of {@code indexName}, in {@code workloadGroupId}, and asserts that it
     * outlives the group's interval.
     */
    private void assertNotCancelledByGroup(SearchRequestBuilder search, String indexName, String workloadGroupId) throws Exception {
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        ActionFuture<SearchResponse> response = search.setQuery(blockingQuery()).execute();
        try {
            assertOutlivesTheGroupInterval(awaitBlockedSearch(indexName, workloadGroupId), response);
        } finally {
            disableBlocks(plugins);
        }
        SearchResponse searchResponse = response.actionGet(TIMEOUT);
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(1, searchResponse.getHits().getTotalHits().value());
    }

    /** Waits until the group's interval cancels {@code blocked}, down to its shard, so releasing the block deterministically fails it. */
    private void awaitCancelled(BlockedSearch blocked) throws Exception {
        assertBusy(() -> {
            assertTrue(
                "the search ran past the group's search.cancel_after_time_interval of [" + GROUP_CANCEL_AFTER + "] without being cancelled",
                blocked.task().isCancelled()
            );
            assertTrue("the cancellation has not reached the shard yet", blocked.shardTask().isCancelled());
        }, 10, TimeUnit.SECONDS);
    }

    /** Asserts that {@code blocked}, whose request completes {@code response}, is not cancelled at the group's interval. */
    private void assertOutlivesTheGroupInterval(BlockedSearch blocked, ActionFuture<?> response) throws Exception {
        // Had the group's interval applied, the search would have been cancelled that long after it started.
        assertFalse(
            "the search was cancelled after the group's search.cancel_after_time_interval of [" + GROUP_CANCEL_AFTER + "]",
            waitUntil(blocked.task()::isCancelled, GROUP_CANCEL_AFTER.millis() * 3, TimeUnit.MILLISECONDS)
        );
        // A search that has finished is not cancelled either, so check that this one is still blocked on its shard.
        assertFalse("the search must still be blocked on its shard", response.isDone());
    }

    private static void assertFailedAsCancelled(Exception e) {
        assertNotNull("expected the search to fail", e);
        assertNotNull("expected the search to fail as cancelled but got: " + e, ExceptionsHelper.unwrap(e, TaskCancelledException.class));
    }

    private static void assertSucceeded(MultiSearchResponse.Item item) {
        assertNull("expected the search to succeed but got: " + item.getFailure(), item.getFailure());
        assertEquals(0, item.getResponse().getFailedShards());
    }

    private static QueryBuilder blockingQuery() {
        return scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()));
    }

    /** A search blocked on its shard: the coordinator's task and the shard's. */
    private record BlockedSearch(SearchTask task, SearchShardTask shardTask) {
    }

    /** The search on {@code indexName}, once its query has reached the shard, where it blocks; it must run in the group. */
    private BlockedSearch awaitBlockedSearch(String indexName, String workloadGroupId) throws Exception {
        return awaitBlockedSearches(indexName, workloadGroupId, 1).get(0);
    }

    /** The {@code count} searches on {@code indexName}, once their queries have reached the shard, where they block. */
    private List<BlockedSearch> awaitBlockedSearches(String indexName, String workloadGroupId, int count) throws Exception {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();
        List<BlockedSearch> blocked = new ArrayList<>();
        assertBusy(() -> {
            blocked.clear();
            for (Task task : taskManager.getTasks().values()) {
                if (task instanceof SearchShardTask shardTask && task.getDescription().contains("[" + indexName + "]")) {
                    blocked.add(new BlockedSearch((SearchTask) taskManager.getTask(shardTask.getParentTaskId().getId()), shardTask));
                }
            }
            assertEquals("the searches have not all reached their shard yet", count, blocked.size());
        });
        for (BlockedSearch search : blocked) {
            assertEquals(
                "the search must run in workload group [" + workloadGroupId + "]",
                workloadGroupId,
                search.task().getWorkloadGroupId()
            );
        }
        return List.copyOf(blocked);
    }

    /** The one search in {@code blocked} whose task's own cancellation timeout, from its request, is {@code taskTimeout}. */
    private static BlockedSearch withTaskTimeout(List<BlockedSearch> blocked, TimeValue taskTimeout) {
        List<BlockedSearch> matching = blocked.stream()
            .filter(search -> Objects.equals(taskTimeout, search.task().getCancellationTimeout()))
            .toList();
        assertEquals("expected one search whose task timeout is [" + taskTimeout + "]", 1, matching.size());
        return matching.get(0);
    }

    /** The page of the scroll {@code scrollId}, once its query has reached the shard, where it blocks. */
    private BlockedSearch awaitBlockedScrollPage(String scrollId) throws Exception {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class).getTaskManager();
        AtomicReference<BlockedSearch> blocked = new AtomicReference<>();
        assertBusy(() -> {
            for (Task task : taskManager.getTasks().values()) {
                if (task instanceof SearchShardTask shardTask
                    && taskManager.getTask(shardTask.getParentTaskId().getId()) instanceof SearchTask page
                    && SearchScrollAction.NAME.equals(page.getAction())
                    && page.getDescription().contains(scrollId)) {
                    blocked.set(new BlockedSearch(page, shardTask));
                }
            }
            assertNotNull("the scroll page has not reached its shard yet", blocked.get());
        });
        return blocked.get();
    }

    private List<ScriptedBlockPlugin> enableBlocks() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.enableBlock();
        }
        return plugins;
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    private int getCompletions(String groupId) throws Exception {
        WlmStatsRequest request = new WlmStatsRequest(null, new HashSet<>(Collections.singletonList(groupId)), null);
        long total = 0;
        for (WlmStats nodeStats : client().execute(WlmStatsAction.INSTANCE, request).get().getNodes()) {
            WorkloadGroupStatsHolder holder = nodeStats.getWorkloadGroupStats().getStats().get(groupId);
            if (holder != null) {
                total += holder.getCompletions();
            }
        }
        return Math.toIntExact(total);
    }

    private void createRule(String ruleId, String indexPattern, String workloadGroupId) throws Exception {
        Rule rule = new Rule(
            ruleId,
            "cancel after rule",
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of(indexPattern)),
            AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME),
            workloadGroupId,
            Instant.now().toString()
        );
        client().execute(CreateRuleAction.INSTANCE, new CreateRuleRequest(rule)).get();
    }

    private void setWlmMode(String mode) throws Exception {
        Settings.Builder settings = Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), mode);
        assertAcked(client().admin().cluster().updateSettings(new ClusterUpdateSettingsRequest().persistentSettings(settings)).get());
    }

    private void indexDocument(String indexName) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        IndexResponse response = client().prepareIndex(indexName)
            .setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
    }

    private void updateWorkloadGroupInClusterState(WorkloadGroup workloadGroup, String method) throws InterruptedException {
        WlmAutoTaggingIT.ExceptionCatchingListener listener = new WlmAutoTaggingIT.ExceptionCatchingListener();
        client().execute(
            WlmAutoTaggingIT.TestClusterUpdateTransportAction.ACTION,
            new WlmAutoTaggingIT.TestClusterUpdateRequest(workloadGroup, method),
            listener
        );
        assertTrue("cluster-state update did not complete in time", listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        if (listener.getException() != null) {
            throw new AssertionError("cluster-state update failed", listener.getException());
        }
    }
}
