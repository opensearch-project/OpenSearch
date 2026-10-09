/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.apache.logging.log4j.LogManager;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.RuleFrameworkPlugin;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.RuleRoutingServiceRegistry;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.lookup.LeafFieldsLookup;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupSharedThrottleService;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * End-to-end test of cluster-level ({@code shared_limit}) WLM throttling: one owner node enforces a bucket's
 * cluster-wide in-flight count whichever coordinator a request lands on. The groups set only {@code shared_limit}, so
 * every admitted request consults the owner, through both the local short-circuit and the remote acquire RPC.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
public class WlmClusterThrottlingIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Blocked fills occupy SEARCH threads on the shard's node, and remote grants continue on the coordinator's
            // SEARCH pool. With a randomized node.processors=1 (2 threads) a grant would queue behind the fills and
            // deadlock the round, so leave headroom.
            .put("thread_pool.search.size", 4)
            // Wide acquire timeout so a CI stall is not reported as "cluster-wide throttle unavailable" mid-assertion.
            .put(ClusterScriptedBlockPlugin.SHARED_ACQUIRE_TIMEOUT.getKey(), TIMEOUT)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(WlmAutoTaggingIT.TestWorkloadManagementPlugin.class);
        plugins.add(RuleFrameworkPlugin.class);
        plugins.add(ClusterScriptedBlockPlugin.class);
        return plugins;
    }

    @Before
    public void registerFeatureTypeIfMissingOnAllNodes() {
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
    public void clearWlmModeSetting() throws Exception {
        Settings.Builder builder = Settings.builder().putNull(WorkloadManagementSettings.WLM_MODE_SETTING.getKey());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder).get());
    }

    public void testClusterWideCeilingHoldsAcrossCoordinators() throws Exception {
        String indexName = "shared_throttle_index";
        setWlmMode("enabled");

        // shared_limit = 2, node_limit unset: a cluster-wide ceiling of 2 in-flight, whichever coordinator is used.
        SharedGroup group = setUpSharedOnlyGroup("shared_test_group", "wlm_shared_throttle_group", indexName, indexName, 2);

        List<String> coordinators = List.of(internalCluster().getNodeNames());
        assertEquals("test requires three distinct coordinators", 3, coordinators.size());
        String thirdCoordinator = coordinators.get(2);

        List<ClusterScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> fill1 = null;
        ActionFuture<SearchResponse> fill2 = null;
        try {
            fill1 = holdSharedPermit(coordinators.get(0), indexName, plugins);
            fill2 = holdSharedPermit(coordinators.get(1), indexName, plugins);
            assertEquals("each admitted fill must hold a shared permit", 2, sharedInFlight(group.bucketKey));

            long throttledBefore = getThrottled(group.id);
            // Bounded: a wrongly admitted third search would park in the armed script, so detect that at once.
            ActionFuture<SearchResponse> third = blockingSearchVia(thirdCoordinator, indexName).execute();
            assertBusy(
                () -> assertTrue("third search neither rejected nor admitted yet", third.isDone() || blockedCount(plugins) > 2),
                30,
                TimeUnit.SECONDS
            );
            assertFalse("third search was admitted past shared_limit=" + group.limit, blockedCount(plugins) > 2);
            Throwable failure = expectThrows(Throwable.class, () -> third.actionGet(TIMEOUT));
            assertSharedLimitRejection(failure, group);
            assertEquals("total_throttled should increment by exactly one", throttledBefore + 1, getThrottled(group.id));
        } finally {
            disableBlocks(plugins);
        }
        assertNotNull(fill1.actionGet(TIMEOUT));
        assertNotNull(fill2.actionGet(TIMEOUT));

        // Asserting the owner's count returns to zero (rather than "one more search is admitted") catches a leaked permit.
        awaitDrained(group);
        assertNotNull(client(thirdCoordinator).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get(TIMEOUT));
    }

    /**
     * A search that is admitted to the shared tier and then fails (index resolution of a missing index, tagged into the
     * group by the rule's index pattern) must still give its permit back, through every coordinator so both the local
     * and the remote release run.
     */
    public void testSharedPermitReleasedWhenSearchFailsAfterAdmission() throws Exception {
        String indexName = "shared_release_index";
        String missingIndex = "shared_release_missing";
        setWlmMode("enabled");
        SharedGroup group = setUpSharedOnlyGroup("shared_release_group", "wlm_shared_release_group", "shared_release_*", indexName, 1);
        List<String> coordinators = List.of(internalCluster().getNodeNames());

        // Prove the missing-index search is charged before it fails, or the drain checks below would pass vacuously.
        List<ClusterScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> holder = null;
        try {
            holder = holdSharedPermit(coordinators.get(0), indexName, plugins);
            Exception failure = expectThrows(
                Exception.class,
                () -> client(coordinators.get(1)).prepareSearch(missingIndex).execute().actionGet(TIMEOUT)
            );
            assertSharedLimitRejection(failure, group);
        } finally {
            disableBlocks(plugins);
        }
        assertNotNull(holder.actionGet(TIMEOUT));
        awaitDrained(group);

        for (String node : coordinators) {
            Exception failure = expectThrows(Exception.class, () -> client(node).prepareSearch(missingIndex).execute().actionGet(TIMEOUT));
            assertNotNull(
                "expected index_not_found after admission via [" + node + "] but was: " + failure,
                ExceptionsHelper.unwrap(failure, IndexNotFoundException.class)
            );
            awaitDrained(group);
        }
    }

    /**
     * Scroll continuations are admitted against the shared tier too: one is rejected at the shared limit while another
     * search holds the only permit, and each admitted scroll page gives its permit back.
     */
    public void testScrollContinuationIsSharedThrottled() throws Exception {
        String indexName = "shared_scroll_index";
        setWlmMode("enabled");
        SharedGroup group = setUpSharedOnlyGroup("shared_scroll_group", "wlm_shared_scroll_group", indexName, indexName, 1);
        List<String> coordinators = List.of(internalCluster().getNodeNames());

        // A cheap query, so the initial search releases its permit immediately.
        String scrollId = client(coordinators.get(0)).prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(1)
            .setScroll(TIMEOUT)
            .get(TIMEOUT)
            .getScrollId();
        try {
            awaitDrained(group);

            List<ClusterScriptedBlockPlugin> plugins = initBlockFactory();
            ActionFuture<SearchResponse> holder = null;
            try {
                holder = holdSharedPermit(coordinators.get(0), indexName, plugins);
                Exception failure = expectThrows(
                    Exception.class,
                    () -> client(coordinators.get(1)).prepareSearchScroll(scrollId).setScroll(TIMEOUT).execute().actionGet(TIMEOUT)
                );
                assertSharedLimitRejection(failure, group);
            } finally {
                disableBlocks(plugins);
            }
            assertNotNull(holder.actionGet(TIMEOUT));
            awaitDrained(group);

            // Proves the rejection was the throttle, not a broken scroll context, and that admitted pages release
            // through every coordinator.
            for (String node : coordinators) {
                assertNotNull(client(node).prepareSearchScroll(scrollId).setScroll(TIMEOUT).get(TIMEOUT));
                awaitDrained(group);
            }
        } finally {
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    // Helpers

    /**
     * Creates a group throttled only by {@code shared_limit} (so every admitted request consults the bucket owner), a rule
     * tagging {@code rulePattern} into it, and {@code indexName} with one document; then waits until a search through every
     * coordinator is tagged and the probes' permits have drained.
     */
    private SharedGroup setUpSharedOnlyGroup(String name, String groupId, String rulePattern, String indexName, int sharedLimit)
        throws Exception {
        SharedGroup group = new SharedGroup(groupId, sharedLimit);
        updateWorkloadGroupInClusterState(createSharedThrottledGroup(name, groupId, group.limit));
        assertBusy(() -> {
            boolean present = client().admin().cluster().prepareState().get().getState().metadata().workloadGroups().containsKey(groupId);
            assertTrue("workload group not yet applied in cluster state", present);
        }, 30, TimeUnit.SECONDS);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(groupId + "_rule", name + " rule", rulePattern, featureType, groupId);
        indexDocument(indexName);

        // Rule refresh is asynchronous per node, and an untagged search bypasses throttling: poll every coordinator until a
        // search through it is tagged to the group (its completions advance).
        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> {
                long before = getCompletions(groupId);
                try {
                    client(node).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
                } catch (Exception e) {
                    // Remote releases are fire-and-forget, so the previous probe's permit may still be held and throttle
                    // this one. assertBusy only retries on AssertionError, so convert that 429; anything else is real.
                    OpenSearchRejectedExecutionException rejection = rejectedExecutionCause(e);
                    assertNull("transient throttle during propagation probe — retry: " + e, rejection);
                    throw e;
                }
                long after = getCompletions(groupId);
                assertTrue("search via [" + node + "] not yet tagged to the throttled workload group", after > before);
            }, 30, TimeUnit.SECONDS);
        }

        // Wait for the probes' asynchronous releases so they can't occupy a slot the test needs.
        awaitDrained(group);
        return group;
    }

    /** Starts a blocking search through {@code node} and returns it once it holds a shared permit (reached the script). */
    private ActionFuture<SearchResponse> holdSharedPermit(String node, String indexName, List<ClusterScriptedBlockPlugin> plugins)
        throws Exception {
        int blockedBefore = blockedCount(plugins);
        ActionFuture<SearchResponse> search = blockingSearchVia(node, indexName).execute();
        assertBusy(
            () -> assertTrue("search neither blocked nor completed yet", search.isDone() || blockedCount(plugins) > blockedBefore),
            30,
            TimeUnit.SECONDS
        );
        assertFalse("search completed before reaching the blocking script", search.isDone());
        return search;
    }

    private static void assertSharedLimitRejection(Throwable failure, SharedGroup group) {
        OpenSearchRejectedExecutionException rejection = rejectedExecutionCause(failure);
        assertNotNull("expected a throttle rejection but was: " + failure, rejection);
        assertTrue(rejection.getMessage(), rejection.getMessage().contains("reached its shared limit of " + group.limit));
    }

    private static OpenSearchRejectedExecutionException rejectedExecutionCause(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof OpenSearchRejectedExecutionException rejection) {
                return rejection;
            }
        }
        return null;
    }

    private long getCompletions(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getCompletions);
    }

    private long getThrottled(String groupId) throws Exception {
        return sumAcrossNodes(groupId, WorkloadGroupStatsHolder::getThrottled);
    }

    private long sumAcrossNodes(String groupId, ToLongFunction<WorkloadGroupStatsHolder> extractor) throws Exception {
        WlmStatsRequest request = new WlmStatsRequest(null, new HashSet<>(Collections.singletonList(groupId)), null);
        WlmStatsResponse response = client().execute(WlmStatsAction.INSTANCE, request).get();
        long total = 0;
        for (WlmStats nodeStats : response.getNodes()) {
            WorkloadGroupStatsHolder holder = nodeStats.getWorkloadGroupStats().getStats().get(groupId);
            if (holder != null) {
                total += extractor.applyAsLong(holder);
            }
        }
        return total;
    }

    private SearchRequestBuilder blockingSearchVia(String nodeName, String indexName) {
        return client(nodeName).prepareSearch(indexName)
            .setQuery(
                scriptQuery(new Script(ScriptType.INLINE, "mockscript", ClusterScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
            );
    }

    private List<ClusterScriptedBlockPlugin> initBlockFactory() {
        List<ClusterScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ClusterScriptedBlockPlugin.class));
        }
        for (ClusterScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private static int blockedCount(List<ClusterScriptedBlockPlugin> plugins) {
        int blocked = 0;
        for (ClusterScriptedBlockPlugin plugin : plugins) {
            blocked += plugin.hits.get();
        }
        return blocked;
    }

    // Permit records for a bucket summed over every node; only the owner holds any, since these tests never move it.
    private int sharedInFlight(String bucketKey) {
        int total = 0;
        for (String node : internalCluster().getNodeNames()) {
            total += internalCluster().getInstance(WorkloadGroupSharedThrottleService.class, node).ownedInFlight(bucketKey);
        }
        return total;
    }

    private void disableBlocks(List<ClusterScriptedBlockPlugin> plugins) {
        for (ClusterScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    /** A shared-only test group and its bucket. */
    private static final class SharedGroup {
        final String id;
        final String bucketKey;
        final int limit;

        SharedGroup(String id, int limit) {
            this.id = id;
            this.bucketKey = id + ":" + WorkloadGroupThrottleSettings.GROUP_SCOPE + ":" + WorkloadGroupThrottleSettings.GROUP_SCOPE;
            this.limit = limit;
        }
    }

    /** Waits until the owner holds no permit for the bucket. */
    private void awaitDrained(SharedGroup group) throws Exception {
        assertBusy(
            () -> assertEquals("shared in-flight count on the bucket owner", 0, sharedInFlight(group.bucketKey)),
            30,
            TimeUnit.SECONDS
        );
    }

    private void createRule(String ruleId, String ruleName, String indexPattern, FeatureType featureType, String workloadGroupId)
        throws Exception {
        Rule rule = new Rule(
            ruleId,
            ruleName,
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of(indexPattern)),
            featureType,
            workloadGroupId,
            Instant.now().toString()
        );
        client().execute(CreateRuleAction.INSTANCE, new CreateRuleRequest(rule)).get();
    }

    private void setWlmMode(String mode) throws Exception {
        Settings.Builder settings = Settings.builder().put("wlm.workload_group.mode", mode);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(settings);
        client().admin().cluster().updateSettings(request).get();
    }

    private WorkloadGroup createSharedThrottledGroup(String name, String id, int sharedLimit) {
        Settings throttling = Settings.builder().put(WorkloadGroupThrottleSettings.SHARED_LIMIT.getKey(), sharedLimit).build();
        return new WorkloadGroup(
            name,
            id,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.CPU, 0.9, ResourceType.MEMORY, 0.9),
                Settings.EMPTY,
                throttling
            ),
            Instant.now().getMillis()
        );
    }

    private void indexDocument(String indexName) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                // One shard, so each search makes exactly one blocking script hit. Admission is on the coordinator,
                // wherever the shard lives.
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        IndexResponse response = client().prepareIndex(indexName)
            .setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
    }

    private void updateWorkloadGroupInClusterState(WorkloadGroup workloadGroup) throws InterruptedException {
        WlmAutoTaggingIT.ExceptionCatchingListener listener = new WlmAutoTaggingIT.ExceptionCatchingListener();
        client().execute(
            WlmAutoTaggingIT.TestClusterUpdateTransportAction.ACTION,
            new WlmAutoTaggingIT.TestClusterUpdateRequest(workloadGroup, "PUT"),
            listener
        );
        boolean completed = listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        assertTrue("cluster-state update did not complete in time", completed);
        if (listener.getException() != null) {
            throw new AssertionError("cluster-state update failed", listener.getException());
        }
    }

    /**
     * Test script plugin that blocks during the query phase until released, keeping a search in-flight on whichever
     * coordinator dispatched it.
     */
    public static class ClusterScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "cluster_search_block";
        // Same key as WorkloadGroupSharedThrottleService's unregistered acquire-timeout setting; registered here so it is
        // valid in this test cluster only.
        static final Setting<TimeValue> SHARED_ACQUIRE_TIMEOUT = Setting.timeSetting(
            "wlm.workload_group.throttle.shared_acquire_timeout",
            TimeValue.timeValueMillis(200),
            Setting.Property.NodeScope
        );

        private final AtomicInteger hits = new AtomicInteger();
        // Count 0 = not armed. A latch releases blocked queries the instant disableBlock() is called.
        private volatile CountDownLatch release = new CountDownLatch(0);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            release.countDown();
        }

        public void enableBlock() {
            release = new CountDownLatch(1);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(SHARED_ACQUIRE_TIMEOUT);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(WlmClusterThrottlingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                CountDownLatch latch = release;
                hits.incrementAndGet();
                try {
                    // Outlast callers' 30s waits. Throw a RuntimeException (a clean shard failure) on expiry, never an
                    // AssertionError: an Error escapes AbstractRunnable, kills the search thread and never answers the search.
                    if (latch.await(120, TimeUnit.SECONDS) == false) {
                        throw new IllegalStateException("blocking script was never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return true;
            });
        }
    }
}
