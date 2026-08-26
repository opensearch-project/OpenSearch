/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.indices.TermsLookup;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.rest.RestHeaderDefinition;
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
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end integration test for per-node WLM request throttling ({@code node_limit}, {@code attribute=group}).
 * <p>
 * The scripted-block plugin holds a search in-flight (occupying a throttle permit) so that a second concurrent
 * search deterministically exceeds the node limit and must be rejected with a 429
 * ({@link OpenSearchRejectedExecutionException}). This exercises the real coordinator admission hook in
 * {@code TransportSearchAction} through auto-tagging, not a mocked service.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class WlmNodeThrottlingIT extends OpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String PUT = "PUT";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(WlmAutoTaggingIT.TestWorkloadManagementPlugin.class);
        plugins.add(RuleFrameworkPlugin.class);
        plugins.add(ScriptedBlockPlugin.class);
        plugins.add(TestPrincipalPlugin.class);
        return plugins;
    }

    @Before
    public void registerFeatureTypeIfMissingOnAllNodes() {
        // AutoTaggingRegistry is a JVM-static singleton, but each test (Scope.TEST) restarts the cluster and rebuilds
        // the feature type — including its WorkloadGroupFeatureValueValidator, which is bound to that cluster's live
        // ClusterService. Always refresh the registry to the current cluster's feature type; otherwise a later test
        // would validate rules against a previous (dead) cluster's state and fail with "not a valid workload group id".
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

    public void testSecondConcurrentSearchRejectedWhenNodeLimitReached() throws Exception {
        String workloadGroupId = "wlm_throttle_group";
        String ruleId = "wlm_throttle_rule";
        String indexName = "throttle_index";

        setWlmMode("enabled");

        // Workload group throttled to a single in-flight request per node.
        WorkloadGroup workloadGroup = createThrottledWorkloadGroup("throttle_test_group", workloadGroupId, 1);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "throttle rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        // Rule propagation to the in-memory processing service is asynchronous. Wait until a
        // (non-blocking) search is actually tagged to the throttled group before exercising
        // the concurrency scenario, otherwise the requests are untagged and never throttled.
        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the throttled workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // First search: blocks in the query phase, holding the single permit.
        ActionFuture<org.opensearch.action.search.SearchResponse> blockedSearch = blockingSearch(indexName).execute();
        awaitForBlock(plugins);

        int throttledBefore = getThrottled(workloadGroupId);
        long inFlightBefore = currentInFlightSearches();

        // Second search while the first is still in-flight: must be rejected (429).
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearch(indexName).execute().actionGet(TIMEOUT));
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );

        // The rejection must be counted in total_throttled.
        assertEquals("total_throttled should increment by exactly one", throttledBefore + 1, getThrottled(workloadGroupId));

        // The rejected request must NOT have entered the request-operations start path. This guards against the gauge
        // leak where a throttle rejection increments 'current' via onRequestStart but never reaches
        // onRequestEnd/onRequestFailure.
        //
        // Poll for the gauge to settle rather than comparing two instantaneous samples: the gauge is node-global and
        // the WLM rule-sync job issues its own search every few seconds, so any single pair of samples can differ by
        // that traffic in either direction. The steady state is well defined here -- the first search is still blocked
        // and nothing else in this test is running -- so the gauge must come back to inFlightBefore. A real leak is a
        // permanent +1 and never settles, so the poll still fails on the regression it is guarding.
        assertBusy(
            () -> assertEquals(
                "in-flight search gauge must exclude the throttle-rejected request",
                inFlightBefore,
                currentInFlightSearches()
            ),
            30,
            TimeUnit.SECONDS
        );

        // Release the block; the first search should complete successfully.
        disableBlocks(plugins);
        assertNotNull(blockedSearch.actionGet(TIMEOUT));

        // Once the blocked search finishes, the gauge must drain back to zero (no leaked in-flight count).
        assertBusy(() -> assertEquals("in-flight search gauge must drain to zero", 0L, currentInFlightSearches()), 30, TimeUnit.SECONDS);
    }

    public void testScrollContinuationIsThrottled() throws Exception {
        String workloadGroupId = "wlm_scroll_throttle_group";
        String ruleId = "wlm_scroll_throttle_rule";
        String indexName = "scroll_throttle_index";

        setWlmMode("enabled");
        WorkloadGroup workloadGroup = createThrottledWorkloadGroup("scroll_throttle_test_group", workloadGroupId, 1);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "scroll throttle rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the throttled workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        // Open a scroll context with a cheap query so the initial search releases its permit immediately.
        String scrollId = client().prepareSearch(indexName)
            .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
            .setSize(1)
            .setScroll(TIMEOUT)
            .get()
            .getScrollId();
        try {
            List<ScriptedBlockPlugin> plugins = initBlockFactory();
            ActionFuture<org.opensearch.action.search.SearchResponse> blockedSearch = blockingSearch(indexName).execute();
            awaitForBlock(plugins);

            int throttledBefore = getThrottled(workloadGroupId);

            // The group's only permit is held. A scroll continuation must be rejected like any other search -- if it is
            // admitted, node_limit is evadable simply by adding ?scroll= to a query.
            final String sid = scrollId;
            Throwable rejection = expectThrows(
                Throwable.class,
                () -> client().prepareSearchScroll(sid).setScroll(TIMEOUT).execute().actionGet(TIMEOUT)
            );
            assertTrue("Expected a scroll continuation to be throttled but was: " + rejection, hasRejectedExecutionCause(rejection));
            assertEquals("a throttled scroll must be counted", throttledBefore + 1, getThrottled(workloadGroupId));

            disableBlocks(plugins);
            assertNotNull(blockedSearch.actionGet(TIMEOUT));

            // With the permit released the same scroll continues normally, proving the rejection was the throttle and
            // not a broken scroll context.
            assertNotNull(client().prepareSearchScroll(sid).setScroll(TIMEOUT).get());
        } finally {
            client().prepareClearScroll().addScrollId(scrollId).get();
        }
    }

    public void testUsernameThrottlingKeepsPerUserBuckets() throws Exception {
        String workloadGroupId = "wlm_user_throttle_group";
        String ruleId = "wlm_user_throttle_rule";
        String indexName = "user_throttle_index";

        setWlmMode("enabled");

        // Group throttled per-username to a single in-flight request per node (attribute = username).
        WorkloadGroup workloadGroup = createThrottledWorkloadGroup("user_throttle_test_group", workloadGroupId, 1, "username");
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        // The rule's feature value (the workload group id) is validated against applied cluster state, which the group
        // update above populates asynchronously. Wait until the group is visible in cluster state before creating the
        // rule, otherwise rule creation races the update and fails validation.
        assertBusy(() -> {
            boolean present = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .metadata()
                .workloadGroups()
                .containsKey(workloadGroupId);
            assertTrue("workload group not yet applied in cluster state", present);
        }, 30, TimeUnit.SECONDS);
        createRule(ruleId, "user throttle rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);

        // Wait for rule propagation: a search tagged as alice must reach the group before the concurrency scenario.
        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            searchAs("alice", indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            int after = getCompletions(workloadGroupId);
            assertTrue("Expected search to be tagged to the throttled workload group", after > before);
        }, 30, TimeUnit.SECONDS);

        List<ScriptedBlockPlugin> plugins = initBlockFactory();

        // alice's first search blocks in the query phase, holding her single per-user permit.
        ActionFuture<org.opensearch.action.search.SearchResponse> aliceBlocked = blockingSearchAs("alice", indexName).execute();
        awaitForBlock(plugins);

        int throttledBefore = getThrottled(workloadGroupId);

        // alice's second concurrent search hits her per-user node_limit -> 429.
        Throwable rejection = expectThrows(Throwable.class, () -> blockingSearchAs("alice", indexName).execute().actionGet(TIMEOUT));
        assertTrue(
            "Expected an OpenSearchRejectedExecutionException in the cause chain but was: " + rejection,
            hasRejectedExecutionCause(rejection)
        );
        assertEquals("total_throttled should increment by exactly one", throttledBefore + 1, getThrottled(workloadGroupId));

        // bob is a different principal -> a different bucket -> admitted even while alice is at her limit.
        // (bob's search also blocks; we just need it to get past admission, so run it async and then release.)
        ActionFuture<org.opensearch.action.search.SearchResponse> bobBlocked = blockingSearchAs("bob", indexName).execute();
        assertBusy(() -> {
            int blocked = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                blocked += plugin.hits.get();
            }
            assertThat("bob's search should have been admitted and reached the blocking script", blocked, greaterThan(1));
        }, 30, TimeUnit.SECONDS);
        // bob was admitted, so no additional throttle beyond alice's one rejection.
        assertEquals("bob must not be throttled by alice's bucket", throttledBefore + 1, getThrottled(workloadGroupId));

        // Release the blocks; both alice's and bob's blocked searches complete successfully.
        disableBlocks(plugins);
        assertNotNull(aliceBlocked.actionGet(TIMEOUT));
        assertNotNull(bobBlocked.actionGet(TIMEOUT));
    }

    // Helpers

    private static boolean hasRejectedExecutionCause(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof OpenSearchRejectedExecutionException) {
                return true;
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return false;
    }

    private int getCompletions(String groupId) throws Exception {
        return sumGroupStat(groupId, org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder::getCompletions);
    }

    private int getThrottled(String groupId) throws Exception {
        return sumGroupStat(groupId, org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder::getThrottled);
    }

    /**
     * Sums one stat for a workload group across every node's WLM stats, read from the response objects directly. The
     * group may be absent from a node that has not registered it yet, which contributes nothing.
     */
    private int sumGroupStat(
        String groupId,
        java.util.function.ToLongFunction<org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder> extractor
    ) throws Exception {
        org.opensearch.action.admin.cluster.wlm.WlmStatsRequest request = new org.opensearch.action.admin.cluster.wlm.WlmStatsRequest(
            null,
            new java.util.HashSet<>(Collections.singletonList(groupId)),
            null
        );
        org.opensearch.action.admin.cluster.wlm.WlmStatsResponse response = client().execute(
            org.opensearch.action.admin.cluster.wlm.WlmStatsAction.INSTANCE,
            request
        ).get();
        long total = 0;
        for (org.opensearch.wlm.stats.WlmStats nodeStats : response.getNodes()) {
            org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder holder = nodeStats.getWorkloadGroupStats()
                .getStats()
                .get(groupId);
            if (holder != null) {
                total += extractor.applyAsLong(holder);
            }
        }
        return Math.toIntExact(total);
    }

    /**
     * Sums the current in-flight search gauge ({@link org.opensearch.action.search.SearchRequestStats#getTookCurrent()})
     * across all data nodes. This is the counter incremented in {@code onRequestStart} and decremented in
     * {@code onRequestEnd}/{@code onRequestFailure}; a throttle rejection must never touch it.
     */
    private long currentInFlightSearches() {
        long total = 0;
        for (org.opensearch.action.search.SearchRequestStats stats : internalCluster().getDataNodeInstances(
            org.opensearch.action.search.SearchRequestStats.class
        )) {
            total += stats.getTookCurrent();
        }
        return total;
    }

    public void testNestedRewriteSearchIsNotChargedASecondPermit() throws Exception {
        String workloadGroupId = "wlm_nested_group";
        String ruleId = "wlm_nested_rule";
        String indexName = "orders";
        String lookupIndex = "lookupidx";

        setWlmMode("enabled");
        // node_limit=1 is the case that exposes re-entrancy: the outer search holds the group's only permit while its
        // rewrite phase issues a nested coordinator search that resolves to the same bucket.
        WorkloadGroup workloadGroup = createThrottledWorkloadGroup("nested_test_group", workloadGroupId, 1);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        assertBusy(
            () -> assertTrue(
                "workload group not yet applied in cluster state",
                client().admin().cluster().prepareState().get().getState().metadata().workloadGroups().containsKey(workloadGroupId)
            ),
            30,
            TimeUnit.SECONDS
        );

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "nested rule", indexName, featureType, workloadGroupId);

        indexDocument(indexName);
        // The lookup index deliberately matches no rule, so the nested search inherits the outer request's workload
        // group id from the thread context -- the same bucket the outer request already holds a permit for.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(lookupIndex)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        client().prepareIndex(lookupIndex)
            .setId("1")
            .setSource(Map.of("uid", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertBusy(() -> {
            int before = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery()).get();
            assertTrue("Expected search to be tagged to the throttled workload group", getCompletions(workloadGroupId) > before);
        }, 30, TimeUnit.SECONDS);

        int throttledBefore = getThrottled(workloadGroupId);

        // A terms lookup with a subquery issues a full nested coordinator search during the rewrite phase. With zero
        // other load this must succeed: the outer request already paid for the bucket.
        TermsLookup lookup = new TermsLookup(lookupIndex, null, "uid", org.opensearch.index.query.QueryBuilders.matchAllQuery());
        SearchResponse response = client().prepareSearch(indexName)
            .setQuery(org.opensearch.index.query.QueryBuilders.termsLookupQuery("field", lookup))
            .execute()
            .actionGet(TIMEOUT);
        assertEquals(RestStatus.OK, response.status());
        assertEquals("a nested rewrite search must not be counted as throttled", throttledBefore, getThrottled(workloadGroupId));

        // The exemption must be scoped to nesting only -- a genuinely concurrent second request still gets a 429,
        // otherwise the fix would have silently disabled throttling for this group.
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        ActionFuture<SearchResponse> blocked = blockingSearch(indexName).execute();
        awaitForBlock(plugins);
        try {
            Throwable rejection = expectThrows(Throwable.class, () -> blockingSearch(indexName).execute().actionGet(TIMEOUT));
            assertTrue(
                "an independent concurrent request must still be throttled but was: " + rejection,
                hasRejectedExecutionCause(rejection)
            );
        } finally {
            disableBlocks(plugins);
            assertNotNull(blocked.actionGet(TIMEOUT));
        }
    }

    private SearchRequestBuilder blockingSearch(String indexName) {
        return client().prepareSearch(indexName)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
    }

    // In production the WLM auto-tagging filter sets the task's throttle principal from the security plugin's principal
    // extractor. There is no such extractor here, so TestPrincipalPlugin below stands in for it, reading the username
    // from a test-only header and setting it on the task exactly as the real filter does. This exercises the real
    // plumbing (task field -> throttle admission) rather than simulating it.
    private org.opensearch.transport.client.Client clientAs(String username) {
        return client().filterWithHeader(Map.of(TestPrincipalPlugin.TEST_PRINCIPAL_HEADER, "username|" + username));
    }

    private SearchRequestBuilder searchAs(String username, String indexName) {
        return clientAs(username).prepareSearch(indexName);
    }

    private SearchRequestBuilder blockingSearchAs(String username, String indexName) {
        return clientAs(username).prepareSearch(indexName)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        assertBusy(() -> {
            int blocked = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                blocked += plugin.hits.get();
            }
            assertThat(blocked, greaterThan(0));
        });
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
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
        Settings.Builder settings = Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), mode);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(request).get());
    }

    private WorkloadGroup createThrottledWorkloadGroup(String name, String id, int nodeLimit) {
        return createThrottledWorkloadGroup(name, id, nodeLimit, "group");
    }

    private WorkloadGroup createThrottledWorkloadGroup(String name, String id, int nodeLimit, String attribute) {
        Settings throttling = Settings.builder()
            .put(WorkloadGroupThrottleSettings.ATTRIBUTE.getKey(), attribute)
            .put(WorkloadGroupThrottleSettings.NODE_LIMIT.getKey(), nodeLimit)
            .build();
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
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        IndexResponse response = client().prepareIndex(indexName)
            .setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertEquals(org.opensearch.action.DocWriteResponse.Result.CREATED, response.getResult());
    }

    private void updateWorkloadGroupInClusterState(String method, WorkloadGroup workloadGroup) throws InterruptedException {
        WlmAutoTaggingIT.ExceptionCatchingListener listener = new WlmAutoTaggingIT.ExceptionCatchingListener();
        client().execute(
            WlmAutoTaggingIT.TestClusterUpdateTransportAction.ACTION,
            new WlmAutoTaggingIT.TestClusterUpdateRequest(workloadGroup, method),
            listener
        );
        boolean completed = listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        assertTrue("cluster-state update did not complete in time", completed);
        if (listener.getException() != null) {
            throw new AssertionError("cluster-state update failed", listener.getException());
        }
    }

    /**
     * Stands in for the security plugin's principal extractor. Registers a test-only header as both a REST header and a
     * task header, then copies it onto the task's throttle principal exactly as the real auto-tagging filter does, so the
     * IT exercises the production plumbing (task field -&gt; throttle admission) instead of simulating it.
     */
    public static class TestPrincipalPlugin extends Plugin implements ActionPlugin {
        static final String TEST_PRINCIPAL_HEADER = "test_throttle_principal";

        @Override
        public Collection<RestHeaderDefinition> getRestHeaders() {
            return List.of(new RestHeaderDefinition(TEST_PRINCIPAL_HEADER, false));
        }

        @Override
        public Collection<String> getTaskHeaders() {
            return List.of(TEST_PRINCIPAL_HEADER);
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Req extends ActionRequest, Resp extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Req request,
                    ActionRequestMetadata<Req, Resp> metadata,
                    ActionListener<Resp> listener,
                    ActionFilterChain<Req, Resp> chain
                ) {
                    if (task instanceof WorkloadGroupTask) {
                        String principal = task.getHeader(TEST_PRINCIPAL_HEADER);
                        if (principal != null) {
                            ((WorkloadGroupTask) task).setThrottlePrincipal(principal);
                        }
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    /**
     * Test script plugin that blocks during the query phase until released, keeping a search in-flight.
     */
    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();
        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        public void reset() {
            hits.set(0);
        }

        public void disableBlock() {
            shouldBlock.set(false);
        }

        public void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(WlmNodeThrottlingIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    // Explicit, generous budget: the default overload is 10s, but callers hold a search here while
                    // running their own 30s assertBusy waits, so the default would expire first and surface as a
                    // baffling "expected false but was true" failure inside an unrelated assertion.
                    assertBusy(() -> assertFalse(shouldBlock.get()), 120, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }
}
