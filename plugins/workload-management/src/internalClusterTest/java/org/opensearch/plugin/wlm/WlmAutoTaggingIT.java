/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Module;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.discovery.SeedHostsProvider;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.GetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureValueValidator;
import org.opensearch.plugin.wlm.rule.WorkloadGroupRuleRoutingService;
import org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.opensearch.plugin.wlm.rule.sync.detect.RuleEventClassifier;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.DiscoveryPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.RuleEntityParser;
import org.opensearch.rule.RuleFrameworkPlugin;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.RuleRoutingService;
import org.opensearch.rule.RuleRoutingServiceRegistry;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.UpdateRuleAction;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.rule.spi.RuleFrameworkExtension;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.rule.storage.IndexBasedRuleQueryMapper;
import org.opensearch.rule.storage.XContentRuleParser;
import org.opensearch.script.ScriptService;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;

public class WlmAutoTaggingIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    final static String PUT = "PUT";

    public WlmAutoTaggingIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestWorkloadManagementPlugin.class);
        plugins.add(RuleFrameworkPlugin.class);
        return plugins;
    }

    @Before
    public void registerFeatureTypeIfMissingOnAllNodes() {
        FeatureType featureType;
        try {
            featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        } catch (ResourceNotFoundException e) {
            featureType = TestWorkloadManagementPlugin.featureType;
            AutoTaggingRegistry.registerFeatureType(featureType);
        }

        for (String node : internalCluster().getNodeNames()) {
            RulePersistenceServiceRegistry persistenceRegistry = internalCluster().getInstance(RulePersistenceServiceRegistry.class, node);
            RuleRoutingServiceRegistry routingRegistry = internalCluster().getInstance(RuleRoutingServiceRegistry.class, node);

            try {
                routingRegistry.getRuleRoutingService(featureType);
            } catch (IllegalArgumentException ex) {
                persistenceRegistry.register(featureType, TestWorkloadManagementPlugin.rulePersistenceService);
                routingRegistry.register(featureType, TestWorkloadManagementPlugin.ruleRoutingService);
            }
        }
    }

    @After
    public void clearWlmModeSetting() {
        Settings.Builder builder = Settings.builder().putNull(WorkloadManagementSettings.WLM_MODE_SETTING.getKey());
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(builder).get());
    }

    public void testExactIndexMatchTriggersTagging() throws Exception {
        String workloadGroupId = "wlm_auto_tag_single";
        String ruleId = "wlm_auto_tag_test_rule";
        String indexName = "logs-tagged-index";

        // Step 0: Enable WLM mode
        setWlmMode("enabled");

        // Step 1: Create workload group
        WorkloadGroup workloadGroup = createWorkloadGroup("tagging_test_group", workloadGroupId);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        // Step 2: Create auto-tagging rule
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "tagging flow test", indexName, featureType, workloadGroupId);

        // Step 3: Index document
        indexDocument(indexName);

        assertBusy(() -> {
            // Step 4: Get pre-query completions
            int completionsBefore = getCompletions(workloadGroupId);

            // Step 5: Execute query
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();

            client().admin().indices().prepareRefresh(indexName).get();

            // Step 6: Get post-query completions
            int completionsAfter = getCompletions(workloadGroupId);
            assertTrue("Expected completions to increase", completionsAfter > completionsBefore);
        });
    }

    public void testWildcardBasedAttributesAreTagged() throws Exception {
        String workloadGroupId = "wlm_auto_tag_single";
        String ruleId = "wlm_auto_tag_test_rule";
        String indexName = "logs-tagged-index";

        setWlmMode("enabled");

        WorkloadGroup workloadGroup = createWorkloadGroup("tagging_test_group", workloadGroupId);
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "tagging flow test", "logs-tagged*", featureType, workloadGroupId);

        indexDocument(indexName);

        assertBusy(() -> {
            int completionsBefore = getCompletions(workloadGroupId);
            client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            client().admin().indices().prepareRefresh(indexName).get();
            int completionsAfter = getCompletions(workloadGroupId);
            assertTrue("Expected completions to increase", completionsAfter > completionsBefore);
        });
    }

    public void testMultipleRulesDoNotInterfereWithEachOther() throws Exception {
        String index1 = "test_1";
        String index2 = "test_2";
        String testIndex = "test_*";
        String workloadGroupId1 = "wlm_auto_tag_group_1";
        String workloadGroupId2 = "wlm_auto_tag_group_2";
        String ruleId1 = "wlm_auto_tag_rule_1";
        String ruleId2 = "wlm_auto_tag_rule_2";

        setWlmMode("enabled");

        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("group_1", workloadGroupId1));
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("group_2", workloadGroupId2));

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);

        createRule(ruleId1, "rule for test_1", index1, featureType, workloadGroupId1);
        createRule(ruleId2, "rule for test_2", index2, featureType, workloadGroupId2);

        indexDocument(index1);
        indexDocument(index2);

        assertBusy(() -> {
            int pre1 = getCompletions(workloadGroupId1);
            int pre2 = getCompletions(workloadGroupId2);

            client().prepareSearch(testIndex).setQuery(QueryBuilders.matchAllQuery()).get();

            int post1 = getCompletions(workloadGroupId1);
            int post2 = getCompletions(workloadGroupId2);

            assertTrue("Expected completions for group 1 not to increase", post1 == pre1);
            assertTrue("Expected completions for group 2 not to increase", post2 == pre2);
        });
    }

    public void testTaggingTriggeredAfterRuleUpdate() throws Exception {
        String workloadGroupId = "wlm_auto_tag_update";
        String ruleId = "wlm_auto_tag_update_rule";
        String indexName = "update_index";

        setWlmMode("enabled");

        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("update_test_group", workloadGroupId));

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule(ruleId, "initial non-matching rule", "random", featureType, workloadGroupId);

        indexDocument(indexName);
        int preUpdateCompletions = getCompletions(workloadGroupId);
        assertEquals("Expected no tagging before rule update", 0, preUpdateCompletions);

        UpdateRuleRequest updatedRule = new UpdateRuleRequest(
            ruleId,
            "updated rule matching nyc_taxis",
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of(indexName)),
            workloadGroupId,
            featureType
        );
        client().execute(UpdateRuleAction.INSTANCE, updatedRule).get();

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
            assertEquals(1, response.getHits().getTotalHits().value());

            int postUpdateCompletions = getCompletions(workloadGroupId);
            assertTrue("Expected completions to increase after rule update", postUpdateCompletions > preUpdateCompletions);
        });
    }

    public void testRuleWithNonexistentWorkloadGroupId() throws Exception {
        String ruleId = "nonexistent_group_rule";
        String indexName = "logs-nonexistent-index";

        setWlmMode("enabled");

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        Throwable thrown = assertThrows(
            Throwable.class,
            () -> createRule(ruleId, "test rule", indexName, featureType, "nonexistent_group")
        );

        Throwable cause = (thrown instanceof ExecutionException && thrown.getCause() != null) ? thrown.getCause() : thrown;

        assertTrue(
            "Got: " + cause.getClass(),
            (cause instanceof org.opensearch.action.ActionRequestValidationException) || (cause instanceof IllegalArgumentException)
        );
        assertTrue(
            "Expected validation message for nonexistent group",
            String.valueOf(cause.getMessage()).contains("nonexistent_group is not a valid workload group id")
        );
    }

    public void testCreateRuleWithTooManyIndexPatterns() throws Exception {
        String ruleId = "too_many_patterns_rule";
        String workloadGroupId = "wlm_group_too_many";

        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("too_many_patterns_group", workloadGroupId));

        // 11 patterns exceeds max allowed
        Set<String> tooManyPatterns = IntStream.range(0, 11).mapToObj(i -> "pattern_" + i).collect(Collectors.toSet());

        Attribute indexPatternAttr = RuleAttribute.INDEX_PATTERN;
        Map<Attribute, Set<String>> attributes = Map.of(indexPatternAttr, tooManyPatterns);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new Rule(ruleId, "desc", attributes, featureType, workloadGroupId, Instant.now().toString())
        );

        assertTrue(
            "Expected validation error about too many values",
            exception.getMessage().contains("Each attribute can only have a maximum of 10 values.")
        );
    }

    public void testCreateRuleWithEmptyIndexPatterns() throws Exception {
        String ruleId = "empty_patterns_rule";
        String workloadGroupId = "wlm_group_empty";

        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("empty_patterns_group", workloadGroupId));

        // Empty index pattern string
        Set<String> emptyPattern = Set.of("");

        Attribute indexPatternAttr = RuleAttribute.INDEX_PATTERN;
        Map<Attribute, Set<String>> attributes = Map.of(indexPatternAttr, emptyPattern);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new Rule(ruleId, "desc", attributes, featureType, workloadGroupId, Instant.now().toString())
        );

        assertTrue(
            "Expected validation error about empty index pattern",
            exception.getMessage().contains("is invalid (empty or exceeds 100 characters)")
        );
    }

    public void testCreateRuleWithLongIndexPatternValue() throws Exception {
        String ruleId = "long_pattern_rule";
        String workloadGroupId = "wlm_group_long_pattern";

        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("long_pattern_group", workloadGroupId));

        // Create a pattern longer than the max allowed (e.g., >100 characters)
        String longPattern = "x".repeat(101);
        Attribute indexPatternAttr = RuleAttribute.INDEX_PATTERN;
        Map<Attribute, Set<String>> attributes = Map.of(indexPatternAttr, Set.of(longPattern));

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);

        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> new Rule(ruleId, "desc", attributes, featureType, workloadGroupId, Instant.now().toString())
        );
        assertTrue(
            "Expected validation error about max length",
            exception.getMessage().contains("is invalid (empty or exceeds 100 characters)")
        );
    }

    public void testDeleteRuleForNonexistentId() throws Exception {
        String fakeRuleId = "nonexistent_rule_id";
        String workloadGroupId = "wlm_fake_delete";

        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("fake_delete_group", workloadGroupId));

        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);

        DeleteRuleRequest request = new DeleteRuleRequest(fakeRuleId, featureType);

        Exception exception = assertThrows(Exception.class, () -> client().execute(DeleteRuleAction.INSTANCE, request).get());

        assertTrue("Expected error message for nonexistent rule ID", exception.getMessage().contains("no such index"));
    }

    // Helper functions
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

    private WorkloadGroup createWorkloadGroup(String name, String id) {
        return new WorkloadGroup(
            name,
            id,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.CPU, 0.1, ResourceType.MEMORY, 0.1)
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

        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
    }

    private int getCompletions(String groupId) throws Exception {
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { groupId }, null);
        validateResponse(response, new String[] { groupId }, null);
        return extractTotalCompletions(response.toString(), groupId);
    }

    public WlmStatsResponse getWlmStatsResponse(String[] nodesId, String[] queryGroupIds, Boolean breach) throws ExecutionException,
        InterruptedException {
        WlmStatsRequest request = new WlmStatsRequest(nodesId, new HashSet<>(Arrays.asList(queryGroupIds)), breach);
        return client().execute(WlmStatsAction.INSTANCE, request).get();
    }

    public void validateResponse(WlmStatsResponse response, String[] validIds, String[] invalidIds) {
        String res = response.toString();
        if (validIds != null) {
            for (String id : validIds) {
                assertTrue("Expected ID not found in response: " + id, res.contains(id));
            }
        }
        if (invalidIds != null) {
            for (String id : invalidIds) {
                assertFalse("Unexpected ID found in response: " + id, res.contains(id));
            }
        }
    }

    private int extractTotalCompletions(String responseBody, String workloadGroupId) {
        int total = 0;
        String groupKey = "\"" + workloadGroupId + "\"";

        int index = 0;
        while ((index = responseBody.indexOf(groupKey, index)) != -1) {
            int groupStart = responseBody.indexOf("{", index);
            int completionsIndex = responseBody.indexOf("\"total_completions\"", groupStart);
            if (completionsIndex == -1) break;

            int colonIndex = responseBody.indexOf(":", completionsIndex);
            int commaIndex = responseBody.indexOf(",", colonIndex);
            String numberStr = responseBody.substring(colonIndex + 1, commaIndex).trim();
            total += Integer.parseInt(numberStr);

            index = commaIndex;
        }

        return total;
    }

    public void updateWorkloadGroupInClusterState(String method, WorkloadGroup workloadGroup) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(workloadGroup, method), listener);
        // wait for transport action to complete
        boolean completed = listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        assertTrue("cluster-state update did not complete in time", completed);

        if (listener.getException() != null) {
            throw new AssertionError("cluster-state update failed", listener.getException());
        }
        assertEquals(0, listener.getLatch().getCount());
    }

    public static class TestClusterUpdateTransportAction extends TransportClusterManagerNodeAction<TestClusterUpdateRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_cluster_update_action", TestResponse::new);

        @Inject
        public TestClusterUpdateTransportAction(
            ThreadPool threadPool,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ClusterService clusterService
        ) {
            super(
                ACTION.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                TestClusterUpdateRequest::new,
                indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return SAME;
        }

        @Override
        protected TestResponse read(StreamInput in) throws IOException {
            return new TestResponse(in);
        }

        @Override
        protected ClusterBlockException checkBlock(TestClusterUpdateRequest request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected void clusterManagerOperation(
            TestClusterUpdateRequest request,
            ClusterState clusterState,
            ActionListener<TestResponse> listener
        ) {
            clusterService.submitStateUpdateTask("query-group-persistence-service", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Map<String, WorkloadGroup> currentGroups = currentState.metadata().workloadGroups();
                    WorkloadGroup workloadGroup = request.getWorkloadGroup();
                    String id = workloadGroup.get_id();
                    String method = request.getMethod();
                    Metadata metadata;
                    if (method.equals(PUT)) { // create
                        metadata = Metadata.builder(currentState.metadata()).put(workloadGroup).build();
                    } else { // delete
                        metadata = Metadata.builder(currentState.metadata()).remove(currentGroups.get(id)).build();
                    }
                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new TestResponse());
                }
            });
        }
    }

    public static class ExceptionCatchingListener implements ActionListener<TestResponse> {
        private final CountDownLatch latch;
        private Exception exception = null;

        public ExceptionCatchingListener() {
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onResponse(TestResponse r) {
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            this.exception = e;
            latch.countDown();
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public Exception getException() {
            return exception;
        }
    }

    public static class TestResponse extends ActionResponse {
        public TestResponse() {}

        public TestResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public static class TestClusterUpdateRequest extends ClusterManagerNodeRequest<TestClusterUpdateRequest> {
        final private String method;
        final private WorkloadGroup workloadGroup;

        public TestClusterUpdateRequest(WorkloadGroup workloadGroup, String method) {
            this.method = method;
            this.workloadGroup = workloadGroup;
        }

        public TestClusterUpdateRequest(StreamInput in) throws IOException {
            super(in);
            this.method = in.readString();
            this.workloadGroup = new WorkloadGroup(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(method);
            workloadGroup.writeTo(out);
        }

        public WorkloadGroup getWorkloadGroup() {
            return workloadGroup;
        }

        public String getMethod() {
            return method;
        }
    }

    /**
     * Test-only plugin implementation for Workload Management.
     * This plugin registers a {@link FeatureType} for {@code workload_group}, along with
     * the necessary components to support rule-based auto-tagging logic in integration tests.
     * It uses in-memory rule processing and index-backed rule persistence for isolated testing.

     * This class is not intended for production use and should only be used in internal
     * cluster tests that validate WLM rule framework behavior end-to-end.
     */
    public static class TestWorkloadManagementPlugin extends Plugin
        implements
            ActionPlugin,
            SystemIndexPlugin,
            DiscoveryPlugin,
            ExtensiblePlugin,
            RuleFrameworkExtension {

        /**
         * Name of the system index used to store workload management rules.
         */
        public static final String INDEX_NAME = ".wlm_rules";
        /**
         * Maximum number of rules returned in a single page during GET operations.
         */
        public static final int MAX_RULES_PER_PAGE = 50;
        static FeatureType featureType;
        static RulePersistenceService rulePersistenceService;
        private static final Map<Attribute, Integer> orderedAttributes = new HashMap<>();
        static RuleRoutingService ruleRoutingService;
        private AutoTaggingActionFilter autoTaggingActionFilter;
        private final Map<Attribute, AttributeExtractorExtension> attributeExtractorExtensions = new HashMap<>();

        /**
         * Default constructor.
         * Required for plugin instantiation during integration tests.
         */
        public TestWorkloadManagementPlugin() {}

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            featureType = new WorkloadGroupFeatureType(new WorkloadGroupFeatureValueValidator(clusterService), new HashMap<>());
            RuleEntityParser parser = new XContentRuleParser(featureType);
            AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
                featureType,
                DefaultAttributeValueStore::new
            );
            InMemoryRuleProcessingService ruleProcessingService = new InMemoryRuleProcessingService(
                attributeValueStoreFactory,
                featureType.getOrderedAttributes()
            );
            rulePersistenceService = new IndexStoredRulePersistenceService(
                INDEX_NAME,
                client,
                clusterService,
                MAX_RULES_PER_PAGE,
                parser,
                new IndexBasedRuleQueryMapper()
            );
            ruleRoutingService = new WorkloadGroupRuleRoutingService(client, clusterService);

            WlmClusterSettingValuesProvider wlmClusterSettingValuesProvider = new WlmClusterSettingValuesProvider(
                clusterService.getSettings(),
                clusterService.getClusterSettings()
            );
            RefreshBasedSyncMechanism refreshMechanism = new RefreshBasedSyncMechanism(
                threadPool,
                clusterService.getSettings(),
                featureType,
                rulePersistenceService,
                new RuleEventClassifier(Collections.emptySet(), ruleProcessingService),
                wlmClusterSettingValuesProvider
            );

            autoTaggingActionFilter = new AutoTaggingActionFilter(
                ruleProcessingService,
                threadPool,
                attributeExtractorExtensions,
                wlmClusterSettingValuesProvider,
                featureType
            );
            return List.of(refreshMechanism, rulePersistenceService, featureType);
        }

        @Override
        public Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(
            TransportService transportService,
            NetworkService networkService
        ) {
            ((WorkloadGroupRuleRoutingService) ruleRoutingService).setTransportService(transportService);
            return Collections.emptyMap();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(autoTaggingActionFilter);
        }

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(
                new ActionPlugin.ActionHandler<>(CreateWorkloadGroupAction.INSTANCE, TransportCreateWorkloadGroupAction.class),
                new ActionPlugin.ActionHandler<>(GetWorkloadGroupAction.INSTANCE, TransportGetWorkloadGroupAction.class),
                new ActionPlugin.ActionHandler<>(DeleteWorkloadGroupAction.INSTANCE, TransportDeleteWorkloadGroupAction.class),
                new ActionPlugin.ActionHandler<>(UpdateWorkloadGroupAction.INSTANCE, TransportUpdateWorkloadGroupAction.class),
                new ActionPlugin.ActionHandler<>(TestClusterUpdateTransportAction.ACTION, TestClusterUpdateTransportAction.class)
            );
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new SystemIndexDescriptor(INDEX_NAME, "System index used for storing workload_group rules"));
        }

        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            WlmClusterSettingValuesProvider settingProvider = new WlmClusterSettingValuesProvider(settings, clusterSettings);
            return List.of(
                new RestCreateWorkloadGroupAction(settingProvider),
                new RestGetWorkloadGroupAction(),
                new RestDeleteWorkloadGroupAction(settingProvider),
                new RestUpdateWorkloadGroupAction(settingProvider)
            );
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT,
                RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING,
                IndexStoredRulePersistenceService.MAX_WLM_RULES_SETTING
            );
        }

        @Override
        public Collection<Module> createGuiceModules() {
            return List.of(new WorkloadManagementPluginModule());
        }

        @Override
        public Supplier<RulePersistenceService> getRulePersistenceServiceSupplier() {
            return () -> rulePersistenceService;
        }

        @Override
        public Supplier<RuleRoutingService> getRuleRoutingServiceSupplier() {
            return () -> ruleRoutingService;
        }

        @Override
        public Supplier<FeatureType> getFeatureTypeSupplier() {
            return () -> featureType;
        }

        @Override
        public void setAttributes(List<Attribute> attributes) {
            for (Attribute attribute : attributes) {
                if (attribute.getName().equals(PRINCIPAL_ATTRIBUTE_NAME)) {
                    orderedAttributes.put(attribute, 1);
                }
            }
        }

        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            for (AttributeExtractorExtension ext : loader.loadExtensions(AttributeExtractorExtension.class)) {
                attributeExtractorExtensions.put(ext.getAttributeExtractor().getAttribute(), ext);
            }
        }
    }
}
