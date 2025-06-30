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
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.metadata.WorkloadGroupMetadata;
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
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.DiscoveryPlugin;
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
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleAction;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleAction;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
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
import org.joda.time.Instant;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;

public class WlmAutoTaggingIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.SECONDS);
    final static String PUT = "PUT";
    final static String MEMORY = "MEMORY";
    final static String CPU = "CPU";
    final static String ENABLED = "enabled";
    final static String DELETE = "DELETE";

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
            featureType = AutoTaggingRegistry.getFeatureType("workload_group");
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

    private static final String RULE_ID = "test_rule_id";

    public void testCreateAutoTaggingRule() throws Exception {

        String workloadGroupId = "wlm_test_group_id";

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "wlm_test_group",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.1, ResourceType.MEMORY, 0.1)
            ),
            Instant.now().getMillis()
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributes = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        Rule rule = new Rule(RULE_ID, "Integration test rule", attributes, featureType, workloadGroupId, "2025-06-20T00:00:00.000Z");

        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> {
                ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
                ClusterState state = clusterService.state();
                WorkloadGroupMetadata metadata = (WorkloadGroupMetadata) state.metadata().custom(WorkloadGroupMetadata.TYPE);
                assertNotNull("Metadata should not be null on node " + node, metadata);
                assertTrue("wlm_test_group_id should exist on node " + node, metadata.workloadGroups().containsKey(workloadGroupId));
            });
        }

        CreateRuleRequest createRequest = new CreateRuleRequest(rule);
        CreateRuleResponse response = client().execute(CreateRuleAction.INSTANCE, createRequest).get();

        assertNotNull(response);
        assertEquals(RULE_ID, response.getRule().getId());
        assertEquals(workloadGroupId, response.getRule().getFeatureValue());
    }

    public void testGetAutoTaggingRuleById() throws Exception {
        String workloadGroupId = "wlm_test_group_id_get";

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "wlm_test_group_get",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.3, ResourceType.MEMORY, 0.4)
            ),
            Instant.now().getMillis()
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributes = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-get-*"));

        Rule rule = new Rule(RULE_ID, "Rule for Get Test", attributes, featureType, workloadGroupId, Instant.now().toString());
        CreateRuleRequest createRequest = new CreateRuleRequest(rule);
        CreateRuleResponse createResponse = client().execute(CreateRuleAction.INSTANCE, createRequest).get();

        assertNotNull("CreateRuleResponse should not be null", createResponse);
        assertEquals(RULE_ID, createResponse.getRule().getId());

        client().admin().indices().prepareRefresh().get();

        GetRuleRequest getRequest = new GetRuleRequest(RULE_ID, Map.of(), null, featureType);

        GetRuleResponse getResponse = client().execute(GetRuleAction.INSTANCE, getRequest).get();

        assertNotNull("GetRuleResponse should not be null", getResponse);
        assertFalse("Returned rule list should not be empty", getResponse.getRules().isEmpty());

        Rule retrievedRule = getResponse.getRules().get(0);
        assertEquals("Rule for Get Test", retrievedRule.getDescription());
        assertEquals(RULE_ID, retrievedRule.getId());
        assertEquals(workloadGroupId, retrievedRule.getFeatureValue());
    }

    public void testUpdateAutoTaggingRule() throws Exception {
        String workloadGroupId = "wlm_test_group_id_update";

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "wlm_test_group_update",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.2, ResourceType.MEMORY, 0.3)
            ),
            Instant.now().getMillis()
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> originalAttributes = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        Rule originalRule = new Rule(RULE_ID, "Original rule", originalAttributes, featureType, workloadGroupId, Instant.now().toString());
        CreateRuleRequest createRequest = new CreateRuleRequest(originalRule);
        CreateRuleResponse createResponse = client().execute(CreateRuleAction.INSTANCE, createRequest).get();
        assertNotNull("CreateRuleResponse should not be null", createResponse);
        assertEquals(RULE_ID, createResponse.getRule().getId());

        client().admin().indices().prepareRefresh().get();

        Map<Attribute, Set<String>> updatedAttributes = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*", "metrics-*"));

        UpdateRuleRequest updateRequest = new UpdateRuleRequest(
            RULE_ID,
            "Updated rule description",
            updatedAttributes,
            workloadGroupId,
            featureType
        );

        UpdateRuleResponse updateResponse = client().execute(UpdateRuleAction.INSTANCE, updateRequest).get();

        client().admin().indices().prepareRefresh().get();
        assertNotNull(updateResponse);
        assertEquals("Updated rule description", updateResponse.getRule().getDescription());
        assertTrue(updateResponse.getRule().getAttributeMap().get(RuleAttribute.INDEX_PATTERN).contains("metrics-*"));
    }

    public void testDeleteAutoTaggingRule() throws Exception {
        String workloadGroupId = "wlm_test_group_delete";

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "wlm_test_group_delete",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.05, ResourceType.MEMORY, 0.05)
            ),
            Instant.now().getMillis()
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Rule rule = new Rule(
            "delete_rule_id",
            "To be deleted",
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-delete-*")),
            featureType,
            workloadGroupId,
            "2025-06-20T00:00:00.000Z"
        );

        CreateRuleResponse createResponse = client().execute(CreateRuleAction.INSTANCE, new CreateRuleRequest(rule)).get();
        assertEquals("delete_rule_id", createResponse.getRule().getId());

        AcknowledgedResponse deleteResponse = client().execute(
            DeleteRuleAction.INSTANCE,
            new DeleteRuleRequest("delete_rule_id", featureType)
        ).get();

        assertTrue(deleteResponse.isAcknowledged());
    }

    public void testCreateRuleWithMultipleAttributeValues() throws Exception {
        String workloadGroupId = "multi_attr_wlm_group";

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "multi_attr_group",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.15, ResourceType.MEMORY, 0.2)
            ),
            Instant.now().getMillis()
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributes = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*", "metrics-*", "events-*"));

        Rule rule = new Rule(RULE_ID, "Multiple index patterns rule", attributes, featureType, workloadGroupId, Instant.now().toString());

        CreateRuleRequest createRequest = new CreateRuleRequest(rule);
        CreateRuleResponse createResponse = client().execute(CreateRuleAction.INSTANCE, createRequest).get();

        assertNotNull("CreateRuleResponse should not be null", createResponse);
        Rule createdRule = createResponse.getRule();
        assertEquals("Rule ID mismatch", RULE_ID, createdRule.getId());
        assertEquals("Description mismatch", "Multiple index patterns rule", createdRule.getDescription());

        Set<String> returnedPatterns = createdRule.getAttributeMap().get(RuleAttribute.INDEX_PATTERN);
        assertTrue("Should contain logs-*", returnedPatterns.contains("logs-*"));
        assertTrue("Should contain metrics-*", returnedPatterns.contains("metrics-*"));
        assertTrue("Should contain events-*", returnedPatterns.contains("events-*"));
    }

    public void testCreateRuleWithInvalidWorkloadGroupFails() throws Exception {
        String invalidGroupId = "nonexistent_group_id";
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");

        Rule rule = new Rule(
            "invalid_rule_id",
            "Should fail",
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-invalid-*")),
            featureType,
            invalidGroupId,
            "2025-06-20T00:00:00.000Z"
        );

        CreateRuleRequest request = new CreateRuleRequest(rule);

        Exception exception = expectThrows(ExecutionException.class, () -> client().execute(CreateRuleAction.INSTANCE, request).get());

        assertTrue(exception.getCause().getMessage().contains("is not a valid workload group id."));
    }

    public void testCreateMultipleAutoTaggingRules() throws Exception {
        String workloadGroupId = "wlm_test_group_multiple";

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "wlm_test_group_multiple",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.5, ResourceType.MEMORY, 0.5)
            ),
            Instant.now().getMillis()
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);

        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");

        List<Rule> rules = List.of(
            new Rule(
                "rule_1",
                "Rule One",
                Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*")),
                featureType,
                workloadGroupId,
                Instant.now().toString()
            ),
            new Rule(
                "rule_2",
                "Rule Two",
                Map.of(RuleAttribute.INDEX_PATTERN, Set.of("metrics-*")),
                featureType,
                workloadGroupId,
                Instant.now().toString()
            ),
            new Rule(
                "rule_3",
                "Rule Three",
                Map.of(RuleAttribute.INDEX_PATTERN, Set.of("traces-*")),
                featureType,
                workloadGroupId,
                Instant.now().toString()
            )
        );

        for (Rule rule : rules) {
            CreateRuleRequest createRequest = new CreateRuleRequest(rule);
            CreateRuleResponse createResponse = client().execute(CreateRuleAction.INSTANCE, createRequest).get();
            assertNotNull("Create response should not be null for " + rule.getId(), createResponse);
            assertEquals(rule.getId(), createResponse.getRule().getId());
        }

        client().admin().indices().prepareRefresh().get();

        for (Rule rule : rules) {
            GetRuleRequest getRequest = new GetRuleRequest(rule.getId(), Map.of(), null, featureType);
            GetRuleResponse getResponse = client().execute(GetRuleAction.INSTANCE, getRequest).get();
            assertNotNull(getResponse);
            assertFalse("Rule " + rule.getId() + " should exist", getResponse.getRules().isEmpty());
            Rule retrieved = getResponse.getRules().get(0);
            assertEquals(rule.getDescription(), retrieved.getDescription());
            assertEquals(rule.getFeatureValue(), retrieved.getFeatureValue());
        }
    }

    public void updateWorkloadGroupInClusterState(String method, WorkloadGroup workloadGroup) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(workloadGroup, method), listener);
        assertTrue(listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
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
     * Plugin class for WorkloadManagement
     */
    public static class TestWorkloadManagementPlugin extends Plugin
        implements
            ActionPlugin,
            SystemIndexPlugin,
            DiscoveryPlugin,
            RuleFrameworkExtension {

        /**
         * The name of the index where rules are stored.
         */
        public static final String INDEX_NAME = ".wlm_rules";
        /**
         * The maximum number of rules allowed per GET request.
         */
        public static final int MAX_RULES_PER_PAGE = 50;
        static FeatureType featureType;
        static RulePersistenceService rulePersistenceService;
        static RuleRoutingService ruleRoutingService;
        private AutoTaggingActionFilter autoTaggingActionFilter;

        /**
         * Default constructor
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
            featureType = new WorkloadGroupFeatureType(new WorkloadGroupFeatureValueValidator(clusterService));
            RuleEntityParser parser = new XContentRuleParser(featureType);
            AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
                featureType,
                DefaultAttributeValueStore::new
            );
            InMemoryRuleProcessingService ruleProcessingService = new InMemoryRuleProcessingService(attributeValueStoreFactory);
            rulePersistenceService = new IndexStoredRulePersistenceService(
                INDEX_NAME,
                client,
                clusterService,
                MAX_RULES_PER_PAGE,
                parser,
                new IndexBasedRuleQueryMapper()
            );
            ruleRoutingService = new WorkloadGroupRuleRoutingService(client, clusterService);

            RefreshBasedSyncMechanism refreshMechanism = new RefreshBasedSyncMechanism(
                threadPool,
                clusterService.getSettings(),
                clusterService.getClusterSettings(),
                parser,
                ruleProcessingService,
                featureType,
                rulePersistenceService,
                new RuleEventClassifier(Collections.emptySet(), ruleProcessingService)
            );

            autoTaggingActionFilter = new AutoTaggingActionFilter(ruleProcessingService, threadPool);
            return List.of(refreshMechanism);
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
            return List.of(
                new RestCreateWorkloadGroupAction(),
                new RestGetWorkloadGroupAction(),
                new RestDeleteWorkloadGroupAction(),
                new RestUpdateWorkloadGroupAction()
            );
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT,
                RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING
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
    }
}
