/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugins.Plugin;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RuleRoutingService;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.spi.RuleFrameworkExtension;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

public class WlmAutoTaggingIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

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
        return plugins;
    }

    @BeforeClass
    public static void registerWorkloadGroupFeatureType() {
        AutoTaggingRegistry.registerFeatureType(new WorkloadGroupFeatureType(featureValue -> {}));
    }

    public void testCreateRuleWithSingleAttribute() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributeMap = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        Rule rule = new Rule("mock_create", "desc", attributeMap, featureType, "feature1", "2025-06-20T00:00:00.000Z");
        CreateRuleRequest request = new CreateRuleRequest(rule);
        PlainActionFuture<CreateRuleResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().createRule(request, future);
        CreateRuleResponse response = future.get();

        assertEquals("mock_create", response.getRule().getId());
        assertEquals("feature1", response.getRule().getFeatureValue());
    }

    public void testGetExistingRule() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributeMap = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        GetRuleRequest request = new GetRuleRequest("mock_id", attributeMap, "token", featureType);
        PlainActionFuture<GetRuleResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().getRule(request, future);
        GetRuleResponse response = future.get();

        assertEquals(1, response.getRules().size());
        assertEquals("mock_id", response.getRules().get(0).getId());
    }

    public void testUpdateRuleValueChange() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributeMap = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        Rule updatedRule = new Rule("mock_id", "updated desc", attributeMap, featureType, "new_feature_value", "2025-06-20T00:00:00.000Z");
        UpdateRuleRequest request = new UpdateRuleRequest(
            updatedRule.getId(),
            updatedRule.getDescription(),
            updatedRule.getAttributeMap(),
            updatedRule.getFeatureValue(),
            updatedRule.getFeatureType()
        );

        PlainActionFuture<UpdateRuleResponse> future = PlainActionFuture.newFuture();
        new TestRulePersistenceService().updateRule(request, future);
        UpdateRuleResponse response = future.get();

        assertEquals("new_feature_value", response.getRule().getFeatureValue());
    }

    public void testDeleteRule() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        DeleteRuleRequest request = new DeleteRuleRequest("mock_id", featureType);
        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().deleteRule(request, future);
        AcknowledgedResponse response = future.get();

        assertTrue(response.isAcknowledged());
    }

    public void testCreateRuleWithMultiplePatterns() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributeMap = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*", "metrics-*", "events-*"));

        Rule rule = new Rule("multi_pattern", "desc", attributeMap, featureType, "value", "2025-06-20T00:00:00.000Z");
        CreateRuleRequest request = new CreateRuleRequest(rule);
        PlainActionFuture<CreateRuleResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().createRule(request, future);
        CreateRuleResponse response = future.get();

        assertEquals("multi_pattern", response.getRule().getId());
        assertTrue(response.getRule().getAttributeMap().get(RuleAttribute.INDEX_PATTERN).contains("events-*"));
    }

    public void testCreateRuleWithExistingId() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Rule rule1 = new Rule(
            "duplicate_id",
            "desc1",
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*")),
            featureType,
            "v1",
            "2025-06-20T00:00:00.000Z"
        );
        Rule rule2 = new Rule(
            "duplicate_id",
            "desc2",
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of("metrics-*")),
            featureType,
            "v2",
            "2025-06-20T00:00:00.000Z"
        );

        CreateRuleRequest request1 = new CreateRuleRequest(rule1);
        CreateRuleRequest request2 = new CreateRuleRequest(rule2);
        PlainActionFuture<CreateRuleResponse> future1 = PlainActionFuture.newFuture();
        PlainActionFuture<CreateRuleResponse> future2 = PlainActionFuture.newFuture();

        new TestRulePersistenceService().createRule(request1, future1);
        new TestRulePersistenceService().createRule(request2, future2);

        assertEquals("duplicate_id", future1.get().getRule().getId());
        assertEquals("duplicate_id", future2.get().getRule().getId());
    }

    public static class TestRuleFrameworkPlugin extends Plugin implements RuleFrameworkExtension {
        @Override
        public Supplier<FeatureType> getFeatureTypeSupplier() {
            return () -> new WorkloadGroupFeatureType(featureValue -> {});
        }

        @Override
        public Supplier<RulePersistenceService> getRulePersistenceServiceSupplier() {
            return TestRulePersistenceService::new;
        }

        @Override
        public Supplier<RuleRoutingService> getRuleRoutingServiceSupplier() {
            return TestRuleRoutingService::new;
        }
    }

    public static class TestRulePersistenceService implements RulePersistenceService {
        private static final FeatureType TEST_FEATURE_TYPE = AutoTaggingRegistry.getFeatureType("workload_group");
        private static final Map<Attribute, Set<String>> TEST_ATTRIBUTES = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        @Override
        public void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
            listener.onResponse(new CreateRuleResponse(request.getRule()));
        }

        @Override
        public void getRule(GetRuleRequest request, ActionListener<GetRuleResponse> listener) {
            Rule rule = new Rule(
                "mock_id",
                "test rule description",
                TEST_ATTRIBUTES,
                TEST_FEATURE_TYPE,
                "mock_feature_value",
                "2025-06-20T00:00:00.000Z"
            );
            listener.onResponse(new GetRuleResponse(List.of(rule), null));
        }

        @Override
        public void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
            Rule rule = new Rule(
                request.getId(),
                request.getDescription(),
                request.getAttributeMap(),
                request.getFeatureType(),
                request.getFeatureValue(),
                "2025-06-20T00:00:00.000Z"
            );
            listener.onResponse(new UpdateRuleResponse(rule));
        }

        @Override
        public void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener) {
            listener.onResponse(new AcknowledgedResponse(true));
        }
    }

    public static class TestRuleRoutingService implements RuleRoutingService {
        private static final FeatureType TEST_FEATURE_TYPE = AutoTaggingRegistry.getFeatureType("workload_group");
        private static final Map<Attribute, Set<String>> TEST_ATTRIBUTES = Map.of(RuleAttribute.INDEX_PATTERN, Set.of("logs-*"));

        @Override
        public void handleCreateRuleRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
            Rule rule = new Rule(
                "mock_id",
                "test rule description",
                TEST_ATTRIBUTES,
                TEST_FEATURE_TYPE,
                "mock_feature_value",
                "2025-06-20T00:00:00.000Z"
            );
            listener.onResponse(new CreateRuleResponse(rule));
        }

        @Override
        public void handleUpdateRuleRequest(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
            Rule rule = new Rule(
                "mock_rule_id",
                "updated rule description",
                TEST_ATTRIBUTES,
                TEST_FEATURE_TYPE,
                "updated_feature_value",
                "2025-06-20T00:00:00.000Z"
            );
            listener.onResponse(new UpdateRuleResponse(rule));
        }
    }
}
