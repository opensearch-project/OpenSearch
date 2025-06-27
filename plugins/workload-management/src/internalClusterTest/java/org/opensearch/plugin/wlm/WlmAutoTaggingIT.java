/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.junit.After;
import org.junit.Before;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rule.*;
import org.opensearch.rule.action.*;
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
        plugins.add(TestRuleFrameworkPlugin.class);
        plugins.add(WorkloadManagementPlugin.class);
        return plugins;
    }

    @BeforeClass
    public static void registerWorkloadGroupFeatureType() {
        AutoTaggingRegistry.registerFeatureType(new WorkloadGroupFeatureType(featureValue -> {}));
    }

    private static final String RULE_ID = "test_rule_id";



    public void testCreateAutoTaggingRule() throws Exception {
        FeatureType featureType = AutoTaggingRegistry.getFeatureType("workload_group");
        Map<Attribute, Set<String>> attributes = Map.of(
            RuleAttribute.INDEX_PATTERN, Set.of("logs-*")
        );

        Rule rule = new Rule(
            RULE_ID,
            "Integration test rule",
            attributes,
            featureType,
            "wlm_group_id_123",
            "2025-06-20T00:00:00.000Z"
        );

        CreateRuleRequest createRequest = new CreateRuleRequest(rule);
        CreateRuleResponse response = client().execute(CreateRuleAction.INSTANCE, createRequest).get();

        assertNotNull(response);
        assertEquals(RULE_ID, response.getRule().getId());
        assertEquals("wlm_group_id_123", response.getRule().getFeatureValue());
    }

    public static class TestRuleFrameworkPlugin extends Plugin implements RuleFrameworkExtension, ActionPlugin {
        public TestRuleFrameworkPlugin() {
            super();
        }

        @Override
        public Supplier<FeatureType> getFeatureTypeSupplier() {
            return () -> new WorkloadGroupFeatureType(v -> {});
        }

        @Override
        public Supplier<RulePersistenceService> getRulePersistenceServiceSupplier() {
            return TestRulePersistenceService::new;
        }

        @Override
        public Supplier<RuleRoutingService> getRuleRoutingServiceSupplier() {
            return TestRuleRoutingService::new;
        }

        @Override
        public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(
                new ActionPlugin.ActionHandler<>(CreateRuleAction.INSTANCE, TransportCreateRuleAction.class),
                new ActionPlugin.ActionHandler<>(GetRuleAction.INSTANCE, TransportGetRuleAction.class),
                new ActionPlugin.ActionHandler<>(UpdateRuleAction.INSTANCE, TransportUpdateRuleAction.class),
                new ActionPlugin.ActionHandler<>(DeleteRuleAction.INSTANCE, TransportDeleteRuleAction.class)
            );
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
