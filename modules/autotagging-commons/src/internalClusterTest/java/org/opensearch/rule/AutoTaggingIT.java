/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.spi.RuleFrameworkExtension;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

public class AutoTaggingIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public AutoTaggingIT(Settings nodeSettings) {
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

    public static class IndexPatternAttribute implements Attribute {
        private static final String NAME = "index_pattern";
        public static final IndexPatternAttribute INSTANCE = new IndexPatternAttribute();

        private IndexPatternAttribute() {
            validateAttribute();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof IndexPatternAttribute;
        }

        @Override
        public int hashCode() {
            return NAME.hashCode();
        }
    }

    public void testCreateRuleWithSingleAttribute() throws Exception {
        FeatureType featureType = new TestFeatureType();
        Map<Attribute, Set<String>> attributeMap = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*"));

        Rule rule = new Rule("mock_create", "desc", attributeMap, featureType, "feature1", "2025-06-20T00:00:00.000Z");
        CreateRuleRequest request = new CreateRuleRequest(rule);
        PlainActionFuture<CreateRuleResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().createRule(request, future);
        CreateRuleResponse response = future.get();

        assertEquals("mock_create", response.getRule().getId());
        assertEquals("feature1", response.getRule().getFeatureValue());
    }

    public void testGetExistingRule() throws Exception {
        FeatureType featureType = new TestFeatureType();
        Map<Attribute, Set<String>> attributeMap = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*"));

        GetRuleRequest request = new GetRuleRequest("mock_id", attributeMap, "token", featureType);
        PlainActionFuture<GetRuleResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().getRule(request, future);
        GetRuleResponse response = future.get();

        assertEquals(1, response.getRules().size());
        assertEquals("mock_id", response.getRules().get(0).getId());
    }

    public void testUpdateRuleValueChange() throws Exception {
        FeatureType featureType = new TestFeatureType();
        Map<Attribute, Set<String>> attributeMap = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*"));

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
        FeatureType featureType = new TestFeatureType();
        DeleteRuleRequest request = new DeleteRuleRequest("mock_id", featureType);
        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().deleteRule(request, future);
        AcknowledgedResponse response = future.get();

        assertTrue(response.isAcknowledged());
    }

    public void testCreateRuleWithMultiplePatterns() throws Exception {
        FeatureType featureType = new TestFeatureType();
        Map<Attribute, Set<String>> attributeMap = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*", "metrics-*", "events-*"));

        Rule rule = new Rule("multi_pattern", "desc", attributeMap, featureType, "value", "2025-06-20T00:00:00.000Z");
        CreateRuleRequest request = new CreateRuleRequest(rule);
        PlainActionFuture<CreateRuleResponse> future = PlainActionFuture.newFuture();

        new TestRulePersistenceService().createRule(request, future);
        CreateRuleResponse response = future.get();

        assertEquals("multi_pattern", response.getRule().getId());
        assertTrue(response.getRule().getAttributeMap().get(IndexPatternAttribute.INSTANCE).contains("events-*"));
    }

    public void testCreateRuleWithExistingId() throws Exception {
        FeatureType featureType = new TestFeatureType();
        Rule rule1 = new Rule(
            "duplicate_id",
            "desc1",
            Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*")),
            featureType,
            "v1",
            "2025-06-20T00:00:00.000Z"
        );
        Rule rule2 = new Rule(
            "duplicate_id",
            "desc2",
            Map.of(IndexPatternAttribute.INSTANCE, Set.of("metrics-*")),
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

    public void testRuleBasedTaggingEndToEnd() throws Exception {
        String indexName = "logs-tagged-index";
        String docId = "doc1";

        // 1. Create index
        client().admin().indices().prepareCreate(indexName).get();

        // 2. Index a sample document
        client().prepareIndex(indexName).setId(docId).setSource(Map.of("message", "This is a test log"), XContentType.JSON).get();

        // 3. Refresh index to make doc searchable
        client().admin().indices().prepareRefresh(indexName).get();

        // 4. Create a rule with pattern "logs-*"
        FeatureType featureType = new TestFeatureType();
        Map<Attribute, Set<String>> attrMap = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*"));
        Rule rule = new Rule("rule_1", "desc", attrMap, featureType, "test_workload_group", Instant.now().toString());
        CreateRuleRequest ruleRequest = new CreateRuleRequest(rule);

        // Send to backend using transport action (or REST if implemented)
        PlainActionFuture<CreateRuleResponse> ruleFuture = PlainActionFuture.newFuture();
        new TestRulePersistenceService().createRule(ruleRequest, ruleFuture);
        CreateRuleResponse ruleResponse = ruleFuture.get();
        assertEquals("rule_1", ruleResponse.getRule().getId());

        // 5. Run a query matching the rule
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();

        assertEquals(1, searchResponse.getHits().getHits().length);
    }

    public static class TestRuleFrameworkPlugin extends Plugin implements RuleFrameworkExtension {
        @Override
        public Supplier<FeatureType> getFeatureTypeSupplier() {
            return TestFeatureType::new;
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

    public static class TestFeatureType implements FeatureType {
        @Override
        public String getName() {
            return "test_feature";
        }

        @Override
        public Map<String, Attribute> getAllowedAttributesRegistry() {
            return Map.of("index_pattern", IndexPatternAttribute.INSTANCE);
        }

        @Override
        public Attribute getAttributeFromName(String name) {
            return getAllowedAttributesRegistry().get(name);
        }
    }

    public static class TestRulePersistenceService implements RulePersistenceService {
        private static final FeatureType TEST_FEATURE_TYPE = new TestFeatureType();
        private static final Map<Attribute, Set<String>> TEST_ATTRIBUTES = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*"));

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
        private static final FeatureType TEST_FEATURE_TYPE = new TestFeatureType();
        private static final Map<Attribute, Set<String>> TEST_ATTRIBUTES = Map.of(IndexPatternAttribute.INSTANCE, Set.of("logs-*"));

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
