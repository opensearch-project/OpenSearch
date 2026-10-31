/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.rule.WorkloadGroupFeatureType;
import org.opensearch.plugin.wlm.spi.AttributeExtractorExtension;
import org.opensearch.plugins.Plugin;
import org.opensearch.rule.RuleAttribute;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.plugin.wlm.WorkloadManagementPlugin.PRINCIPAL_ATTRIBUTE_NAME;

/**
 * Runs the auto-tagging tests with a principal attribute registered, as the security plugin does
 */
public class WlmAutoTaggingWithPrincipalIT extends WlmAutoTaggingIT {

    public WlmAutoTaggingWithPrincipalIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return WlmAutoTaggingIT.parameters();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestWorkloadManagementPlugin.class);
        plugins.add(TestWorkloadManagementPluginWithPrincipal.class);
        return plugins;
    }

    public void testDeletingRuleKeepsOtherRulesOfGroupTagging() throws Exception {
        String workloadGroupId = "wlm_auto_tag_shared_group";
        String keptIndex = "kept_rule_index";
        String deletedIndex = "deleted_rule_index";

        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("shared_group", workloadGroupId));

        // Neither rule sets the principal, so both store the group under its empty value
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createRule("wlm_auto_tag_kept_rule", "kept rule", keptIndex, featureType, workloadGroupId);
        createRule("wlm_auto_tag_deleted_rule", "deleted rule", deletedIndex, featureType, workloadGroupId);
        indexDocument(keptIndex);
        indexDocument(deletedIndex);

        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> assertTrue("Expected kept rule to tag", searchCompletions(node, keptIndex, workloadGroupId) > 0));
            assertBusy(() -> assertTrue("Expected deleted rule to tag", searchCompletions(node, deletedIndex, workloadGroupId) > 0));
        }
        client().execute(DeleteRuleAction.INSTANCE, new DeleteRuleRequest("wlm_auto_tag_deleted_rule", featureType)).get();
        for (String node : internalCluster().getNodeNames()) {
            // Wait for the node to sync the deletion
            assertBusy(() -> assertEquals(0, searchCompletions(node, deletedIndex, workloadGroupId)));
            assertBusy(() -> assertTrue("Expected kept rule to still tag", searchCompletions(node, keptIndex, workloadGroupId) > 0));
        }
    }

    public void testDeletingRuleKeepsSameIndexPatternRuleOfGroupTagging() throws Exception {
        String workloadGroupId = "wlm_auto_tag_same_pattern_group";
        String sentinelGroupId = "wlm_auto_tag_sentinel_group";
        String sharedIndex = "same_pattern_index";
        String sentinelIndex = "sentinel_index";

        setWlmMode("enabled");
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("same_pattern_group", workloadGroupId));
        updateWorkloadGroupInClusterState(PUT, createWorkloadGroup("sentinel_group", sentinelGroupId));

        // Both rules set the same index_pattern for the same group and differ in the principal
        FeatureType featureType = AutoTaggingRegistry.getFeatureType(WorkloadGroupFeatureType.NAME);
        createPrincipalRule("wlm_auto_tag_test_user_rule", sharedIndex, "username|test_user", featureType, workloadGroupId);
        createPrincipalRule("wlm_auto_tag_other_user_rule", sharedIndex, "username|other_user", featureType, workloadGroupId);
        // Searches only match test_user's rule, so a later sentinel rule shows when a node has synced the other user's rule
        createRule("wlm_auto_tag_sentinel_rule", "sentinel rule", sentinelIndex, featureType, sentinelGroupId);
        indexDocument(sharedIndex);
        indexDocument(sentinelIndex);

        for (String node : internalCluster().getNodeNames()) {
            assertBusy(() -> assertTrue("Expected sentinel rule to tag", searchCompletions(node, sentinelIndex, sentinelGroupId) > 0));
            assertBusy(() -> assertTrue("Expected test_user rule to tag", searchCompletions(node, sharedIndex, workloadGroupId) > 0));
        }
        client().execute(DeleteRuleAction.INSTANCE, new DeleteRuleRequest("wlm_auto_tag_other_user_rule", featureType)).get();
        client().execute(DeleteRuleAction.INSTANCE, new DeleteRuleRequest("wlm_auto_tag_sentinel_rule", featureType)).get();
        for (String node : internalCluster().getNodeNames()) {
            // Wait for the node to sync both deletions
            assertBusy(() -> assertEquals(0, searchCompletions(node, sentinelIndex, sentinelGroupId)));
            assertBusy(() -> assertTrue("Expected test_user rule to still tag", searchCompletions(node, sharedIndex, workloadGroupId) > 0));
        }
    }

    private void createPrincipalRule(String ruleId, String indexPattern, String principal, FeatureType featureType, String workloadGroupId)
        throws Exception {
        Map<Attribute, Set<String>> attributes = Map.of(
            RuleAttribute.INDEX_PATTERN,
            Set.of(indexPattern),
            TestPrincipalAttribute.PRINCIPAL,
            Set.of(principal)
        );
        Rule rule = new Rule(ruleId, "principal rule", attributes, featureType, workloadGroupId, Instant.now().toString());
        client().execute(CreateRuleAction.INSTANCE, new CreateRuleRequest(rule)).get();
    }

    private int searchCompletions(String node, String indexName, String workloadGroupId) throws Exception {
        int completionsBefore = getCompletions(workloadGroupId);
        internalCluster().client(node).prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).get();
        return getCompletions(workloadGroupId) - completionsBefore;
    }

    /**
     * Test plugin that also registers a principal attribute and extractor, as the security plugin does
     */
    public static class TestWorkloadManagementPluginWithPrincipal extends TestWorkloadManagementPlugin {
        @Override
        protected void registerAttributes(Map<Attribute, Integer> attributes, Map<Attribute, AttributeExtractorExtension> extensions) {
            attributes.put(TestPrincipalAttribute.PRINCIPAL, 1);
            extensions.put(TestPrincipalAttribute.PRINCIPAL, TestPrincipalAttribute::extractor);
        }
    }

    /**
     * Test principal attribute, standing in for the security plugin's principal attribute
     */
    public enum TestPrincipalAttribute implements Attribute {
        PRINCIPAL;

        @Override
        public String getName() {
            return PRINCIPAL_ATTRIBUTE_NAME;
        }

        static AttributeExtractor<String> extractor() {
            return new AttributeExtractor<>() {
                @Override
                public Attribute getAttribute() {
                    return PRINCIPAL;
                }

                @Override
                public Iterable<String> extract() {
                    return List.of("username|test_user");
                }

                @Override
                public LogicalOperator getLogicalOperator() {
                    return LogicalOperator.OR;
                }
            };
        }
    }
}
