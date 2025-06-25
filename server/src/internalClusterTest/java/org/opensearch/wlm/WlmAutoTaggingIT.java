/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.wlm.WorkloadManagementPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rule.RuleFrameworkPlugin;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        return List.of(WorkloadManagementPlugin.class, RuleFrameworkPlugin.class);
    }

    public void testAutoTaggingIntegration() throws Exception {
        String index = "logs-integration";
        String docId = "1";

        // 1. Create the index
        client().admin().indices().prepareCreate(index).get();

        // 2. Index a document
        client().prepareIndex(index).setId(docId).setSource(Map.of("message", "integration test log"), XContentType.JSON).get();

        client().admin().indices().prepareRefresh(index).get();

        // 3. Retrieve registered feature type from AutoTaggingRegistry
        FeatureType featureType = FeatureType.from("workload_group");

        // 4. Prepare valid attributes
        Attribute indexPatternAttr = featureType.getAttributeFromName("index_pattern");
        assertNotNull("index_pattern attribute should exist", indexPatternAttr);

        Map<Attribute, Set<String>> attributes = Map.of(indexPatternAttr, Set.of("logs-*"));

        // 5. Create and send the rule
        Rule rule = new Rule("integration_test_rule", "desc", attributes, featureType, "group_A", Instant.now().toString());

        CreateRuleRequest createReq = new CreateRuleRequest(rule);
        CreateRuleResponse ruleResponse = client().execute(CreateRuleAction.INSTANCE, createReq).actionGet();

        assertEquals("integration_test_rule", ruleResponse.getRule().getId());

        // 6. Run a search to verify tagging logic (manual inspection or later automation)
        SearchResponse searchResponse = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();

        assertEquals(1, searchResponse.getHits().getHits().length);
    }
}
