/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.Rule;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugin.wlm.rule.QueryGroupAttribute;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.service.RulePersistenceService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RuleTestUtils {
    public static final String _ID_ONE = "AgfUO5Ja9yfvhdONlYi3TQ==";
    public static final String _ID_TWO = "G5iIq84j7eK1qIAAAAIH53=1";
    public static final String FEATURE_VALUE_ONE = "feature_value_one";
    public static final String FEATURE_VALUE_TWO = "feature_value_two";
    public static final String PATTERN_ONE = "pattern_1";
    public static final String PATTERN_TWO = "pattern_2";
    public static final String DESCRIPTION_ONE = "description_1";
    public static final String DESCRIPTION_TWO = "description_2";
    public static final String TIMESTAMP_ONE = "2024-01-26T08:58:57.558Z";
    public static final String TIMESTAMP_TWO = "2023-01-26T08:58:57.558Z";
    public static final String SEARCH_AFTER = "search_after_id";
    public static final Map<Attribute, Set<String>> ATTRIBUTE_MAP = Map.of(QueryGroupAttribute.INDEX_PATTERN, Set.of(PATTERN_ONE));
    public static final Rule ruleOne = Rule.builder()
        .description(DESCRIPTION_ONE)
        .featureType(QueryGroupFeatureType.INSTANCE)
        .featureValue(FEATURE_VALUE_ONE)
        .attributeMap(Map.of(QueryGroupAttribute.INDEX_PATTERN, Set.of(PATTERN_ONE)))
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final Rule ruleTwo = Rule.builder()
        .description(DESCRIPTION_TWO)
        .featureType(QueryGroupFeatureType.INSTANCE)
        .featureValue(FEATURE_VALUE_TWO)
        .attributeMap(Map.of(QueryGroupAttribute.INDEX_PATTERN, Set.of(PATTERN_TWO)))
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static Map<String, Rule> ruleMap() {
        return Map.of(_ID_ONE, ruleOne, _ID_TWO, ruleTwo);
    }

    public static RulePersistenceService setUpRulePersistenceService(Map<String, QueryGroup> queryGroupMap) {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        ThreadPool threadPool = mock(ThreadPool.class);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.queryGroups()).thenReturn(queryGroupMap);
        return new RulePersistenceService(clusterService, client);
    }

    public static void assertEqualRules(Map<String, Rule> mapOne, Map<String, Rule> mapTwo, boolean ruleUpdated) {
        assertEquals(mapOne.size(), mapTwo.size());
        for (Map.Entry<String, Rule> entry : mapOne.entrySet()) {
            String id = entry.getKey();
            assertTrue(mapTwo.containsKey(id));
            Rule one = mapOne.get(id);
            Rule two = mapTwo.get(id);
            assertEqualRule(one, two, ruleUpdated);
        }
    }

    public static void assertEqualRule(Rule one, Rule two, boolean ruleUpdated) {
        if (ruleUpdated) {
            assertEquals(one.getDescription(), two.getDescription());
            assertEquals(one.getFeatureType(), two.getFeatureType());
            assertEquals(one.getFeatureValue(), two.getFeatureValue());
            assertEquals(one.getAttributeMap(), two.getAttributeMap());
            assertEquals(one.getAttributeMap(), two.getAttributeMap());
        } else {
            assertEquals(one, two);
        }
    }
}
