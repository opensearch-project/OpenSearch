/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.cluster.metadata.Rule;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.opensearch.cluster.metadata.Rule.builder;

public class RuleTestUtils {
    public static final String _ID_ONE = "AgfUO5Ja9yfvhdONlYi3TQ==";
    public static final String _ID_TWO = "G5iIq84j7eK1qIAAAAIH53=1";
    public static final String LABEL_ONE = "label_one";
    public static final String LABEL_TWO = "label_two";
    public static final String WLM_STRING = "wlm";
    public static final String TIMESTAMP_ONE = "1738011537558";
    public static final String TIMESTAMP_TWO = "1738013454494";
    public static final Rule ruleOne = builder()
        ._id(_ID_ONE)
        .feature(WLM_STRING)
        .label(LABEL_ONE)
        .attributeMap(Map.of("index_pattern", List.of("pattern_1")))
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final Rule ruleTwo = builder()
        ._id(_ID_TWO)
        .feature(WLM_STRING)
        .label(LABEL_TWO)
        .attributeMap(Map.of("index_pattern", List.of("pattern_2", "pattern_3")))
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static List<Rule> ruleList() {
        List<Rule> list = new ArrayList<>();
        list.add(ruleOne);
        list.add(ruleTwo);
        return list;
    }
//
//    public static ClusterState clusterState() {
//        final Metadata metadata = Metadata.builder().queryGroups(Map.of(_ID_ONE, queryGroupOne, _ID_TWO, queryGroupTwo)).build();
//        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
//    }
//
//    public static Set<Setting<?>> clusterSettingsSet() {
//        Set<Setting<?>> set = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
//        set.add(QueryGroupPersistenceService.MAX_QUERY_GROUP_COUNT);
//        assertFalse(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(QueryGroupPersistenceService.MAX_QUERY_GROUP_COUNT));
//        return set;
//    }
//
//    public static Settings settings() {
//        return Settings.builder().build();
//    }
//
//    public static ClusterSettings clusterSettings() {
//        return new ClusterSettings(settings(), clusterSettingsSet());
//    }
//
//    public static QueryGroupPersistenceService queryGroupPersistenceService() {
//        ClusterApplierService clusterApplierService = new ClusterApplierService(
//            "name",
//            settings(),
//            clusterSettings(),
//            mock(ThreadPool.class)
//        );
//        clusterApplierService.setInitialState(clusterState());
//        ClusterService clusterService = new ClusterService(
//            settings(),
//            clusterSettings(),
//            mock(ClusterManagerService.class),
//            clusterApplierService
//        );
//        return new QueryGroupPersistenceService(clusterService, settings(), clusterSettings());
//    }
//
//    public static Tuple<QueryGroupPersistenceService, ClusterState> preparePersistenceServiceSetup(Map<String, QueryGroup> queryGroups) {
//        Metadata metadata = Metadata.builder().queryGroups(queryGroups).build();
//        Settings settings = Settings.builder().build();
//        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
//        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingsSet());
//        ClusterApplierService clusterApplierService = new ClusterApplierService(
//            "name",
//            settings(),
//            clusterSettings(),
//            mock(ThreadPool.class)
//        );
//        clusterApplierService.setInitialState(clusterState);
//        ClusterService clusterService = new ClusterService(
//            settings(),
//            clusterSettings(),
//            mock(ClusterManagerService.class),
//            clusterApplierService
//        );
//        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
//            clusterService,
//            settings,
//            clusterSettings
//        );
//        return new Tuple<QueryGroupPersistenceService, ClusterState>(queryGroupPersistenceService, clusterState);
//    }

//    public static void assertEqualResourceLimits(
//        Map<ResourceType, Double> resourceLimitMapOne,
//        Map<ResourceType, Double> resourceLimitMapTwo
//    ) {
//        assertTrue(resourceLimitMapOne.keySet().containsAll(resourceLimitMapTwo.keySet()));
//        assertTrue(resourceLimitMapOne.values().containsAll(resourceLimitMapTwo.values()));
//    }

    public static void assertEqualRules(
        Collection<Rule> collectionOne,
        Collection<Rule> collectionTwo,
        boolean ruleUpdated
    ) {
        assertEquals(collectionOne.size(), collectionTwo.size());
        List<Rule> listOne = new ArrayList<>(collectionOne);
        List<Rule> listTwo = new ArrayList<>(collectionTwo);
        listOne.sort(Comparator.comparing(Rule::get_id));
        listTwo.sort(Comparator.comparing(Rule::get_id));
        for (int i = 0; i < listOne.size(); i++) {
            if (ruleUpdated) {
                Rule one = listOne.get(i);
                Rule two = listTwo.get(i);
                assertEquals(one.get_id(), two.get_id());
                assertEquals(one.getFeature(), two.getFeature());
                assertEquals(one.getLabel(), two.getLabel());
                assertEquals(one.getAttributeMap(), two.getAttributeMap());
            } else {
                assertEquals(listOne.get(i), listTwo.get(i));
            }
        }
    }
}
