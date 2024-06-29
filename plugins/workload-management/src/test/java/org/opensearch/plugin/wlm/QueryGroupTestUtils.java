/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class QueryGroupTestUtils {
    public static final String SANDBOX_MAX_SETTING_NAME = "node.sandbox.max_count";
    public static final String NAME_ONE = "sandbox_one";
    public static final String NAME_TWO = "sandbox_two";
    public static final String _ID_ONE = "AgfUO5Ja9yfsYlONlYi3TQ==";
    public static final String _ID_TWO = "G5iIqHy4g7eK1qIAAAAIH53=1";
    public static final String NAME_NONE_EXISTED = "sandbox_none_existed";
    public static final String MONITOR = "monitor";
    public static final long TIMESTAMP_ONE = 4513232413L;
    public static final long TIMESTAMP_TWO = 4513232415L;
    public static final QueryGroup queryGroupOne = builder().name(NAME_ONE)
        ._id(_ID_ONE)
        .mode(MONITOR)
        .resourceLimits(Map.of("jvm", 0.3))
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final QueryGroup queryGroupTwo = builder().name(NAME_TWO)
        ._id(_ID_TWO)
        .mode(MONITOR)
        .resourceLimits(Map.of("jvm", 0.6))
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static final Map<String, QueryGroup> queryGroupMap = Map.of(NAME_ONE, queryGroupOne, NAME_TWO, queryGroupTwo);

    public static List<QueryGroup> queryGroupList() {
        return List.of(queryGroupOne, queryGroupTwo);
    }

    public static ClusterState clusterState() {
        final Metadata metadata = Metadata.builder().queryGroups(new HashSet<>(queryGroupList())).build();
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    public static Settings settings() {
        return Settings.builder().build();
    }

    public static ClusterSettings clusterSettings() {
        return new ClusterSettings(settings(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    public static QueryGroupPersistenceService queryGroupPersistenceService() {
        ClusterService clusterService = new ClusterService(settings(), clusterSettings(), mock(ThreadPool.class));
        return new QueryGroupPersistenceService(clusterService, settings(), clusterSettings());
    }

    public static List<Object> prepareSandboxPersistenceService(List<QueryGroup> queryGroups) {
        Metadata metadata = Metadata.builder().queryGroups(new HashSet<>(queryGroups)).build();
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        return List.of(queryGroupPersistenceService, clusterState);
    }

    public static void compareResourceLimits(Map<String, Object> resourceLimitMapOne, Map<String, Object> resourceLimitMapTwo) {
        for (String resourceName : resourceLimitMapOne.keySet()) {
            assertTrue(resourceLimitMapTwo.containsKey(resourceName));
            assertEquals(resourceLimitMapOne.get(resourceName), resourceLimitMapTwo.get(resourceName));
        }
    }

    public static void compareQueryGroups(List<QueryGroup> listOne, List<QueryGroup> listTwo) {
        assertEquals(listOne.size(), listTwo.size());
        for (QueryGroup groupOne : listOne) {
            String groupOneName = groupOne.getName();
            List<QueryGroup> groupTwoList = listTwo.stream().filter(sb -> sb.getName().equals(groupOneName)).collect(Collectors.toList());
            assertEquals(1, groupTwoList.size());
            QueryGroup groupTwo = groupTwoList.get(0);
            assertEquals(groupOne.getName(), groupTwo.getName());
            assertEquals(groupOne.get_id(), groupTwo.get_id());
            compareResourceLimits(groupOne.getResourceLimits(), groupTwo.getResourceLimits());
            assertEquals(groupOne.getMode(), groupTwo.getMode());
            assertEquals(groupOne.getUpdatedAtInMillis(), groupTwo.getUpdatedAtInMillis());
        }
    }

    public static void assertInflightValuesAreZero(QueryGroupPersistenceService queryGroupPersistenceService) {
        assertEquals(0, queryGroupPersistenceService.getInflightCreateQueryGroupRequestCount().get());
        Map<String, DoubleAdder> inflightResourceMap = queryGroupPersistenceService.getInflightResourceLimitValues();
        if (inflightResourceMap != null) {
            for (String resourceName : inflightResourceMap.keySet()) {
                assertEquals(0, inflightResourceMap.get(resourceName).intValue());
            }
        }
    }
}
