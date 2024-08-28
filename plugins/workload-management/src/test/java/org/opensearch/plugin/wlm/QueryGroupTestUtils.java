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
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.metadata.QueryGroup.builder;
import static org.opensearch.search.ResourceType.fromName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class QueryGroupTestUtils {
    public static final String NAME_ONE = "query_group_one";
    public static final String NAME_TWO = "query_group_two";
    public static final String _ID_ONE = "AgfUO5Ja9yfsYlONlYi3TQ==";
    public static final String _ID_TWO = "G5iIqHy4g7eK1qIAAAAIH53=1";
    public static final String NAME_NONE_EXISTED = "query_group_none_existed";
    public static final String MEMORY_STRING = "memory";
    public static final String MONITOR_STRING = "monitor";
    public static final long TIMESTAMP_ONE = 4513232413L;
    public static final long TIMESTAMP_TWO = 4513232415L;
    public static final QueryGroup queryGroupOne = builder().name(NAME_ONE)
        ._id(_ID_ONE)
        .mode(MONITOR_STRING)
        .resourceLimits(Map.of(fromName(MEMORY_STRING), 0.3))
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final QueryGroup queryGroupTwo = builder().name(NAME_TWO)
        ._id(_ID_TWO)
        .mode(MONITOR_STRING)
        .resourceLimits(Map.of(fromName(MEMORY_STRING), 0.6))
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static List<QueryGroup> queryGroupList() {
        List<QueryGroup> list = new ArrayList<>();
        list.add(queryGroupOne);
        list.add(queryGroupTwo);
        return list;
    }

    public static ClusterState clusterState() {
        final Metadata metadata = Metadata.builder().queryGroups(Map.of(_ID_ONE, queryGroupOne, _ID_TWO, queryGroupTwo)).build();
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    public static Set<Setting<?>> clusterSettingsSet() {
        Set<Setting<?>> set = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        set.add(QueryGroupPersistenceService.MAX_QUERY_GROUP_COUNT);
        assertFalse(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(QueryGroupPersistenceService.MAX_QUERY_GROUP_COUNT));
        return set;
    }

    public static Settings settings() {
        return Settings.builder().build();
    }

    public static ClusterSettings clusterSettings() {
        return new ClusterSettings(settings(), clusterSettingsSet());
    }

    public static QueryGroupPersistenceService queryGroupPersistenceService() {
        ClusterApplierService clusterApplierService = new ClusterApplierService(
            "name",
            settings(),
            clusterSettings(),
            mock(ThreadPool.class)
        );
        clusterApplierService.setInitialState(clusterState());
        ClusterService clusterService = new ClusterService(
            settings(),
            clusterSettings(),
            mock(ClusterManagerService.class),
            clusterApplierService
        );
        return new QueryGroupPersistenceService(clusterService, settings(), clusterSettings());
    }

    public static Tuple<QueryGroupPersistenceService, ClusterState> preparePersistenceServiceSetup(Map<String, QueryGroup> queryGroups) {
        Metadata metadata = Metadata.builder().queryGroups(queryGroups).build();
        Settings settings = Settings.builder().build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, clusterSettingsSet());
        ClusterApplierService clusterApplierService = new ClusterApplierService(
            "name",
            settings(),
            clusterSettings(),
            mock(ThreadPool.class)
        );
        clusterApplierService.setInitialState(clusterState);
        ClusterService clusterService = new ClusterService(
            settings(),
            clusterSettings(),
            mock(ClusterManagerService.class),
            clusterApplierService
        );
        QueryGroupPersistenceService queryGroupPersistenceService = new QueryGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        return new Tuple<QueryGroupPersistenceService, ClusterState>(queryGroupPersistenceService, clusterState);
    }

    public static void assertEqualQueryGroups(Collection<QueryGroup> collectionOne, Collection<QueryGroup> collectionTwo) {
        assertEquals(collectionOne.size(), collectionTwo.size());
        List<QueryGroup> listOne = new ArrayList<>(collectionOne);
        List<QueryGroup> listTwo = new ArrayList<>(collectionTwo);
        listOne.sort(Comparator.comparing(QueryGroup::getName));
        listTwo.sort(Comparator.comparing(QueryGroup::getName));
        for (int i = 0; i < listOne.size(); i++) {
            assertTrue(listOne.get(i).equals(listTwo.get(i)));
        }
    }
}
