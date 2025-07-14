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
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.wlm.rule.sync.RefreshBasedSyncMechanism;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.metadata.WorkloadGroup.builder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class WorkloadManagementTestUtils {
    public static final String NAME_ONE = "workload_group_one";
    public static final String NAME_TWO = "workload_group_two";
    public static final String _ID_ONE = "AgfUO5Ja9yfsYlONlYi3TQ==";
    public static final String _ID_TWO = "G5iIqHy4g7eK1qIAAAAIH53=1";
    public static final String NAME_NONE_EXISTED = "workload_group_none_existed";
    public static final long TIMESTAMP_ONE = 4513232413L;
    public static final long TIMESTAMP_TWO = 4513232415L;
    public static final WorkloadGroup workloadGroupOne = builder().name(NAME_ONE)
        ._id(_ID_ONE)
        .mutableWorkloadGroupFragment(
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.3))
        )
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final WorkloadGroup workloadGroupTwo = builder().name(NAME_TWO)
        ._id(_ID_TWO)
        .mutableWorkloadGroupFragment(
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.6))
        )
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static List<WorkloadGroup> workloadGroupList() {
        List<WorkloadGroup> list = new ArrayList<>();
        list.add(workloadGroupOne);
        list.add(workloadGroupTwo);
        return list;
    }

    public static ClusterState clusterState() {
        final Metadata metadata = Metadata.builder().workloadGroups(Map.of(_ID_ONE, workloadGroupOne, _ID_TWO, workloadGroupTwo)).build();
        return ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
    }

    public static Set<Setting<?>> clusterSettingsSet() {
        Set<Setting<?>> set = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        set.add(WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT);
        assertFalse(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.contains(WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT));
        return set;
    }

    public static Settings settings() {
        return Settings.builder().build();
    }

    public static ClusterSettings clusterSettings() {
        return new ClusterSettings(settings(), clusterSettingsSet());
    }

    public static WorkloadGroupPersistenceService workloadGroupPersistenceService() {
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
        return new WorkloadGroupPersistenceService(clusterService, settings(), clusterSettings());
    }

    public static Tuple<WorkloadGroupPersistenceService, ClusterState> preparePersistenceServiceSetup(
        Map<String, WorkloadGroup> workloadGroups
    ) {
        Metadata metadata = Metadata.builder().workloadGroups(workloadGroups).build();
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
        WorkloadGroupPersistenceService workloadGroupPersistenceService = new WorkloadGroupPersistenceService(
            clusterService,
            settings,
            clusterSettings
        );
        return new Tuple<WorkloadGroupPersistenceService, ClusterState>(workloadGroupPersistenceService, clusterState);
    }

    public static void assertEqualResourceLimits(
        Map<ResourceType, Double> resourceLimitMapOne,
        Map<ResourceType, Double> resourceLimitMapTwo
    ) {
        assertTrue(resourceLimitMapOne.keySet().containsAll(resourceLimitMapTwo.keySet()));
        assertTrue(resourceLimitMapOne.values().containsAll(resourceLimitMapTwo.values()));
    }

    public static void assertEqualWorkloadGroups(
        Collection<WorkloadGroup> collectionOne,
        Collection<WorkloadGroup> collectionTwo,
        boolean assertUpdateAt
    ) {
        assertEquals(collectionOne.size(), collectionTwo.size());
        List<WorkloadGroup> listOne = new ArrayList<>(collectionOne);
        List<WorkloadGroup> listTwo = new ArrayList<>(collectionTwo);
        listOne.sort(Comparator.comparing(WorkloadGroup::getName));
        listTwo.sort(Comparator.comparing(WorkloadGroup::getName));
        for (int i = 0; i < listOne.size(); i++) {
            if (assertUpdateAt) {
                WorkloadGroup one = listOne.get(i);
                WorkloadGroup two = listTwo.get(i);
                assertEquals(one.getName(), two.getName());
                assertEquals(one.getResourceLimits(), two.getResourceLimits());
                assertEquals(one.getResiliencyMode(), two.getResiliencyMode());
                assertEquals(one.get_id(), two.get_id());
            } else {
                assertEquals(listOne.get(i), listTwo.get(i));
            }
        }
    }

    public static WlmClusterSettingValuesProvider setUpNonPluginSettingValuesProvider(String wlmMode) throws Exception {
        try (WorkloadManagementPlugin plugin = new WorkloadManagementPlugin()) {
            Settings settings = Settings.builder()
                .put(RefreshBasedSyncMechanism.RULE_SYNC_REFRESH_INTERVAL_SETTING_NAME, 1000)
                .put(WorkloadManagementSettings.WLM_MODE_SETTING_NAME, wlmMode)
                .build();
            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(plugin.getSettings()));
            clusterSettings.registerSetting(WorkloadManagementSettings.WLM_MODE_SETTING);
            return new WlmClusterSettingValuesProvider(settings, clusterSettings);
        }
    }
}
