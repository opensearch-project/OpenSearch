/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.cluster.metadata.ResourceLimitGroup.ResourceLimit;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.resource_limit_group.service.ResourceLimitGroupPersistenceService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;

public class ResourceLimitGroupTestUtils extends OpenSearchTestCase {
    public static final String SANDBOX_MAX_SETTING_NAME = "node.sandbox.max_count";
    public static final String NAME_ONE = "sandbox_one";
    public static final String NAME_TWO = "sandbox_two";
    public static final String NAME_NONE_EXISTED = "sandbox_none_existed";
    public static final String MONITOR = "monitor";
    public static final ResourceLimitGroup resourceLimitGroupOne = new ResourceLimitGroup(NAME_ONE, List.of(new ResourceLimitGroup.ResourceLimit("jvm", 0.3)), MONITOR);
    public static final ResourceLimitGroup resourceLimitGroupTwo = new ResourceLimitGroup(NAME_TWO, List.of(new ResourceLimitGroup.ResourceLimit("jvm", 0.6)), MONITOR);
    public static final Map<String, ResourceLimitGroup> resourceLimitGroupMap = Map.of(
        NAME_ONE, resourceLimitGroupOne,
        NAME_TWO, resourceLimitGroupTwo
    );
    public static final List<ResourceLimitGroup> resourceLimitGroupList= List.of(resourceLimitGroupOne, resourceLimitGroupTwo);
    public static final Metadata metadata = Metadata.builder().resourceLimitGroups(resourceLimitGroupMap).build();
    public static final ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();

    public static final Settings settings = Settings.builder().build();
    public static final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    public static final ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
    public static final ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService = new ResourceLimitGroupPersistenceService(clusterService, settings, clusterSettings);


    public static List<Object> prepareSandboxPersistenceService(List<ResourceLimitGroup> resourceLimitGroups) {
        Map<String, ResourceLimitGroup> resourceLimitGroupMap = new HashMap<>();
        for (ResourceLimitGroup group : resourceLimitGroups) {
            resourceLimitGroupMap.put(group.getName(), group);
        }
        Metadata metadata = Metadata.builder().resourceLimitGroups(resourceLimitGroupMap).build();
        Settings settings = Settings.builder().build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        ResourceLimitGroupPersistenceService resourceLimitGroupPersistenceService = new ResourceLimitGroupPersistenceService(clusterService, settings, clusterSettings);
        return List.of(resourceLimitGroupPersistenceService, clusterState);
    }

    public static void compareResourceLimits(List<ResourceLimit> limitsOne, List<ResourceLimit> limitsTwo) {
        assertEquals(limitsOne.size(), limitsTwo.size());
        Map<String, Double> resourceLimitMapOne = limitsOne.stream()
            .collect(Collectors.toMap(ResourceLimitGroup.ResourceLimit::getResourceName, ResourceLimitGroup.ResourceLimit::getValue));
        Map<String, Double> resourceLimitMapTwo = limitsTwo.stream()
            .collect(Collectors.toMap(ResourceLimitGroup.ResourceLimit::getResourceName, ResourceLimitGroup.ResourceLimit::getValue));
        for (String resourceName : resourceLimitMapOne.keySet()) {
            assertTrue(resourceLimitMapTwo.containsKey(resourceName));
            assertEquals(resourceLimitMapOne.get(resourceName), resourceLimitMapTwo.get(resourceName));
        }
    }

    public static void compareResourceLimitGroups(List<ResourceLimitGroup> listOne, List<ResourceLimitGroup> listTwo) {
        assertEquals(listOne.size(), listTwo.size());
        for (ResourceLimitGroup groupOne : listOne) {
            String groupOneName = groupOne.getName();
            List<ResourceLimitGroup> groupTwoList = listTwo.stream().filter(sb -> sb.getName().equals(groupOneName)).collect(Collectors.toList());
            assertEquals(1, groupTwoList.size());
            ResourceLimitGroup groupTwo = groupTwoList.get(0);
            assertEquals(groupOne.getName(), groupTwo.getName());
            compareResourceLimits(groupOne.getResourceLimits(), groupTwo.getResourceLimits());
            assertEquals(groupOne.getEnforcement(), groupTwo.getEnforcement());
        }
    }
}

