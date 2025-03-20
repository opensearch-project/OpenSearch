/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.backpressure.SearchBackpressureIT.ExceptionCatchingListener;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.wlm.WorkloadManagementIT.TestClusterUpdatePlugin;
import org.opensearch.wlm.WorkloadManagementIT.TestClusterUpdateRequest;
import org.opensearch.wlm.WorkloadManagementIT.TestClusterUpdateTransportAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.wlm.QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER;
import static org.opensearch.wlm.WorkloadManagementIT.CPU;
import static org.opensearch.wlm.WorkloadManagementIT.DELETE;
import static org.opensearch.wlm.WorkloadManagementIT.MEMORY;
import static org.opensearch.wlm.WorkloadManagementIT.PUT;
import static org.opensearch.wlm.WorkloadManagementIT.TIMEOUT;
import static org.opensearch.wlm.WorkloadManagementIT.executeQueryGroupTask;

public class WorkloadManagementStatsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    final static String DEFAULT_QUERY_GROUP = DEFAULT_QUERY_GROUP_ID_SUPPLIER.get();
    final static String _ALL = "_all";
    final static String NAME1 = "name1";
    final static String NAME2 = "name2";
    final static String INVALID_ID = "invalid_id";

    public WorkloadManagementStatsIT(Settings nodeSettings) {
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
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestClusterUpdatePlugin.class);
        return plugins;
    }

    public void testDefaultQueryGroup() throws ExecutionException, InterruptedException {
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(response, new String[] { DEFAULT_QUERY_GROUP }, null);
    }

    public void testBasicWlmStats() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(response, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        updateQueryGroupInClusterState(DELETE, queryGroup);
        WlmStatsResponse updated_response = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(updated_response, new String[] { DEFAULT_QUERY_GROUP }, new String[] { id });
    }

    public void testWlmStatsWithQueryGroupId() throws Exception {
        QueryGroup enforcedQueryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String enforcedQueryGroupId = enforcedQueryGroup.get_id();
        updateQueryGroupInClusterState(PUT, enforcedQueryGroup);
        WlmStatsResponse enforcedCpuStats = getWlmStatsResponse(null, new String[] { enforcedQueryGroupId }, null);
        validateResponse(enforcedCpuStats, new String[] { enforcedQueryGroupId }, new String[] { DEFAULT_QUERY_GROUP });

        WlmStatsResponse defaultGroupStats = getWlmStatsResponse(null, new String[] { DEFAULT_QUERY_GROUP }, null);
        validateResponse(defaultGroupStats, new String[] { DEFAULT_QUERY_GROUP }, new String[] { enforcedQueryGroupId });

        QueryGroup monitoredQueryGroup = new QueryGroup(
            NAME2,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.2))
        );
        String monitoredQueryGroupId = monitoredQueryGroup.get_id();
        updateQueryGroupInClusterState(PUT, monitoredQueryGroup);
        WlmStatsResponse updatedStats = getWlmStatsResponse(null, new String[] { DEFAULT_QUERY_GROUP, monitoredQueryGroupId }, null);
        validateResponse(updatedStats, new String[] { DEFAULT_QUERY_GROUP, monitoredQueryGroupId }, new String[] { enforcedQueryGroupId });

        WlmStatsResponse invalidStatsResponse = getWlmStatsResponse(null, new String[] { INVALID_ID }, null);
        validateResponse(
            invalidStatsResponse,
            null,
            new String[] { DEFAULT_QUERY_GROUP, enforcedQueryGroupId, monitoredQueryGroupId, INVALID_ID }
        );

        updateQueryGroupInClusterState(DELETE, enforcedQueryGroup);
        updateQueryGroupInClusterState(DELETE, monitoredQueryGroup);
    }

    public void testWlmStatsWithBreach() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.0000001))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse breachedGroupsResponse = getWlmStatsResponse(null, new String[] { _ALL }, true);
        validateResponse(breachedGroupsResponse, null, new String[] { DEFAULT_QUERY_GROUP, id });

        WlmStatsResponse nonBreachedGroupsResponse = getWlmStatsResponse(null, new String[] { _ALL }, false);
        validateResponse(nonBreachedGroupsResponse, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        WlmStatsResponse allGroupsResponse = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(allGroupsResponse, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        executeQueryGroupTask(MEMORY, id);
        WlmStatsResponse updatedBreachedGroupsResponse = getWlmStatsResponse(null, new String[] { _ALL }, true);
        validateResponse(updatedBreachedGroupsResponse, new String[] { id }, new String[] { DEFAULT_QUERY_GROUP });

        updateQueryGroupInClusterState(DELETE, queryGroup);
    }

    public void testWlmStatsWithNodesId() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String queryGroupId = queryGroup.get_id();
        String nodeId = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes().getLocalNodeId();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse breachedGroupsResponse = getWlmStatsResponse(new String[] { nodeId }, new String[] { _ALL }, true);
        validateResponse(breachedGroupsResponse, new String[] { nodeId }, new String[] { DEFAULT_QUERY_GROUP, queryGroupId });

        WlmStatsResponse nonBreachedGroupsResponse = getWlmStatsResponse(new String[] { nodeId, INVALID_ID }, new String[] { _ALL }, false);
        validateResponse(
            nonBreachedGroupsResponse,
            new String[] { nodeId, DEFAULT_QUERY_GROUP, queryGroupId },
            new String[] { INVALID_ID }
        );

        WlmStatsResponse invalidGroupsResponse = getWlmStatsResponse(new String[] { INVALID_ID }, new String[] { _ALL }, false);
        validateResponse(invalidGroupsResponse, null, new String[] { nodeId, DEFAULT_QUERY_GROUP, queryGroupId });

        updateQueryGroupInClusterState(DELETE, queryGroup);
    }

    public void testWlmStatsWithIdAndBreach() throws Exception {
        QueryGroup enforcedQueryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String enforcedQueryGroupId = enforcedQueryGroup.get_id();
        String nodeId = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes().getLocalNodeId();
        updateQueryGroupInClusterState(PUT, enforcedQueryGroup);
        QueryGroup softQueryGroup = new QueryGroup(
            NAME2,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.SOFT, Map.of(ResourceType.CPU, 0.000000001))
        );
        updateQueryGroupInClusterState(PUT, softQueryGroup);

        WlmStatsResponse nodeStatsResponse = getWlmStatsResponse(new String[] { nodeId }, new String[] { DEFAULT_QUERY_GROUP }, true);
        validateResponse(nodeStatsResponse, new String[] { nodeId }, new String[] { DEFAULT_QUERY_GROUP, enforcedQueryGroupId });
        WlmStatsResponse breachedGroupsResponse = getWlmStatsResponse(
            null,
            new String[] { DEFAULT_QUERY_GROUP, enforcedQueryGroupId },
            true
        );
        validateResponse(breachedGroupsResponse, null, new String[] { DEFAULT_QUERY_GROUP, enforcedQueryGroupId });
        WlmStatsResponse nonBreachedGroupsResponse = getWlmStatsResponse(
            null,
            new String[] { DEFAULT_QUERY_GROUP, enforcedQueryGroupId },
            false
        );
        validateResponse(nonBreachedGroupsResponse, new String[] { DEFAULT_QUERY_GROUP, enforcedQueryGroupId }, null);

        updateQueryGroupInClusterState(DELETE, enforcedQueryGroup);
        updateQueryGroupInClusterState(DELETE, softQueryGroup);
    }

    public void testWlmStatsWithBreachForSoftQueryGroup() throws Exception {
        QueryGroup softQueryGroup = new QueryGroup(
            NAME2,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.SOFT, Map.of(ResourceType.CPU, 0.000000001))
        );
        String softQueryGroupId = softQueryGroup.get_id();
        updateQueryGroupInClusterState(PUT, softQueryGroup);
        executeQueryGroupTask(CPU, softQueryGroupId);
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { softQueryGroupId }, true);
        validateResponse(response, new String[] { softQueryGroupId }, new String[] { DEFAULT_QUERY_GROUP });
        updateQueryGroupInClusterState(DELETE, softQueryGroup);
    }

    public void updateQueryGroupInClusterState(String method, QueryGroup queryGroup) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(queryGroup, method), listener);
        assertTrue(listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertEquals(0, listener.getLatch().getCount());
    }

    public WlmStatsResponse getWlmStatsResponse(String[] nodesId, String[] queryGroupIds, Boolean breach) throws ExecutionException,
        InterruptedException {
        WlmStatsRequest wlmStatsRequest = new WlmStatsRequest(nodesId, new HashSet<>(Arrays.asList(queryGroupIds)), breach);
        return client().execute(WlmStatsAction.INSTANCE, wlmStatsRequest).get();
    }

    public void validateResponse(WlmStatsResponse response, String[] validIds, String[] invalidIds) {
        assertNotNull(response.toString());
        String res = response.toString();
        if (validIds != null) {
            for (String validId : validIds) {
                assertTrue(res.contains(validId));
            }
        }
        if (invalidIds != null) {
            for (String invalidId : invalidIds) {
                assertFalse(res.contains(invalidId));
            }
        }
    }
}
