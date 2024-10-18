/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.backpressure.SearchBackpressureIT.ExceptionCatchingListener;
import org.opensearch.search.backpressure.SearchBackpressureIT.TestResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0)
public class WorkloadManagementStatsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    final static String PUT = "PUT";
    final static String DELETE = "DELETE";
    final static String DEFAULT_QUERY_GROUP = "DEFAULT_QUERY_GROUP";
    final static String _ALL = "_all";
    private static final TimeValue TIMEOUT = new TimeValue(10, TimeUnit.SECONDS);

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
        HashSet<String> set = new HashSet<>();
        set.add(_ALL);
        WlmStatsRequest wlmStatsRequest = new WlmStatsRequest(null, set, null);
        WlmStatsResponse response = client().execute(WlmStatsAction.INSTANCE, wlmStatsRequest).get();
        validateResponse(response, new String[] { DEFAULT_QUERY_GROUP }, null);
    }

    public void testBasicWlmStats() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            "name",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(new String[] { _ALL }, null);
        validateResponse(response, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        updateQueryGroupInClusterState(DELETE, queryGroup);
        WlmStatsResponse response2 = getWlmStatsResponse(new String[] { _ALL }, null);
        validateResponse(response2, new String[] { DEFAULT_QUERY_GROUP }, new String[] { id });
    }

    public void testWlmStatsWithId() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            "name",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(new String[] { id }, null);
        validateResponse(response, new String[] { id }, new String[] { DEFAULT_QUERY_GROUP });

        WlmStatsResponse response2 = getWlmStatsResponse(new String[] { DEFAULT_QUERY_GROUP }, null);
        validateResponse(response2, new String[] { DEFAULT_QUERY_GROUP }, new String[] { id });

        QueryGroup queryGroup2 = new QueryGroup(
            "name2",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.2))
        );
        String id2 = queryGroup2.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup2);
        WlmStatsResponse response3 = getWlmStatsResponse(new String[] { DEFAULT_QUERY_GROUP, id2 }, null);
        validateResponse(response3, new String[] { DEFAULT_QUERY_GROUP, id2 }, new String[] { id });

        WlmStatsResponse response4 = getWlmStatsResponse(new String[] { "erfgrdgbhn" }, null);
        validateResponse(response4, null, new String[] { DEFAULT_QUERY_GROUP, id, id2, "erfgrdgbhn" });

        updateQueryGroupInClusterState(DELETE, queryGroup);
        updateQueryGroupInClusterState(DELETE, queryGroup2);
    }

    public void testWlmStatsWithBreach() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            "name",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(new String[] { _ALL }, true);
        validateResponse(response, null, new String[] { DEFAULT_QUERY_GROUP, id });

        WlmStatsResponse response2 = getWlmStatsResponse(new String[] { _ALL }, false);
        validateResponse(response2, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        WlmStatsResponse response3 = getWlmStatsResponse(new String[] { _ALL }, null);
        validateResponse(response3, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        updateQueryGroupInClusterState(DELETE, queryGroup);
    }

    public void testWlmStatsWithIdAndBreach() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            "name",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(new String[] { DEFAULT_QUERY_GROUP }, true);
        validateResponse(response, null, new String[] { DEFAULT_QUERY_GROUP, id });

        WlmStatsResponse response2 = getWlmStatsResponse(new String[] { DEFAULT_QUERY_GROUP, id }, true);
        validateResponse(response2, null, new String[] { DEFAULT_QUERY_GROUP, id });

        WlmStatsResponse response3 = getWlmStatsResponse(new String[] { DEFAULT_QUERY_GROUP, id }, false);
        validateResponse(response3, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        WlmStatsResponse response4 = getWlmStatsResponse(new String[] { id }, false);
        validateResponse(response4, new String[] { id }, new String[] { DEFAULT_QUERY_GROUP });

        updateQueryGroupInClusterState(DELETE, queryGroup);
    }

    public void updateQueryGroupInClusterState(String method, QueryGroup queryGroup) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(queryGroup, method), listener);
        assertTrue(listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertEquals(0, listener.getLatch().getCount());
    }

    public WlmStatsResponse getWlmStatsResponse(String[] queryGroupIds, Boolean breach) throws ExecutionException, InterruptedException {
        WlmStatsRequest wlmStatsRequest = new WlmStatsRequest(null, new HashSet<>(Arrays.asList(queryGroupIds)), breach);
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

    public static class TestClusterUpdateRequest extends ActionRequest {
        final private String method;
        final private QueryGroup queryGroup;

        public TestClusterUpdateRequest(QueryGroup queryGroup, String method) {
            this.method = method;
            this.queryGroup = queryGroup;
        }

        public TestClusterUpdateRequest(StreamInput in) throws IOException {
            super(in);
            this.method = in.readString();
            this.queryGroup = new QueryGroup(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(method);
            queryGroup.writeTo(out);
        }

        public QueryGroup getQueryGroup() {
            return queryGroup;
        }

        public String getMethod() {
            return method;
        }
    }

    public static class TestClusterUpdateTransportAction extends HandledTransportAction<TestClusterUpdateRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_cluster_update_action", TestResponse::new);
        private final ClusterService clusterService;

        @Inject
        public TestClusterUpdateTransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ActionFilters actionFilters
        ) {
            super(ACTION.name(), transportService, actionFilters, TestClusterUpdateRequest::new);
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, TestClusterUpdateRequest request, ActionListener<TestResponse> listener) {
            clusterService.submitStateUpdateTask("query-group-persistence-service", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Map<String, QueryGroup> currentGroups = currentState.metadata().queryGroups();
                    QueryGroup queryGroup = request.getQueryGroup();
                    String id = queryGroup.get_id();
                    String method = request.getMethod();
                    Metadata metadata;
                    if (method.equals(PUT)) { // create
                        metadata = Metadata.builder(currentState.metadata()).put(queryGroup).build();
                    } else { // delete
                        metadata = Metadata.builder(currentState.metadata()).remove(currentGroups.get(id)).build();
                    }
                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new TestResponse());
                }
            });
        }
    }

    public static class TestClusterUpdatePlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(new ActionHandler<>(TestClusterUpdateTransportAction.ACTION, TestClusterUpdateTransportAction.class));
        }

        @Override
        public List<ActionType<? extends ActionResponse>> getClientActions() {
            return Arrays.asList(TestClusterUpdateTransportAction.ACTION);
        }
    }
}
