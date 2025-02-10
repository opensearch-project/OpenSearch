/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.cluster.RemoteInfoRequest;
import org.opensearch.client.cluster.RemoteInfoResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.common.Booleans;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.Pipeline;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchModule;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

public abstract class OpenSearchRestHighLevelClientTestCase extends OpenSearchRestTestCase {

    protected static final String CONFLICT_PIPELINE_ID = "conflict_pipeline";
    protected static final double DOUBLE_DELTA = 0.000001;

    private static RestHighLevelClient restHighLevelClient;
    private static boolean async = Booleans.parseBoolean(System.getProperty("tests.rest.async", "false"));

    @Before
    public void initHighLevelClient() throws IOException {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        IOUtils.close(restHighLevelClient);
        restHighLevelClient = null;
    }

    protected static RestHighLevelClient highLevelClient() {
        return restHighLevelClient;
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    protected static <Req, Resp> Resp execute(Req request, SyncMethod<Req, Resp> syncMethod, AsyncMethod<Req, Resp> asyncMethod)
        throws IOException {
        return execute(request, syncMethod, asyncMethod, RequestOptions.DEFAULT);
    }

    /**
     * Executes the provided request using either the sync method or its async variant, both provided as functions
     */
    protected static <Req, Resp> Resp execute(
        Req request,
        SyncMethod<Req, Resp> syncMethod,
        AsyncMethod<Req, Resp> asyncMethod,
        RequestOptions options
    ) throws IOException {
        if (async == false) {
            return syncMethod.execute(request, options);
        } else {
            PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
            asyncMethod.execute(request, options, future);
            return future.actionGet();
        }
    }

    /**
     * Executes the provided request using either the sync method or its async
     * variant, both provided as functions. This variant is used when the call does
     * not have a request object (only headers and the request path).
     */
    protected static <Resp> Resp execute(
        SyncMethodNoRequest<Resp> syncMethodNoRequest,
        AsyncMethodNoRequest<Resp> asyncMethodNoRequest,
        RequestOptions requestOptions
    ) throws IOException {
        if (async == false) {
            return syncMethodNoRequest.execute(requestOptions);
        } else {
            PlainActionFuture<Resp> future = PlainActionFuture.newFuture();
            asyncMethodNoRequest.execute(requestOptions, future);
            return future.actionGet();
        }
    }

    @FunctionalInterface
    protected interface SyncMethod<Request, Response> {
        Response execute(Request request, RequestOptions options) throws IOException;
    }

    @FunctionalInterface
    protected interface SyncMethodNoRequest<Response> {
        Response execute(RequestOptions options) throws IOException;
    }

    @FunctionalInterface
    protected interface AsyncMethod<Request, Response> {
        void execute(Request request, RequestOptions options, ActionListener<Response> listener);
    }

    @FunctionalInterface
    protected interface AsyncMethodNoRequest<Response> {
        void execute(RequestOptions options, ActionListener<Response> listener);
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        }
    }

    protected static XContentBuilder buildRandomXContentPipeline(XContentBuilder pipelineBuilder) throws IOException {
        pipelineBuilder.startObject();
        {
            pipelineBuilder.field(Pipeline.DESCRIPTION_KEY, "some random set of processors");
            pipelineBuilder.startArray(Pipeline.PROCESSORS_KEY);
            {
                pipelineBuilder.startObject().startObject("set");
                {
                    pipelineBuilder.field("field", "foo").field("value", "bar");
                }
                pipelineBuilder.endObject().endObject();
                pipelineBuilder.startObject().startObject("convert");
                {
                    pipelineBuilder.field("field", "rank").field("type", "integer");
                }
                pipelineBuilder.endObject().endObject();
            }
            pipelineBuilder.endArray();
        }
        pipelineBuilder.endObject();
        return pipelineBuilder;
    }

    protected static XContentBuilder buildRandomXContentPipeline() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder pipelineBuilder = XContentBuilder.builder(xContentType.xContent());
        return buildRandomXContentPipeline(pipelineBuilder);
    }

    protected static void createFieldAddingPipleine(String id, String fieldName, String value) throws IOException {
        XContentBuilder pipeline = jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject("set")
            .field("field", fieldName)
            .field("value", value)
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        createPipeline(new PutPipelineRequest(id, BytesReference.bytes(pipeline), MediaTypeRegistry.JSON));
    }

    protected static void createPipeline(String pipelineId) throws IOException {
        XContentBuilder builder = buildRandomXContentPipeline();
        createPipeline(new PutPipelineRequest(pipelineId, BytesReference.bytes(builder), builder.contentType()));
    }

    protected static void createPipeline(PutPipelineRequest putPipelineRequest) throws IOException {
        assertTrue(
            execute(putPipelineRequest, highLevelClient().ingest()::putPipeline, highLevelClient().ingest()::putPipelineAsync)
                .isAcknowledged()
        );
    }

    protected static void clusterUpdateSettings(Settings persistentSettings, Settings transientSettings) throws IOException {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(persistentSettings);
        request.transientSettings(transientSettings);
        assertTrue(
            execute(request, highLevelClient().cluster()::putSettings, highLevelClient().cluster()::putSettingsAsync).isAcknowledged()
        );
    }

    protected void putConflictPipeline() throws IOException {
        final XContentBuilder pipelineBuilder = jsonBuilder().startObject()
            .startArray("processors")
            .startObject()
            .startObject("set")
            .field("field", "_version")
            .field("value", 1)
            .endObject()
            .endObject()
            .startObject()
            .startObject("set")
            .field("field", "_id")
            .field("value", "1")
            .endObject()
            .endObject()
            .endArray()
            .endObject();
        final PutPipelineRequest putPipelineRequest = new PutPipelineRequest(
            CONFLICT_PIPELINE_ID,
            BytesReference.bytes(pipelineBuilder),
            pipelineBuilder.contentType()
        );
        assertTrue(highLevelClient().ingest().putPipeline(putPipelineRequest, RequestOptions.DEFAULT).isAcknowledged());
    }

    @Override
    protected Settings restClientSettings() {
        final String user = Objects.requireNonNull(System.getProperty("tests.rest.cluster.username"));
        final String pass = Objects.requireNonNull(System.getProperty("tests.rest.cluster.password"));
        final String token = "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected Iterable<SearchHit> searchAll(String... indices) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indices);
        return searchAll(searchRequest);
    }

    protected Iterable<SearchHit> searchAll(SearchRequest searchRequest) throws IOException {
        refreshIndexes(searchRequest.indices());
        SearchResponse search = highLevelClient().search(searchRequest, RequestOptions.DEFAULT);
        return search.getHits();
    }

    protected void refreshIndexes(String... indices) throws IOException {
        String joinedIndices = Arrays.stream(indices).collect(Collectors.joining(","));
        Response refreshResponse = client().performRequest(new Request("POST", "/" + joinedIndices + "/_refresh"));
        assertEquals(200, refreshResponse.getStatusLine().getStatusCode());
    }

    protected void createIndexWithMultipleShards(String index) throws IOException {
        CreateIndexRequest indexRequest = new CreateIndexRequest(index);
        int shards = randomIntBetween(8, 10);
        indexRequest.settings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0));
        highLevelClient().indices().create(indexRequest, RequestOptions.DEFAULT);
    }

    protected static void setupRemoteClusterConfig(String remoteClusterName) throws Exception {
        // Configure local cluster as remote cluster:
        // TODO: replace with nodes info highlevel rest client code when it is available:
        final Request request = new Request("GET", "/_nodes");
        Map<?, ?> nodesResponse = (Map<?, ?>) toMap(client().performRequest(request)).get("nodes");
        // Select node info of first node (we don't know the node id):
        nodesResponse = (Map<?, ?>) nodesResponse.get(nodesResponse.keySet().iterator().next());
        String transportAddress = (String) nodesResponse.get("transport_address");

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(singletonMap("cluster.remote." + remoteClusterName + ".seeds", transportAddress));
        ClusterUpdateSettingsResponse updateSettingsResponse = restHighLevelClient.cluster()
            .putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
        assertThat(updateSettingsResponse.isAcknowledged(), is(true));

        assertBusy(() -> {
            RemoteInfoResponse response = highLevelClient().cluster().remoteInfo(new RemoteInfoRequest(), RequestOptions.DEFAULT);
            assertThat(response, notNullValue());
            assertThat(response.getInfos().size(), greaterThan(0));
        });
    }

    protected static Map<String, Object> toMap(Response response) throws IOException, OpenSearchParseException, ParseException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

    protected static TaskId findTaskToRethrottle(String actionName, String description) throws IOException {
        long start = System.nanoTime();
        ListTasksRequest request = new ListTasksRequest();
        request.setActions(actionName);
        request.setDetailed(true);
        do {
            ListTasksResponse list = highLevelClient().tasks().list(request, RequestOptions.DEFAULT);
            list.rethrowFailures("Finding tasks to rethrottle");
            List<TaskGroup> taskGroups = list.getTaskGroups()
                .stream()
                .filter(taskGroup -> taskGroup.getTaskInfo().getDescription().equals(description))
                .collect(Collectors.toList());
            assertThat("tasks are left over from the last execution of this test", taskGroups, hasSize(lessThan(2)));
            if (0 == taskGroups.size()) {
                // The parent task hasn't started yet
                continue;
            }
            TaskGroup taskGroup = taskGroups.get(0);
            assertThat(taskGroup.getChildTasks(), empty());
            return taskGroup.getTaskInfo().getTaskId();
        } while (System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
        throw new AssertionError(
            "Couldn't find tasks to rethrottle. Here are the running tasks "
                + highLevelClient().tasks().list(request, RequestOptions.DEFAULT)
        );
    }

    protected static CheckedRunnable<Exception> checkTaskCompletionStatus(RestClient client, String taskId) {
        return () -> {
            Response response = client.performRequest(new Request("GET", "/_tasks/" + taskId));
            assertTrue((boolean) entityAsMap(response).get("completed"));
        };
    }
}
