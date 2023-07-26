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

package org.opensearch.http;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.logging.log4j.LogManager;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Cancellable;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseListener;
import org.opensearch.common.SetOnce;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.lookup.LeafFieldsLookup;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.TransportService;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.awaitLatch;

public class SearchRestCancellationIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(ScriptedBlockPlugin.class);
        plugins.addAll(super.nodePlugins());
        return plugins;
    }

    public void testAutomaticCancellationDuringQueryPhase() throws Exception {
        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().query(scriptQuery(
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())));
        searchRequest.setJsonEntity(Strings.toString(XContentType.JSON, searchSource));
        verifyCancellationDuringQueryPhase(SearchAction.NAME, searchRequest);
    }

    public void testAutomaticCancellationMultiSearchDuringQueryPhase() throws Exception {
        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(new SearchRequest("test")
            .source(new SearchSourceBuilder().scriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))));
        Request restRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        restRequest.setEntity(new ByteArrayEntity(requestBody, createContentType(contentType)));
        verifyCancellationDuringQueryPhase(MultiSearchAction.NAME, restRequest);
    }

    void verifyCancellationDuringQueryPhase(String searchAction, Request searchRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        Cancellable cancellable = getRestClient().performRequestAsync(searchRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        awaitForBlock(plugins);
        cancellable.cancel();
        ensureSearchTaskIsCancelled(searchAction, nodeIdToName::get);

        disableBlocks(plugins);
        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    public void testAutomaticCancellationDuringFetchPhase() throws Exception {
        Request searchRequest = new Request("GET", "/test/_search");
        SearchSourceBuilder searchSource = new SearchSourceBuilder().scriptField("test_field",
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()));
        searchRequest.setJsonEntity(Strings.toString(XContentType.JSON, searchSource));
        verifyCancellationDuringFetchPhase(SearchAction.NAME, searchRequest);
    }

    public void testAutomaticCancellationMultiSearchDuringFetchPhase() throws Exception {
        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(new SearchRequest("test")
            .source(new SearchSourceBuilder().scriptField("test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))));
        Request restRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        restRequest.setEntity(new ByteArrayEntity(requestBody, createContentType(contentType)));
        verifyCancellationDuringFetchPhase(MultiSearchAction.NAME, restRequest);
    }

    void verifyCancellationDuringFetchPhase(String searchAction, Request searchRequest) throws Exception {
        Map<String, String> nodeIdToName = readNodesInfo();

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        Cancellable cancellable = getRestClient().performRequestAsync(searchRequest, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception exception) {
                error.set(exception);
                latch.countDown();
            }
        });

        latch.await(2, TimeUnit.SECONDS);
        awaitForBlock(plugins);
        cancellable.cancel();
        ensureSearchTaskIsCancelled(searchAction, nodeIdToName::get);

        disableBlocks(plugins);
        latch.await();
        assertThat(error.get(), instanceOf(CancellationException.class));
    }

    private static Map<String, String> readNodesInfo() {
        Map<String, String> nodeIdToName = new HashMap<>();
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            nodeIdToName.put(node.getNode().getId(), node.getNode().getName());
        }
        return nodeIdToName;
    }

    private static void ensureSearchTaskIsCancelled(String transportAction, Function<String, String> nodeIdToName) throws Exception {
        SetOnce<TaskInfo> searchTask = new SetOnce<>();
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().get();
        for (TaskInfo task : listTasksResponse.getTasks()) {
            if (task.getAction().equals(transportAction)) {
                searchTask.set(task);
            }
        }
        assertNotNull(searchTask.get());
        TaskId taskId = searchTask.get().getTaskId();
        String nodeName = nodeIdToName.apply(taskId.getNodeId());
        assertBusy(() -> {
            TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
            Task task = taskManager.getTask(taskId.getId());
            assertThat(task, instanceOf(CancellableTask.class));
            assertTrue(((CancellableTask)task).isCancelled());
        });
    }

    private static void indexTestData() {
        for (int i = 0; i < 5; i++) {
            // Make sure we have a few segments
            BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < 20; j++) {
                bulkRequestBuilder.add(client().prepareIndex("test").setId(Integer.toString(i * 5 + j)).setSource("field", "value"));
            }
            assertNoFailures(bulkRequestBuilder.get());
        }
    }

    private static List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void awaitForBlock(List<ScriptedBlockPlugin> plugins) throws Exception {
        int numberOfShards = getNumShards("test").numPrimaries;
        assertBusy(() -> {
            int numberOfBlockedPlugins = 0;
            for (ScriptedBlockPlugin plugin : plugins) {
                numberOfBlockedPlugins += plugin.hits.get();
            }
            logger.info("The plugin blocked on {} out of {} shards", numberOfBlockedPlugins, numberOfShards);
            assertThat(numberOfBlockedPlugins, greaterThan(0));
        }, 10, TimeUnit.SECONDS);
    }

    private static void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    public static class ScriptedBlockPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_block";

        private final AtomicInteger hits = new AtomicInteger();

        private final AtomicBoolean shouldBlock = new AtomicBoolean(true);

        void reset() {
            hits.set(0);
        }

        void disableBlock() {
            shouldBlock.set(false);
        }

        void enableBlock() {
            shouldBlock.set(true);
        }

        @Override
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                LeafFieldsLookup fieldsLookup = (LeafFieldsLookup) params.get("_fields");
                LogManager.getLogger(SearchRestCancellationIT.class).info("Blocking on the document {}", fieldsLookup.get("_id"));
                hits.incrementAndGet();
                try {
                    waitUntil(() -> shouldBlock.get() == false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
    }

    private static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }
}
