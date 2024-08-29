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

package org.opensearch.ingest;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.OpenSearchParseException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.TransportBulkAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.cbor.CborXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestServiceTests extends OpenSearchTestCase {

    private static final IngestPlugin DUMMY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Collections.singletonMap("foo", (factories, tag, description, config) -> null);
        }
    };

    private ThreadPool threadPool;
    private BulkRequest mockBulkRequest;

    @Before
    public void setup() {
        threadPool = mock(ThreadPool.class);
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        mockBulkRequest = mock(BulkRequest.class);
        lenient().when(mockBulkRequest.batchSize()).thenReturn(1);
    }

    public void testIngestPlugin() {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        IngestService ingestService = new IngestService(
            clusterService,
            threadPool,
            null,
            null,
            null,
            Collections.singletonList(DUMMY_PLUGIN),
            client,
            mock(IndicesService.class)
        );
        Map<String, Processor.Factory> factories = ingestService.getProcessorFactories();
        assertTrue(factories.containsKey("foo"));
        assertEquals(1, factories.size());
    }

    public void testIngestPluginDuplicate() {
        Client client = mock(Client.class);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestService(
                mock(ClusterService.class),
                threadPool,
                null,
                null,
                null,
                Arrays.asList(DUMMY_PLUGIN, DUMMY_PLUGIN),
                client,
                mock(IndicesService.class)
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }

    public void testExecuteIndexPipelineDoesNotExist() {
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        IngestService ingestService = new IngestService(
            clusterService,
            threadPool,
            null,
            null,
            null,
            Collections.singletonList(DUMMY_PLUGIN),
            client,
            mock(IndicesService.class)
        );
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");

        final SetOnce<Boolean> failure = new SetOnce<>();
        final BiConsumer<Integer, Exception> failureHandler = (slot, e) -> {
            failure.set(true);
            assertThat(slot, equalTo(0));
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        };

        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);

        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            new BulkRequest()
        );

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline = new PipelineConfiguration(
            "_id",
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"),
            MediaTypeRegistry.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(1));
        assertThat(ingestService.pipelines().get("_id").pipeline.getId(), equalTo("_id"));
        assertThat(ingestService.pipelines().get("_id").pipeline.getDescription(), nullValue());
        assertThat(ingestService.pipelines().get("_id").pipeline.getProcessors().size(), equalTo(1));
        assertThat(ingestService.pipelines().get("_id").pipeline.getProcessors().get(0).getType(), equalTo("set"));
    }

    public void testInnerUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline1 = new PipelineConfiguration("_id1", new BytesArray("{\"processors\": []}"), MediaTypeRegistry.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(mapOf("_id1", pipeline1));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(1));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));

        PipelineConfiguration pipeline2 = new PipelineConfiguration("_id2", new BytesArray("{\"processors\": []}"), MediaTypeRegistry.JSON);
        ingestMetadata = new IngestMetadata(mapOf("_id1", pipeline1, "_id2", pipeline2));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getId(), equalTo("_id2"));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getProcessors().size(), equalTo(0));

        PipelineConfiguration pipeline3 = new PipelineConfiguration("_id3", new BytesArray("{\"processors\": []}"), MediaTypeRegistry.JSON);
        ingestMetadata = new IngestMetadata(mapOf("_id1", pipeline1, "_id2", pipeline2, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(3));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getId(), equalTo("_id2"));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getId(), equalTo("_id3"));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().size(), equalTo(0));

        ingestMetadata = new IngestMetadata(mapOf("_id1", pipeline1, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getId(), equalTo("_id3"));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().size(), equalTo(0));

        pipeline3 = new PipelineConfiguration(
            "_id3",
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"),
            MediaTypeRegistry.JSON
        );
        ingestMetadata = new IngestMetadata(mapOf("_id1", pipeline1, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getId(), equalTo("_id3"));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().size(), equalTo(1));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().get(0).getType(), equalTo("set"));

        // Perform an update with no changes:
        Map<String, IngestService.PipelineHolder> pipelines = ingestService.pipelines();
        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines(), sameInstance(pipelines));
    }

    private static <K, V> Map<K, V> mapOf(K key, V value) {
        return Collections.singletonMap(key, value);
    }

    private static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    public void testDelete() {
        IngestService ingestService = createWithProcessors();
        PipelineConfiguration config = new PipelineConfiguration(
            "_id",
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"),
            MediaTypeRegistry.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        // Delete pipeline:
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), nullValue());

        // Delete existing pipeline:
        try {
            IngestService.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [_id] is missing"));
        }
    }

    public void testValidateNoIngestInfo() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"),
            MediaTypeRegistry.JSON
        );
        Exception e = expectThrows(IllegalStateException.class, () -> ingestService.validatePipeline(emptyMap(), putRequest));
        assertEquals("Ingest info is empty", e.getMessage());

        DiscoveryNode discoveryNode = new DiscoveryNode(
            "_node_id",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        IngestInfo ingestInfo = new IngestInfo(Collections.singletonList(new ProcessorInfo("set")));
        ingestService.validatePipeline(Collections.singletonMap(discoveryNode, ingestInfo), putRequest);
    }

    public void testGetProcessorsInPipeline() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}},"
                    + "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"
            ),
            MediaTypeRegistry.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());

        assertThat(ingestService.getProcessorsInPipeline(id, Processor.class).size(), equalTo(3));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessorImpl.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessor.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, FakeProcessor.class).size(), equalTo(2));

        assertThat(ingestService.getProcessorsInPipeline(id, ConditionalProcessor.class).size(), equalTo(0));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ingestService.getProcessorsInPipeline("fakeID", Processor.class)
        );
        assertThat("pipeline with id [fakeID] does not exist", equalTo(e.getMessage()));
    }

    public void testGetProcessorsInPipelineComplexConditional() throws Exception {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        String scriptName = "conditionalScript";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Collections.singletonMap(scriptName, ctx -> {
                    ctx.get("_type");
                    return true;
                }), Collections.emptyMap())
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );

        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("complexSet", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");

            return new ConditionalProcessor(
                randomAlphaOfLength(10),
                null,
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
                scriptService,
                new ConditionalProcessor(
                    randomAlphaOfLength(10) + "-nested",
                    null,
                    new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Collections.emptyMap()),
                    scriptService,
                    new FakeProcessor("complexSet", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value))
                )
            );
        });

        IngestService ingestService = createWithProcessors(processors);
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"processors\": [{\"complexSet\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());

        assertThat(ingestService.getProcessorsInPipeline(id, Processor.class).size(), equalTo(3));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessor.class).size(), equalTo(2));
        assertThat(ingestService.getProcessorsInPipeline(id, FakeProcessor.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, ConditionalProcessor.class).size(), equalTo(2));

        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessorImpl.class).size(), equalTo(0));
    }

    public void testCrud() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest(id);
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
    }

    public void testPut() {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        // add a new pipeline:
        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("{\"processors\": []}"), MediaTypeRegistry.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"processors\": [], \"description\": \"_description\"}"),
            MediaTypeRegistry.JSON
        );
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(0));
    }

    public void testPutWithErrorResponse() throws IllegalAccessException {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        PutPipelineRequest putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"description\": \"empty processors\"}"),
            MediaTypeRegistry.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(IngestService.class))) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "test1",
                    IngestService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to update ingest pipelines"
                )
            );
            ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
            mockAppender.assertAllExpectationsMatched();
        }
        pipeline = ingestService.getPipeline(id);
        assertNotNull(pipeline);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(
            pipeline.getDescription(),
            equalTo("this is a place holder pipeline, because pipeline with" + " id [_id] could not be loaded")
        );
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertNull(pipeline.getProcessors().get(0).getTag());
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("unknown"));
    }

    public void testDeleteUsingWildcard() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}");
        pipelines.put("p1", new PipelineConfiguration("p1", definition, MediaTypeRegistry.JSON));
        pipelines.put("p2", new PipelineConfiguration("p2", definition, MediaTypeRegistry.JSON));
        pipelines.put("q1", new PipelineConfiguration("q1", definition, MediaTypeRegistry.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());
        assertThat(ingestService.getPipeline("p2"), notNullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Delete pipeline matching wildcard
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("p*");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Exception if we used name which does not exist
        try {
            IngestService.innerDelete(new DeletePipelineRequest("unknown"), clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [unknown] is missing"));
        }

        // match all wildcard works on last remaining pipeline
        DeletePipelineRequest matchAllDeleteRequest = new DeletePipelineRequest("*");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(matchAllDeleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), nullValue());

        // match all wildcard does not throw exception if none match
        IngestService.innerDelete(matchAllDeleteRequest, clusterState);
    }

    public void testDeleteWithExistingUnmatchedPipelines() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}");
        pipelines.put("p1", new PipelineConfiguration("p1", definition, MediaTypeRegistry.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("z*");
        try {
            IngestService.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [z*] is missing"));
        }
    }

    public void testGetPipelines() {
        Map<String, PipelineConfiguration> configs = new HashMap<>();
        configs.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{\"processors\": []}"), MediaTypeRegistry.JSON));
        configs.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{\"processors\": []}"), MediaTypeRegistry.JSON));

        assertThat(IngestService.innerGetPipelines(null, "_id1").isEmpty(), is(true));

        IngestMetadata ingestMetadata = new IngestMetadata(configs);
        List<PipelineConfiguration> pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id1");
        assertThat(pipelines.size(), equalTo(1));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id1", "_id2");
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id*");
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        // get all variants: (no IDs or '*')
        pipelines = IngestService.innerGetPipelines(ingestMetadata);
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "*");
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));
    }

    public void testValidate() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}},"
                    + "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"
            ),
            MediaTypeRegistry.JSON
        );

        DiscoveryNode node1 = new DiscoveryNode("_node_id1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("_node_id2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"))));

        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> ingestService.validatePipeline(ingestInfos, putRequest)
        );
        assertEquals("Processor type [remove] is not installed on node [" + node2 + "]", e.getMessage());
        assertEquals("remove", e.getMetadata("opensearch.processor_type").get(0));
        assertEquals("tag2", e.getMetadata("opensearch.processor_tag").get(0));

        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestService.validatePipeline(ingestInfos, putRequest);
    }

    public void testValidateProcessorCountForIngestPipelineThrowsException(){
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray(
                "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}},"
                    + "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"
            ),
            MediaTypeRegistry.JSON
        );

        DiscoveryNode node1 = new DiscoveryNode("_node_id1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("_node_id2", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"))));

        Settings newSettings = Settings.builder().put("cluster.ingest.max_number_processors", 1).build();
        ingestService.getClusterService().getClusterSettings().applySettings(newSettings);

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ingestService.validatePipeline(ingestInfos, putRequest)
        );
    }

    public void testExecuteIndexPipelineExistsButFailedParsing() {
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> new AbstractProcessor("mock", "description") {
                @Override
                public IngestDocument execute(IngestDocument ingestDocument) {
                    throw new IllegalStateException("error");
                }

                @Override
                public String getType() {
                    return null;
                }
            })
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        String id = "_id";
        PutPipelineRequest putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final SetOnce<Boolean> failure = new SetOnce<>();
        BulkRequest bulkRequest = new BulkRequest();
        final IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1")
            .source(emptyMap())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline(id).setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);

        final BiConsumer<Integer, Exception> failureHandler = (slot, e) -> {
            assertThat(e.getCause(), instanceOf(IllegalStateException.class));
            assertThat(e.getCause().getMessage(), equalTo("error"));
            failure.set(true);
            assertThat(slot, equalTo(1));
        };

        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);

        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteBulkPipelineDoesNotExist() {
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor())
        );

        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1")
            .source(emptyMap())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3")
            .source(emptyMap())
            .setPipeline("does_not_exist")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest3);
        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, times(1)).accept(
            argThat((Integer item) -> item == 2),
            argThat((IllegalArgumentException iae) -> "pipeline with id [does_not_exist] does not exist".equals(iae.getMessage()))
        );
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteSuccess() {
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor())
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteEmptyPipeline() throws Exception {
        IngestService ingestService = createWithProcessors(emptyMap());
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [], \"description\": \"_description\"}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecutePropagateAllMetadataUpdates() throws Exception {
        final CompoundProcessor processor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> processor)
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final long newVersion = randomLong();
        final String versionType = randomFrom("internal", "external", "external_gt", "external_gte");
        final long ifSeqNo = randomNonNegativeLong();
        final long ifPrimaryTerm = randomNonNegativeLong();
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.Metadata metadata : IngestDocument.Metadata.values()) {
                if (metadata == IngestDocument.Metadata.VERSION) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), newVersion);
                } else if (metadata == IngestDocument.Metadata.VERSION_TYPE) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), versionType);
                } else if (metadata == IngestDocument.Metadata.IF_SEQ_NO) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), ifSeqNo);
                } else if (metadata == IngestDocument.Metadata.IF_PRIMARY_TERM) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), ifPrimaryTerm);
                } else {
                    ingestDocument.setFieldValue(metadata.getFieldName(), "update" + metadata.getFieldName());
                }
            }

            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer<IngestDocument, Exception>) invocationOnMock.getArguments()[1];
            handler.accept(ingestDocument, null);
            return null;
        }).when(processor).execute(any(), any());
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        verify(processor).execute(any(), any());
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.version(), equalTo(newVersion));
        assertThat(indexRequest.versionType(), equalTo(VersionType.fromString(versionType)));
        assertThat(indexRequest.ifSeqNo(), equalTo(ifSeqNo));
        assertThat(indexRequest.ifPrimaryTerm(), equalTo(ifPrimaryTerm));
    }

    public void testExecuteFailure() throws Exception {
        final CompoundProcessor processor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> processor)
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        doThrow(new RuntimeException()).when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()), any());
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()), any());
        verify(failureHandler, times(1)).accept(eq(0), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteSuccessWithOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock_processor_type");
        when(processor.getTag()).thenReturn("mock_processor_tag");
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(null, new RuntimeException());
            return null;
        }).when(processor).execute(eqIndexTypeId(emptyMap()), any());

        final Processor onFailureProcessor = mock(Processor.class);
        doAnswer(args -> {
            IngestDocument ingestDocument = (IngestDocument) args.getArguments()[0];
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(ingestDocument, null);
            return null;
        }).when(onFailureProcessor).execute(eqIndexTypeId(emptyMap()), any());

        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Collections.singletonList(processor),
            Collections.singletonList(new CompoundProcessor(onFailureProcessor))
        );
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> compoundProcessor)
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        verify(failureHandler, never()).accept(eq(0), any(IngestProcessorException.class));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteFailureWithNestedOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor onFailureProcessor = mock(Processor.class);
        final Processor onFailureOnFailureProcessor = mock(Processor.class);
        final List<Processor> processors = Collections.singletonList(onFailureProcessor);
        final List<Processor> onFailureProcessors = Collections.singletonList(onFailureOnFailureProcessor);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Collections.singletonList(processor),
            Collections.singletonList(new CompoundProcessor(false, processors, onFailureProcessors))
        );
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> compoundProcessor)
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        doThrow(new RuntimeException()).when(onFailureOnFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()), any());
        doThrow(new RuntimeException()).when(onFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()), any());
        doThrow(new RuntimeException()).when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()), any());
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()), any());
        verify(failureHandler, times(1)).accept(eq(0), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testBulkRequestExecutionWithFailures() {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numIndexRequests = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < numIndexRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("_index").id("_id").setPipeline(pipelineId).setFinalPipeline("_none");
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
            bulkRequest.add(indexRequest);
        }
        int numOtherRequests = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < numOtherRequests; i++) {
            if (randomBoolean()) {
                bulkRequest.add(new DeleteRequest("_index", "_id"));
            } else {
                bulkRequest.add(new UpdateRequest("_index", "_id"));
            }
        }

        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        Exception error = new RuntimeException();
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            List<IngestDocumentWrapper> ingestDocumentWrappers = (List) args.getArguments()[0];
            Consumer<List<IngestDocumentWrapper>> handler = (Consumer) args.getArguments()[1];
            for (IngestDocumentWrapper wrapper : ingestDocumentWrappers) {
                wrapper.update(wrapper.getIngestDocument(), error);
            }
            handler.accept(ingestDocumentWrappers);
            return null;
        }).when(processor).batchExecute(any(), any());
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> processor)
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        final Map<Integer, Exception> errorHandler = new HashMap<>();
        final Map<Thread, Exception> completionHandler = new HashMap<>();
        ingestService.executeBulkRequest(
            numIndexRequests + numOtherRequests,
            bulkRequest.requests(),
            errorHandler::put,
            completionHandler::put,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );

        MatcherAssert.assertThat(errorHandler.entrySet(), hasSize(numIndexRequests));
        errorHandler.values().forEach(e -> assertEquals(e.getCause(), error));

        MatcherAssert.assertThat(completionHandler.keySet(), contains(Thread.currentThread()));
    }

    public void testBulkRequestExecution() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        // Test to make sure that ingest respects content types other than the default index content type
        XContentType xContentType = randomFrom(
            Arrays.stream(XContentType.values()).filter(t -> Requests.INDEX_CONTENT_TYPE.equals(t) == false).collect(Collectors.toList())
        );

        logger.info("Using [{}], not randomly determined default [{}]", xContentType, Requests.INDEX_CONTENT_TYPE);
        int numRequest = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index").id("_id").setPipeline(pipelineId).setFinalPipeline("_none");
            indexRequest.source(xContentType, "field1", "value1");
            bulkRequest.add(indexRequest);
        }

        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            List<IngestDocumentWrapper> ingestDocumentWrappers = (List) args.getArguments()[0];
            Consumer<List<IngestDocumentWrapper>> handler = (Consumer) args.getArguments()[1];
            handler.accept(ingestDocumentWrappers);
            return null;
        }).when(processor).batchExecute(any(), any());
        Map<String, Processor.Factory> map = new HashMap<>(2);
        map.put("mock", (factories, tag, description, config) -> processor);

        IngestService ingestService = createWithProcessors(map);
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\": {}}], \"description\": \"_description\"}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            numRequest,
            bulkRequest.requests(),
            requestItemErrorHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );

        verify(requestItemErrorHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        for (DocWriteRequest<?> docWriteRequest : bulkRequest.requests()) {
            IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(docWriteRequest);
            assertThat(indexRequest, notNullValue());
            assertThat(indexRequest.getContentType(), equalTo(xContentType));
        }
    }

    public void testStats() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor processorFailure = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        when(processorFailure.getType()).thenReturn("failure-mock");
        // avoid returning null and dropping the document
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(RandomDocumentPicks.randomIngestDocument(random()), null);
            return null;
        }).when(processor).execute(any(IngestDocument.class), any());
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(null, new RuntimeException("error"));
            return null;
        }).when(processorFailure).execute(any(IngestDocument.class), any());
        Map<String, Processor.Factory> map = new HashMap<>(2);
        map.put("mock", (factories, tag, description, config) -> processor);
        map.put("failure-mock", (factories, tag, description, config) -> processorFailure);
        IngestService ingestService = createWithProcessors(map);

        final IngestStats initialStats = ingestService.stats();
        assertThat(initialStats.getPipelineStats().size(), equalTo(0));
        assertStats(initialStats.getTotalStats(), 0, 0, 0);

        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        putRequest = new PutPipelineRequest("_id2", new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), MediaTypeRegistry.JSON);
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);

        final IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1").setFinalPipeline("_none");
        indexRequest.source(randomAlphaOfLength(10), randomAlphaOfLength(10));
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        final IngestStats afterFirstRequestStats = ingestService.stats();
        assertThat(afterFirstRequestStats.getPipelineStats().size(), equalTo(2));

        afterFirstRequestStats.getProcessorStats().get("_id1").forEach(p -> assertEquals(p.getName(), "mock:mockTag"));
        afterFirstRequestStats.getProcessorStats().get("_id2").forEach(p -> assertEquals(p.getName(), "mock:mockTag"));

        // total
        assertStats(afterFirstRequestStats.getTotalStats(), 1, 0, 0);
        // pipeline
        assertPipelineStats(afterFirstRequestStats.getPipelineStats(), "_id1", 1, 0, 0);
        assertPipelineStats(afterFirstRequestStats.getPipelineStats(), "_id2", 0, 0, 0);
        // processor
        assertProcessorStats(0, afterFirstRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterFirstRequestStats, "_id2", 0, 0, 0);

        indexRequest.setPipeline("_id2");
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        final IngestStats afterSecondRequestStats = ingestService.stats();
        assertThat(afterSecondRequestStats.getPipelineStats().size(), equalTo(2));
        // total
        assertStats(afterSecondRequestStats.getTotalStats(), 2, 0, 0);
        // pipeline
        assertPipelineStats(afterSecondRequestStats.getPipelineStats(), "_id1", 1, 0, 0);
        assertPipelineStats(afterSecondRequestStats.getPipelineStats(), "_id2", 1, 0, 0);
        // processor
        assertProcessorStats(0, afterSecondRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterSecondRequestStats, "_id2", 1, 0, 0);

        // update cluster state and ensure that new stats are added to old stats
        putRequest = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}, {\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        final IngestStats afterThirdRequestStats = ingestService.stats();
        assertThat(afterThirdRequestStats.getPipelineStats().size(), equalTo(2));
        // total
        assertStats(afterThirdRequestStats.getTotalStats(), 3, 0, 0);
        // pipeline
        assertPipelineStats(afterThirdRequestStats.getPipelineStats(), "_id1", 2, 0, 0);
        assertPipelineStats(afterThirdRequestStats.getPipelineStats(), "_id2", 1, 0, 0);
        // The number of processors for the "id1" pipeline changed, so the per-processor metrics are not carried forward. This is
        // due to the parallel array's used to identify which metrics to carry forward. With out unique ids or semantic equals for each
        // processor, parallel arrays are the best option for of carrying forward metrics between pipeline changes. However, in some cases,
        // like this one it may not readily obvious why the metrics were not carried forward.
        assertProcessorStats(0, afterThirdRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(1, afterThirdRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterThirdRequestStats, "_id2", 1, 0, 0);

        // test a failure, and that the processor stats are added from the old stats
        putRequest = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"failure-mock\" : { \"on_failure\": [{\"mock\" : {}}]}}, {\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            mockBulkRequest
        );
        final IngestStats afterForthRequestStats = ingestService.stats();
        assertThat(afterForthRequestStats.getPipelineStats().size(), equalTo(2));
        // total
        assertStats(afterForthRequestStats.getTotalStats(), 4, 0, 0);
        // pipeline
        assertPipelineStats(afterForthRequestStats.getPipelineStats(), "_id1", 3, 0, 0);
        assertPipelineStats(afterForthRequestStats.getPipelineStats(), "_id2", 1, 0, 0);
        // processor
        assertProcessorStats(0, afterForthRequestStats, "_id1", 1, 1, 0); // not carried forward since type changed
        assertProcessorStats(1, afterForthRequestStats, "_id1", 2, 0, 0); // carried forward and added from old stats
        assertProcessorStats(0, afterForthRequestStats, "_id2", 1, 0, 0);
    }

    public void testStatName() {
        Processor processor = mock(Processor.class);
        String name = randomAlphaOfLength(10);
        when(processor.getType()).thenReturn(name);
        assertThat(IngestService.getProcessorName(processor), equalTo(name));
        String tag = randomAlphaOfLength(10);
        when(processor.getTag()).thenReturn(tag);
        assertThat(IngestService.getProcessorName(processor), equalTo(name + ":" + tag));

        ConditionalProcessor conditionalProcessor = mock(ConditionalProcessor.class);
        when(conditionalProcessor.getInnerProcessor()).thenReturn(processor);
        assertThat(IngestService.getProcessorName(conditionalProcessor), equalTo(name + ":" + tag));

        PipelineProcessor pipelineProcessor = mock(PipelineProcessor.class);
        String pipelineName = randomAlphaOfLength(10);
        when(pipelineProcessor.getPipelineTemplate()).thenReturn(new TestTemplateService.MockTemplateScript.Factory(pipelineName));
        name = PipelineProcessor.TYPE;
        when(pipelineProcessor.getType()).thenReturn(name);
        assertThat(IngestService.getProcessorName(pipelineProcessor), equalTo(name + ":" + pipelineName));
        when(pipelineProcessor.getTag()).thenReturn(tag);
        assertThat(IngestService.getProcessorName(pipelineProcessor), equalTo(name + ":" + pipelineName + ":" + tag));
    }

    public void testExecuteWithDrop() {
        Map<String, Processor.Factory> factories = new HashMap<>();
        factories.put("drop", new DropProcessor.Factory());
        factories.put("mock", (processorFactories, tag, description, config) -> new Processor() {
            @Override
            public IngestDocument execute(final IngestDocument ingestDocument) {
                throw new AssertionError("Document should have been dropped but reached this processor");
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public String getTag() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }
        });
        IngestService ingestService = createWithProcessors(factories);
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"drop\" : {}}, {\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        BulkRequest bulkRequest = new BulkRequest();
        final IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1")
            .source(Collections.emptyMap())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);

        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2")
            .source(Collections.emptyMap())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);

        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final IntConsumer dropHandler = mock(IntConsumer.class);
        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            dropHandler,
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(dropHandler, times(1)).accept(1);
    }

    public void testIngestClusterStateListeners_orderOfExecution() {
        final AtomicInteger counter = new AtomicInteger(0);

        // Ingest cluster state listener state should be invoked first:
        Consumer<ClusterState> ingestClusterStateListener = clusterState -> { assertThat(counter.compareAndSet(0, 1), is(true)); };

        // Processor factory should be invoked secondly after ingest cluster state listener:
        IngestPlugin testPlugin = new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
                return Collections.singletonMap("test", (factories, tag, description, config) -> {
                    assertThat(counter.compareAndSet(1, 2), is(true));
                    return new FakeProcessor("test", tag, description, ingestDocument -> {});
                });
            }
        };

        // Create ingest service:
        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        IngestService ingestService = new IngestService(
            clusterService,
            threadPool,
            null,
            null,
            null,
            Arrays.asList(testPlugin),
            client,
            mock(IndicesService.class)
        );
        ingestService.addIngestClusterStateListener(ingestClusterStateListener);

        // Create pipeline and apply the resulting cluster state, which should update the counter in the right order:
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"test\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        // Sanity check that counter has been updated twice:
        assertThat(counter.get(), equalTo(2));
    }

    public void testCBORParsing() throws Exception {
        AtomicReference<Object> reference = new AtomicReference<>();
        Consumer<IngestDocument> executor = doc -> reference.set(doc.getFieldValueAsBytes("data"));
        final IngestService ingestService = createWithProcessors(
            Collections.singletonMap("foo", (factories, tag, description, config) -> new FakeProcessor("foo", tag, description, executor))
        );

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"foo\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        try (XContentBuilder builder = CborXContent.contentBuilder()) {
            builder.startObject();
            builder.field("data", "This is my data".getBytes(StandardCharsets.UTF_8));
            builder.endObject();

            IndexRequest indexRequest = new IndexRequest("_index").id("_doc-id")
                .source(builder)
                .setPipeline("_id")
                .setFinalPipeline("_none");

            ingestService.executeBulkRequest(
                1,
                Collections.singletonList(indexRequest),
                (integer, e) -> {},
                (thread, e) -> {},
                indexReq -> {},
                Names.WRITE,
                mockBulkRequest
            );
        }

        assertThat(reference.get(), is(instanceOf(byte[].class)));
    }

    public void testResolveRequiredOrDefaultPipelineDefaultPipeline() {
        IndexMetadata.Builder builder = IndexMetadata.builder("idx")
            .settings(settings(Version.CURRENT).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias").writeIndex(true).build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // index name matches with IDM:
        IndexRequest indexRequest = new IndexRequest("idx");
        boolean result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
        assertThat(result, is(true));
        assertThat(indexRequest.isPipelineResolved(), is(true));
        assertThat(indexRequest.getPipeline(), equalTo("default-pipeline"));

        // alias name matches with IDM:
        indexRequest = new IndexRequest("alias");
        result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
        assertThat(result, is(true));
        assertThat(indexRequest.isPipelineResolved(), is(true));
        assertThat(indexRequest.getPipeline(), equalTo("default-pipeline"));

        // index name matches with ITMD:
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("name1")
            .patterns(Collections.singletonList("id*"))
            .settings(settings(Version.CURRENT).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"));
        metadata = Metadata.builder().put(templateBuilder).build();
        indexRequest = new IndexRequest("idx");
        result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
        assertThat(result, is(true));
        assertThat(indexRequest.isPipelineResolved(), is(true));
        assertThat(indexRequest.getPipeline(), equalTo("default-pipeline"));

        // index name matches with ITMD for bulk upsert
        UpdateRequest updateRequest = new UpdateRequest("idx", "id1").upsert(emptyMap()).script(mockScript("1"));
        result = IngestService.resolvePipelines(updateRequest, TransportBulkAction.getIndexWriteRequest(updateRequest), metadata);
        assertThat(result, is(true));
        assertThat(updateRequest.upsertRequest().isPipelineResolved(), is(true));
        assertThat(updateRequest.upsertRequest().getPipeline(), equalTo("default-pipeline"));
    }

    public void testResolveFinalPipeline() {
        IndexMetadata.Builder builder = IndexMetadata.builder("idx")
            .settings(settings(Version.CURRENT).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias").writeIndex(true).build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // index name matches with IDM:
        IndexRequest indexRequest = new IndexRequest("idx");
        boolean result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
        assertThat(result, is(true));
        assertThat(indexRequest.isPipelineResolved(), is(true));
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));

        // alias name matches with IDM:
        indexRequest = new IndexRequest("alias");
        result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
        assertThat(result, is(true));
        assertThat(indexRequest.isPipelineResolved(), is(true));
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));

        // index name matches with ITMD:
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("name1")
            .patterns(Collections.singletonList("id*"))
            .settings(settings(Version.CURRENT).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"));
        metadata = Metadata.builder().put(templateBuilder).build();
        indexRequest = new IndexRequest("idx");
        result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
        assertThat(result, is(true));
        assertThat(indexRequest.isPipelineResolved(), is(true));
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));

        // index name matches with ITMD for bulk upsert:
        UpdateRequest updateRequest = new UpdateRequest("idx", "id1").upsert(emptyMap()).script(mockScript("1"));
        result = IngestService.resolvePipelines(updateRequest, TransportBulkAction.getIndexWriteRequest(updateRequest), metadata);
        assertThat(result, is(true));
        assertThat(updateRequest.upsertRequest().isPipelineResolved(), is(true));
        assertThat(updateRequest.upsertRequest().getFinalPipeline(), equalTo("final-pipeline"));
    }

    public void testResolveRequestOrDefaultPipelineAndFinalPipeline() {
        // no pipeline:
        {
            Metadata metadata = Metadata.builder().build();
            IndexRequest indexRequest = new IndexRequest("idx");
            boolean result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
            assertThat(result, is(false));
            assertThat(indexRequest.isPipelineResolved(), is(true));
            assertThat(indexRequest.getPipeline(), equalTo(IngestService.NOOP_PIPELINE_NAME));
        }

        // request pipeline:
        {
            Metadata metadata = Metadata.builder().build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("request-pipeline");
            boolean result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
            assertThat(result, is(true));
            assertThat(indexRequest.isPipelineResolved(), is(true));
            assertThat(indexRequest.getPipeline(), equalTo("request-pipeline"));
        }

        // request pipeline with default pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(Version.CURRENT).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("request-pipeline");
            boolean result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
            assertThat(result, is(true));
            assertThat(indexRequest.isPipelineResolved(), is(true));
            assertThat(indexRequest.getPipeline(), equalTo("request-pipeline"));
        }

        // request pipeline with final pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(Version.CURRENT).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("request-pipeline");
            boolean result = IngestService.resolvePipelines(indexRequest, indexRequest, metadata);
            assertThat(result, is(true));
            assertThat(indexRequest.isPipelineResolved(), is(true));
            assertThat(indexRequest.getPipeline(), equalTo("request-pipeline"));
            assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));
        }
    }

    public void testExecuteBulkRequestInBatch() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3").source(emptyMap()).setPipeline("_none").setFinalPipeline("_id");
        bulkRequest.add(indexRequest3);
        IndexRequest indexRequest4 = new IndexRequest("_index").id("_id4").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest4);
        bulkRequest.batchSize(2);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            4,
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(mockCompoundProcessor, times(2)).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchWithDefaultAndFinalPipeline() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        ClusterState clusterState = createPipeline("_id", ingestService);
        createPipeline("_final", ingestService, clusterState);
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_final");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_final");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3").source(emptyMap()).setPipeline("_id").setFinalPipeline("_final");
        bulkRequest.add(indexRequest3);
        IndexRequest indexRequest4 = new IndexRequest("_index").id("_id4").source(emptyMap()).setPipeline("_id").setFinalPipeline("_final");
        bulkRequest.add(indexRequest4);
        bulkRequest.batchSize(2);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            4,
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(mockCompoundProcessor, times(4)).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchFallbackWithOneDocument() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        bulkRequest.batchSize(2);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(mockCompoundProcessor, never()).batchExecute(any(), any());
        verify(mockCompoundProcessor, times(1)).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchNoValidPipeline() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        // will not be handled as not valid document type
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1")
            .source(emptyMap())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2")
            .source(emptyMap())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        bulkRequest.batchSize(2);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            2,
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(mockCompoundProcessor, never()).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchNoValidDocument() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        // will not be handled as not valid document type
        bulkRequest.add(new DeleteRequest("_index", "_id"));
        bulkRequest.add(new DeleteRequest("_index", "_id"));
        bulkRequest.batchSize(2);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            2,
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(mockCompoundProcessor, never()).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchWithException() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        doThrow(new RuntimeException()).when(mockCompoundProcessor).batchExecute(any(), any());
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        // will not be handled as not valid document type
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        bulkRequest.batchSize(2);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            2,
            bulkRequest.requests(),
            failureHandler,
            completionHandler,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        verify(failureHandler, times(2)).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(mockCompoundProcessor, times(1)).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchWithExceptionAndDropInCallback() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        // will not be handled as not valid document type
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest3);
        bulkRequest.batchSize(3);

        List<IngestDocumentWrapper> results = Arrays.asList(
            new IngestDocumentWrapper(0, IngestService.toIngestDocument(indexRequest1), null),
            new IngestDocumentWrapper(1, null, new RuntimeException()),
            new IngestDocumentWrapper(2, null, null)
        );
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            Consumer<List<IngestDocumentWrapper>> handler = (Consumer) args.getArguments()[1];
            handler.accept(results);
            return null;
        }).when(mockCompoundProcessor).batchExecute(any(), any());

        final Map<Integer, Exception> failureHandler = new HashMap<>();
        final Map<Thread, Exception> completionHandler = new HashMap<>();
        final List<Integer> dropHandler = new ArrayList<>();
        ingestService.executeBulkRequest(
            3,
            bulkRequest.requests(),
            failureHandler::put,
            completionHandler::put,
            dropHandler::add,
            Names.WRITE,
            bulkRequest
        );
        assertEquals(Set.of(1), failureHandler.keySet());
        assertEquals(List.of(2), dropHandler);
        assertEquals(Set.of(Thread.currentThread()), completionHandler.keySet());
        verify(mockCompoundProcessor, times(1)).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteBulkRequestInBatchWithDefaultBatchSize() {
        CompoundProcessor mockCompoundProcessor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Collections.singletonMap("mock", (factories, tag, description, config) -> mockCompoundProcessor)
        );
        createPipeline("_id", ingestService);
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3").source(emptyMap()).setPipeline("_none").setFinalPipeline("_id");
        bulkRequest.add(indexRequest3);
        IndexRequest indexRequest4 = new IndexRequest("_index").id("_id4").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest4);
        @SuppressWarnings("unchecked")
        final Map<Integer, Exception> failureHandler = new HashMap<>();
        final Map<Thread, Exception> completionHandler = new HashMap<>();
        final List<Integer> dropHandler = new ArrayList<>();
        ingestService.executeBulkRequest(
            4,
            bulkRequest.requests(),
            failureHandler::put,
            completionHandler::put,
            dropHandler::add,
            Names.WRITE,
            bulkRequest
        );
        assertTrue(failureHandler.isEmpty());
        assertTrue(dropHandler.isEmpty());
        assertEquals(1, completionHandler.size());
        assertNull(completionHandler.get(Thread.currentThread()));
        verify(mockCompoundProcessor, times(1)).batchExecute(any(), any());
        verify(mockCompoundProcessor, never()).execute(any(), any());
    }

    public void testExecuteEmptyPipelineInBatch() throws Exception {
        IngestService ingestService = createWithProcessors(emptyMap());
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [], \"description\": \"_description\"}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest3);
        IndexRequest indexRequest4 = new IndexRequest("_index").id("_id4").source(emptyMap()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest4);
        bulkRequest.batchSize(4);
        final Map<Integer, Exception> failureHandler = new HashMap<>();
        final Map<Thread, Exception> completionHandler = new HashMap<>();
        ingestService.executeBulkRequest(
            4,
            bulkRequest.requests(),
            failureHandler::put,
            completionHandler::put,
            indexReq -> {},
            Names.WRITE,
            bulkRequest
        );
        assertTrue(failureHandler.isEmpty());
        assertEquals(Set.of(Thread.currentThread()), completionHandler.keySet());
    }

    public void testPrepareBatches_same_index_pipeline() {
        IngestService.IndexRequestWrapper wrapper1 = createIndexRequestWrapper("index1", Collections.singletonList("p1"));
        IngestService.IndexRequestWrapper wrapper2 = createIndexRequestWrapper("index1", Collections.singletonList("p1"));
        IngestService.IndexRequestWrapper wrapper3 = createIndexRequestWrapper("index1", Collections.singletonList("p1"));
        IngestService.IndexRequestWrapper wrapper4 = createIndexRequestWrapper("index1", Collections.singletonList("p1"));
        List<List<IngestService.IndexRequestWrapper>> batches = IngestService.prepareBatches(
            2,
            Arrays.asList(wrapper1, wrapper2, wrapper3, wrapper4)
        );
        assertEquals(2, batches.size());
        for (int i = 0; i < 2; ++i) {
            assertEquals(2, batches.get(i).size());
        }
    }

    public void testPrepareBatches_different_index_pipeline() {
        IngestService.IndexRequestWrapper wrapper1 = createIndexRequestWrapper("index1", Collections.singletonList("p1"));
        IngestService.IndexRequestWrapper wrapper2 = createIndexRequestWrapper("index2", Collections.singletonList("p1"));
        IngestService.IndexRequestWrapper wrapper3 = createIndexRequestWrapper("index1", Arrays.asList("p1", "p2"));
        IngestService.IndexRequestWrapper wrapper4 = createIndexRequestWrapper("index1", Collections.singletonList("p2"));
        List<List<IngestService.IndexRequestWrapper>> batches = IngestService.prepareBatches(
            2,
            Arrays.asList(wrapper1, wrapper2, wrapper3, wrapper4)
        );
        assertEquals(4, batches.size());
    }

    public void testUpdateMaxIngestProcessorCountSetting() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // verify defaults
        assertEquals(Integer.MAX_VALUE, clusterSettings.get(IngestService.MAX_NUMBER_OF_INGEST_PROCESSORS).intValue());

        // verify update max processor
        Settings newSettings = Settings.builder().put("cluster.ingest.max_number_processors", 3).build();
        clusterSettings.applySettings(newSettings);
        assertEquals(3, clusterSettings.get(IngestService.MAX_NUMBER_OF_INGEST_PROCESSORS).intValue());
    }

    private IngestService.IndexRequestWrapper createIndexRequestWrapper(String index, List<String> pipelines) {
        IndexRequest indexRequest = new IndexRequest(index);
        return new IngestService.IndexRequestWrapper(0, indexRequest, pipelines, true);
    }

    private IngestDocument eqIndexTypeId(final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", -3L, VersionType.INTERNAL, source));
    }

    private IngestDocument eqIndexTypeId(final Long version, final VersionType versionType, final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", version, versionType, source));
    }

    private static IngestService createWithProcessors() {
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("set", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");
            return new FakeProcessor("set", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
        });
        processors.put("remove", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            return new WrappingProcessorImpl("remove", tag, description, (ingestDocument -> ingestDocument.removeField(field))) {
            };
        });
        return createWithProcessors(processors);
    }

    private static IngestService createWithProcessors(Map<String, Processor.Factory> processors) {

        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = OpenSearchExecutors.newDirectExecutorService();
        when(threadPool.generic()).thenReturn(executorService);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        return new IngestService(clusterService, threadPool, null, null, null, Collections.singletonList(new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
                return processors;
            }
        }), client, mock(IndicesService.class));
    }

    private CompoundProcessor mockCompoundProcessor() {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept((IngestDocument) args.getArguments()[0], null);
            return null;
        }).when(processor).execute(any(), any());

        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            Consumer<List<IngestDocumentWrapper>> handler = (Consumer) args.getArguments()[1];
            handler.accept((List<IngestDocumentWrapper>) args.getArguments()[0]);
            return null;
        }).when(processor).batchExecute(any(), any());
        return processor;
    }

    private class IngestDocumentMatcher implements ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        IngestDocumentMatcher(String index, String type, String id, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, id, null, null, null, source);
        }

        IngestDocumentMatcher(String index, String type, String id, Long version, VersionType versionType, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, id, null, version, versionType, source);
        }

        @Override
        public boolean matches(IngestDocument o) {
            return Objects.equals(ingestDocument.getSourceAndMetadata(), o.getSourceAndMetadata());
        }
    }

    private void assertProcessorStats(int processor, IngestStats stats, String pipelineId, long count, long failed, long time) {
        assertStats(stats.getProcessorStats().get(pipelineId).get(processor).getStats(), count, failed, time);
    }

    private void assertPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String pipelineId, long count, long failed, long time) {
        assertStats(getPipelineStats(pipelineStats, pipelineId), count, failed, time);
    }

    private void assertStats(OperationStats stats, long count, long failed, long time) {
        assertThat(stats.getCount(), equalTo(count));
        assertThat(stats.getCurrent(), equalTo(0L));
        assertThat(stats.getFailedCount(), equalTo(failed));
        assertThat(stats.getTotalTimeInMillis(), greaterThanOrEqualTo(time));
    }

    private OperationStats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream().filter(p1 -> p1.getPipelineId().equals(id)).findFirst().map(p2 -> p2.getStats()).orElse(null);
    }

    private ClusterState createPipeline(String pipeline, IngestService ingestService) {
        return createPipeline(pipeline, ingestService, null);
    }

    private ClusterState createPipeline(String pipeline, IngestService ingestService, ClusterState previousState) {
        PutPipelineRequest putRequest = new PutPipelineRequest(
            pipeline,
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            MediaTypeRegistry.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        if (previousState != null) {
            clusterState = previousState;
        }
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        return clusterState;
    }
}
