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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.ingest.SimulateDocumentBaseResult;
import org.opensearch.action.ingest.SimulatePipelineRequest;
import org.opensearch.action.ingest.SimulatePipelineResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.Requests;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.hamcrest.MatcherAssert;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.NodeRoles.nonIngestNode;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@OpenSearchIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class IngestClientIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public IngestClientIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return replicationSettings;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (nodeOrdinal % 2 == 0) {
            return Settings.builder().put(nonIngestNode()).put(super.nodeSettings(nodeOrdinal)).build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ExtendedIngestTestPlugin.class);
    }

    public void testSimulate() throws Exception {
        BytesReference pipelineSource = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        client().admin().cluster().preparePutPipeline("_id", pipelineSource, MediaTypeRegistry.JSON).get();
        GetPipelineResponse getResponse = client().admin().cluster().prepareGetPipeline("_id").get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        BytesReference bytes = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("docs")
                .startObject()
                .field("_index", "index")
                .field("_id", "id")
                .startObject("_source")
                .field("foo", "bar")
                .field("fail", false)
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        SimulatePipelineResponse response;
        if (randomBoolean()) {
            response = client().admin().cluster().prepareSimulatePipeline(bytes, MediaTypeRegistry.JSON).setId("_id").get();
        } else {
            SimulatePipelineRequest request = new SimulatePipelineRequest(bytes, MediaTypeRegistry.JSON);
            request.setId("_id");
            response = client().admin().cluster().simulatePipeline(request).get();
        }
        assertThat(response.isVerbose(), equalTo(false));
        assertThat(response.getPipelineId(), equalTo("_id"));
        assertThat(response.getResults().size(), equalTo(1));
        assertThat(response.getResults().get(0), instanceOf(SimulateDocumentBaseResult.class));
        SimulateDocumentBaseResult simulateDocumentBaseResult = (SimulateDocumentBaseResult) response.getResults().get(0);
        Map<String, Object> source = new HashMap<>();
        source.put("foo", "bar");
        source.put("fail", false);
        source.put("processed", true);
        IngestDocument ingestDocument = new IngestDocument("index", "id", null, null, null, source);
        assertThat(simulateDocumentBaseResult.getIngestDocument().getSourceAndMetadata(), equalTo(ingestDocument.getSourceAndMetadata()));
        assertThat(simulateDocumentBaseResult.getFailure(), nullValue());

        // cleanup
        AcknowledgedResponse deletePipelineResponse = client().admin().cluster().prepareDeletePipeline("_id").get();
        assertTrue(deletePipelineResponse.isAcknowledged());
    }

    public void testBulkWithIngestFailures() throws Exception {
        runBulkTestWithRandomDocs(false);
    }

    public void testBulkWithIngestFailuresWithBatchSize() throws Exception {
        runBulkTestWithRandomDocs(true);
    }

    private void runBulkTestWithRandomDocs(boolean shouldSetBatchSize) throws Exception {
        createIndex("index");

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        int numRequests = scaledRandomIntBetween(32, 128);
        BulkRequest bulkRequest = new BulkRequest();
        if (shouldSetBatchSize) {
            bulkRequest.batchSize(scaledRandomIntBetween(2, numRequests));
        }
        for (int i = 0; i < numRequests; i++) {
            IndexRequest indexRequest = new IndexRequest("index").id(Integer.toString(i)).setPipeline("_id");
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field", "value", "fail", i % 2 == 0);
            bulkRequest.add(indexRequest);
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            BulkItemResponse itemResponse = response.getItems()[i];
            if (i % 2 == 0) {
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                OpenSearchException compoundProcessorException = (OpenSearchException) failure.getCause();
                assertThat(compoundProcessorException.getRootCause().getMessage(), equalTo("test processor failed"));
            } else {
                IndexResponse indexResponse = itemResponse.getResponse();
                assertThat(
                    "Expected a successful response but found failure [" + itemResponse.getFailure() + "].",
                    itemResponse.isFailed(),
                    is(false)
                );
                assertThat(indexResponse, notNullValue());
                assertThat(indexResponse.getId(), equalTo(Integer.toString(i)));
                // verify field of successful doc
                Map<String, Object> successDoc = client().prepareGet("index", indexResponse.getId()).get().getSourceAsMap();
                assertThat(successDoc.get("processed"), equalTo(true));
                assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
            }
        }

        // cleanup
        AcknowledgedResponse deletePipelineResponse = client().admin().cluster().prepareDeletePipeline("_id").get();
        assertTrue(deletePipelineResponse.isAcknowledged());
    }

    public void testBulkWithIngestFailuresAndDropBatch() throws Exception {
        createIndex("index");

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.batchSize(3);
        bulkRequest.add(
            new IndexRequest("index").id("_fail").setPipeline("_id").source(Requests.INDEX_CONTENT_TYPE, "field", "value", "fail", true)
        );
        bulkRequest.add(
            new IndexRequest("index").id("_success").setPipeline("_id").source(Requests.INDEX_CONTENT_TYPE, "field", "value", "fail", false)
        );
        bulkRequest.add(
            new IndexRequest("index").id("_drop").setPipeline("_id").source(Requests.INDEX_CONTENT_TYPE, "field", "value", "drop", true)
        );

        BulkResponse response = client().bulk(bulkRequest).actionGet();
        MatcherAssert.assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));

        Map<String, BulkItemResponse> results = Arrays.stream(response.getItems())
            .collect(Collectors.toMap(BulkItemResponse::getId, r -> r));

        MatcherAssert.assertThat(results.keySet(), containsInAnyOrder("_fail", "_success", "_drop"));
        assertNotNull(results.get("_fail").getFailure());
        assertNull(results.get("_success").getFailure());
        assertNull(results.get("_drop").getFailure());

        // verify dropped doc not in index
        assertNull(client().prepareGet("index", "_drop").get().getSourceAsMap());

        // verify field of successful doc
        Map<String, Object> successDoc = client().prepareGet("index", "_success").get().getSourceAsMap();
        assertThat(successDoc.get("processed"), equalTo(true));

        // cleanup
        AcknowledgedResponse deletePipelineResponse = client().admin().cluster().prepareDeletePipeline("_id").get();
        assertTrue(deletePipelineResponse.isAcknowledged());
    }

    public void testBulkWithUpsert() throws Exception {
        createIndex("index");

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index").id("1").setPipeline("_id");
        indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "val1");
        bulkRequest.add(indexRequest);
        UpdateRequest updateRequest = new UpdateRequest("index", "2");
        updateRequest.doc("{}", Requests.INDEX_CONTENT_TYPE);
        updateRequest.upsert("{\"field1\":\"upserted_val\"}", MediaTypeRegistry.JSON).upsertRequest().setPipeline("_id");
        bulkRequest.add(updateRequest);

        BulkResponse response = client().bulk(bulkRequest).actionGet();

        assertThat(response.getItems().length, equalTo(bulkRequest.requests().size()));
        Map<String, Object> inserted = client().prepareGet("index", "1").get().getSourceAsMap();
        assertThat(inserted.get("field1"), equalTo("val1"));
        assertThat(inserted.get("processed"), equalTo(true));
        Map<String, Object> upserted = client().prepareGet("index", "2").get().getSourceAsMap();
        assertThat(upserted.get("field1"), equalTo("upserted_val"));
        assertThat(upserted.get("processed"), equalTo(true));
    }

    public void testSingleDocIngestFailure() throws Exception {
        createIndex("test");
        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        GetPipelineRequest getPipelineRequest = new GetPipelineRequest("_id");
        GetPipelineResponse getResponse = client().admin().cluster().getPipeline(getPipelineRequest).get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        assertThrows(
            IllegalArgumentException.class,
            () -> client().prepareIndex("test")
                .setId("1")
                .setPipeline("_id")
                .setSource(Requests.INDEX_CONTENT_TYPE, "field", "value", "fail", true)
                .get()
        );

        DeletePipelineRequest deletePipelineRequest = new DeletePipelineRequest("_id");
        AcknowledgedResponse response = client().admin().cluster().deletePipeline(deletePipelineRequest).get();
        assertThat(response.isAcknowledged(), is(true));

        getResponse = client().admin().cluster().prepareGetPipeline("_id").get();
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.pipelines().size(), equalTo(0));
    }

    public void testSingleDocIngestDrop() throws Exception {
        createIndex("test");
        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        GetPipelineRequest getPipelineRequest = new GetPipelineRequest("_id");
        GetPipelineResponse getResponse = client().admin().cluster().getPipeline(getPipelineRequest).get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        DocWriteResponse indexResponse = client().prepareIndex("test")
            .setId("1")
            .setPipeline("_id")
            .setSource(Requests.INDEX_CONTENT_TYPE, "field", "value", "drop", true)
            .get();
        assertEquals(DocWriteResponse.Result.NOOP, indexResponse.getResult());

        Map<String, Object> doc = client().prepareGet("test", "1").get().getSourceAsMap();
        assertNull(doc);

        DeletePipelineRequest deletePipelineRequest = new DeletePipelineRequest("_id");
        AcknowledgedResponse response = client().admin().cluster().deletePipeline(deletePipelineRequest).get();
        assertThat(response.isAcknowledged(), is(true));

        getResponse = client().admin().cluster().prepareGetPipeline("_id").get();
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.pipelines().size(), equalTo(0));
    }

    public void test() throws Exception {
        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        GetPipelineRequest getPipelineRequest = new GetPipelineRequest("_id");
        GetPipelineResponse getResponse = client().admin().cluster().getPipeline(getPipelineRequest).get();
        assertThat(getResponse.isFound(), is(true));
        assertThat(getResponse.pipelines().size(), equalTo(1));
        assertThat(getResponse.pipelines().get(0).getId(), equalTo("_id"));

        client().prepareIndex("test").setId("1").setPipeline("_id").setSource("field", "value", "fail", false).get();

        Map<String, Object> doc = client().prepareGet("test", "1").get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value"));
        assertThat(doc.get("processed"), equalTo(true));

        client().prepareBulk()
            .add(client().prepareIndex("test").setId("2").setSource("field", "value2", "fail", false).setPipeline("_id"))
            .get();
        doc = client().prepareGet("test", "2").get().getSourceAsMap();
        assertThat(doc.get("field"), equalTo("value2"));
        assertThat(doc.get("processed"), equalTo(true));

        DeletePipelineRequest deletePipelineRequest = new DeletePipelineRequest("_id");
        AcknowledgedResponse response = client().admin().cluster().deletePipeline(deletePipelineRequest).get();
        assertThat(response.isAcknowledged(), is(true));

        getResponse = client().admin().cluster().prepareGetPipeline("_id").get();
        assertThat(getResponse.isFound(), is(false));
        assertThat(getResponse.pipelines().size(), equalTo(0));
    }

    public void testPutWithPipelineFactoryError() throws Exception {
        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .field("unused", ":sad_face:")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id2", source, MediaTypeRegistry.JSON);
        Exception e = expectThrows(
            OpenSearchParseException.class,
            () -> client().admin().cluster().putPipeline(putPipelineRequest).actionGet()
        );
        assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));

        GetPipelineResponse response = client().admin().cluster().prepareGetPipeline("_id2").get();
        assertFalse(response.isFound());
    }

    public void testWithDedicatedClusterManager() throws Exception {
        String clusterManagerOnlyNode = internalCluster().startClusterManagerOnlyNode();
        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .field("description", "my_pipeline")
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
        PutPipelineRequest putPipelineRequest = new PutPipelineRequest("_id", source, MediaTypeRegistry.JSON);
        client().admin().cluster().putPipeline(putPipelineRequest).get();

        BulkItemResponse item = client(clusterManagerOnlyNode).prepareBulk()
            .add(client().prepareIndex("test").setSource("field", "value2", "drop", true).setPipeline("_id"))
            .get()
            .getItems()[0];
        assertFalse(item.isFailed());
        assertEquals("auto-generated", item.getResponse().getId());
    }

    public void testPipelineOriginHeader() throws Exception {
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "2");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("1", BytesReference.bytes(source), MediaTypeRegistry.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "3");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("2", BytesReference.bytes(source), MediaTypeRegistry.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("fail");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("3", BytesReference.bytes(source), MediaTypeRegistry.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }

        Exception e = expectThrows(Exception.class, () -> {
            IndexRequest indexRequest = new IndexRequest("test");
            indexRequest.source("{}", MediaTypeRegistry.JSON);
            indexRequest.setPipeline("1");
            client().index(indexRequest).get();
        });
        IngestProcessorException ingestException = (IngestProcessorException) ExceptionsHelper.unwrap(e, IngestProcessorException.class);
        assertThat(ingestException.getHeader("processor_type"), equalTo(Collections.singletonList("fail")));
        assertThat(ingestException.getHeader("pipeline_origin"), equalTo(Arrays.asList("3", "2", "1")));
    }

    public void testPipelineProcessorOnFailure() throws Exception {
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "2");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            {
                source.startArray("on_failure");
                source.startObject();
                {
                    source.startObject("onfailure_processor");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("1", BytesReference.bytes(source), MediaTypeRegistry.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("pipeline");
                    source.field("name", "3");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("2", BytesReference.bytes(source), MediaTypeRegistry.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }
        {
            XContentBuilder source = jsonBuilder().startObject();
            {
                source.startArray("processors");
                source.startObject();
                {
                    source.startObject("fail");
                    source.endObject();
                }
                source.endObject();
                source.endArray();
            }
            source.endObject();
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest("3", BytesReference.bytes(source), MediaTypeRegistry.JSON);
            client().admin().cluster().putPipeline(putPipelineRequest).get();
        }

        client().prepareIndex("test").setId("1").setSource("{}", MediaTypeRegistry.JSON).setPipeline("1").get();
        Map<String, Object> inserted = client().prepareGet("test", "1").get().getSourceAsMap();
        assertThat(inserted.get("readme"), equalTo("pipeline with id [3] is a bad pipeline"));
    }

    public static class ExtendedIngestTestPlugin extends IngestTestPlugin {

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> factories = new HashMap<>(super.getProcessors(parameters));
            factories.put(PipelineProcessor.TYPE, new PipelineProcessor.Factory(parameters.ingestService));
            factories.put(
                "fail",
                (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail", description, new RuntimeException())
            );
            factories.put(
                "onfailure_processor",
                (processorFactories, tag, description, config) -> new TestProcessor(tag, "fail", description, document -> {
                    String onFailurePipeline = document.getFieldValue("_ingest.on_failure_pipeline", String.class);
                    document.setFieldValue("readme", "pipeline with id [" + onFailurePipeline + "] is a bad pipeline");
                })
            );
            return factories;
        }
    }

}
