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

package org.opensearch.indices.mapping;

import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class UpdateMappingIntegrationIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testDynamicUpdates() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
            )
            .execute()
            .actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder().put(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(5))
            )
            .get();

        int recCount = randomIntBetween(20, 200);
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int rec = 0; rec < recCount; rec++) {
            String type = "type";
            String fieldName = "field_" + type + "_" + rec;
            indexRequests.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(rec))
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setSource(fieldName, "some_value")
            );
        }
        indexRandom(true, false, indexRequests);

        logger.info("checking all the documents are there");
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        SearchResponse response = client().prepareSearch("test").setSize(0).execute().actionGet();
        assertThat(response.getHits().getTotalHits().value(), equalTo((long) recCount));

        logger.info("checking all the fields are in the mappings");

        for (int rec = 0; rec < recCount; rec++) {
            String type = "type";
            String fieldName = "field_" + type + "_" + rec;
            assertConcreteMappingsOnAll("test", fieldName);
        }

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(MappingUpdatedAction.INDICES_MAPPING_DYNAMIC_TIMEOUT_SETTING.getKey()))
            .get();
    }

    public void testUpdateMappingWithoutType() {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .setMapping("{\"properties\":{\"body\":{\"type\":\"text\"}}}")
            .execute()
            .actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        AcknowledgedResponse putMappingResponse = client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", MediaTypeRegistry.JSON)
            .execute()
            .actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").execute().actionGet();
        assertThat(
            getMappingsResponse.mappings().get("test").source().toString(),
            equalTo("{\"_doc\":{\"properties\":{\"body\":{\"type\":\"text\"},\"date\":{\"type\":\"integer\"}}}}")
        );
    }

    public void testUpdateMappingWithoutTypeMultiObjects() {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .execute()
            .actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        AcknowledgedResponse putMappingResponse = client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", MediaTypeRegistry.JSON)
            .execute()
            .actionGet();

        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));

        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings("test").execute().actionGet();
        assertThat(
            getMappingsResponse.mappings().get("test").source().toString(),
            equalTo("{\"_doc\":{\"properties\":{\"date\":{\"type\":\"integer\"}}}}")
        );
    }

    public void testUpdateMappingWithConflicts() {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
            .setMapping("{\"properties\":{\"body\":{\"type\":\"text\"}}}")
            .execute()
            .actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        try {
            client().admin()
                .indices()
                .preparePutMapping("test")
                .setSource(
                    "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"body\":{\"type\":\"integer\"}}}}",
                    MediaTypeRegistry.JSON
                )
                .execute()
                .actionGet();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [body] cannot be changed from type [text] to [integer]"));
        }
    }

    public void testUpdateMappingWithNormsConflicts() {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping("{\"properties\":{\"body\":{\"type\":\"text\", \"norms\": false }}}")
            .execute()
            .actionGet();
        try {
            client().admin()
                .indices()
                .preparePutMapping("test")
                .setSource(
                    "{\"" + MapperService.SINGLE_MAPPING_NAME + "\":{\"properties\":{\"body\":{\"type\":\"text\", \"norms\": true }}}}",
                    MediaTypeRegistry.JSON
                )
                .execute()
                .actionGet();
            fail("Expected MergeMappingException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot update parameter [norms] from [false] to [true]"));
        }
    }

    /*
    Second regression test for https://github.com/elastic/elasticsearch/issues/3381
     */
    public void testUpdateMappingNoChanges() {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
            .setMapping("{\"properties\":{\"body\":{\"type\":\"text\"}}}")
            .execute()
            .actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        AcknowledgedResponse putMappingResponse = client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource("{\"properties\":{\"body\":{\"type\":\"text\"}}}", MediaTypeRegistry.JSON)
            .execute()
            .actionGet();

        // no changes, we return
        assertThat(putMappingResponse.isAcknowledged(), equalTo(true));
    }

    public void testUpdateMappingConcurrently() throws Throwable {
        createIndex("test1", "test2");

        final AtomicReference<Exception> threadException = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread[] threads = new Thread[3];
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final ArrayList<Client> clientArray = new ArrayList<>();
        for (Client c : clients()) {
            clientArray.add(c);
        }

        for (int j = 0; j < threads.length; j++) {
            threads[j] = new Thread(() -> {
                try {
                    barrier.await();

                    for (int i = 0; i < 100; i++) {
                        if (stop.get()) {
                            return;
                        }

                        Client client1 = clientArray.get(i % clientArray.size());
                        Client client2 = clientArray.get((i + 1) % clientArray.size());
                        String indexName = i % 2 == 0 ? "test2" : "test1";
                        String fieldName = Thread.currentThread().getName() + "_" + i;

                        AcknowledgedResponse response = client1.admin()
                            .indices()
                            .preparePutMapping(indexName)
                            .setSource(
                                JsonXContent.contentBuilder()
                                    .startObject()
                                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                                    .startObject("properties")
                                    .startObject(fieldName)
                                    .field("type", "text")
                                    .endObject()
                                    .endObject()
                                    .endObject()
                                    .endObject()
                            )
                            .setClusterManagerNodeTimeout(TimeValue.timeValueMinutes(5))
                            .get();

                        assertThat(response.isAcknowledged(), equalTo(true));
                        GetMappingsResponse getMappingResponse = client2.admin().indices().prepareGetMappings(indexName).get();
                        MappingMetadata mappings = getMappingResponse.getMappings().get(indexName);
                        assertThat(
                            ((Map<String, Object>) mappings.getSourceAsMap().get("properties")).keySet(),
                            Matchers.hasItem(fieldName)
                        );
                    }
                } catch (Exception e) {
                    threadException.set(e);
                    stop.set(true);
                }
            });

            threads[j].setName("t_" + j);
            threads[j].start();
        }

        for (Thread t : threads)
            t.join();

        if (threadException.get() != null) {
            throw threadException.get();
        }

    }

    public void testPutMappingsWithBlocks() {
        createIndex("test");
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", block);
                assertAcked(
                    client().admin()
                        .indices()
                        .preparePutMapping("test")
                        .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", MediaTypeRegistry.JSON)
                );
            } finally {
                disableIndexBlock("test", block);
            }
        }

        for (String block : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", block);
                assertBlocked(
                    client().admin()
                        .indices()
                        .preparePutMapping("test")
                        .setSource("{\"properties\":{\"date\":{\"type\":\"integer\"}}}", MediaTypeRegistry.JSON)
                );
            } finally {
                disableIndexBlock("test", block);
            }
        }
    }

    /**
     * Waits until mappings for the provided fields exist on all nodes. Note, this waits for the current
     * started shards and checks for concrete mappings.
     */
    private void assertConcreteMappingsOnAll(final String index, final String... fieldNames) {
        Set<String> nodes = internalCluster().nodesInclude(index);
        assertThat(nodes, Matchers.not(Matchers.emptyIterable()));
        for (String node : nodes) {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            assertThat("index service doesn't exists on " + node, indexService, notNullValue());
            MapperService mapperService = indexService.mapperService();
            for (String fieldName : fieldNames) {
                MappedFieldType fieldType = mapperService.fieldType(fieldName);
                assertNotNull("field " + fieldName + " doesn't exists on " + node, fieldType);
            }
        }
        assertMappingOnClusterManager(index, fieldNames);
    }

    /**
     * Waits for the given mapping type to exists on the cluster-manager node.
     */
    private void assertMappingOnClusterManager(final String index, final String... fieldNames) {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(index).get();
        MappingMetadata mappings = response.getMappings().get(index);
        assertThat(mappings, notNullValue());
        Map<String, Object> mappingSource = mappings.getSourceAsMap();

        assertFalse(mappingSource.isEmpty());
        assertTrue(mappingSource.containsKey("properties"));

        for (String fieldName : fieldNames) {
            Map<String, Object> mappingProperties = (Map<String, Object>) mappingSource.get("properties");
            if (fieldName.indexOf('.') != -1) {
                fieldName = fieldName.replace(".", ".properties.");
            }
            assertThat(
                "field " + fieldName + " doesn't exists in mapping " + mappings.source().string(),
                XContentMapValues.extractValue(fieldName, mappingProperties),
                notNullValue()
            );
        }
    }
}
