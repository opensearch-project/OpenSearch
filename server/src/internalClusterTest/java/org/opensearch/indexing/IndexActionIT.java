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

package org.opensearch.indexing;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.indices.InvalidIndexNameException;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class IndexActionIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public IndexActionIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return replicationSettings;
    }

    /**
     * This test tries to simulate load while creating an index and indexing documents
     * while the index is being created.
     */

    public void testAutoGenerateIdNoDuplicates() throws Exception {
        int numberOfIterations = scaledRandomIntBetween(10, 50);
        for (int i = 0; i < numberOfIterations; i++) {
            Exception firstError = null;
            createIndex("test");
            int numOfDocs = randomIntBetween(10, 100);
            logger.info("indexing [{}] docs", numOfDocs);
            List<IndexRequestBuilder> builders = new ArrayList<>(numOfDocs);
            for (int j = 0; j < numOfDocs; j++) {
                builders.add(client().prepareIndex("test").setSource("field", "value_" + j));
            }
            indexRandom(true, builders);
            logger.info("verifying indexed content");
            int numOfChecks = randomIntBetween(8, 12);
            for (int j = 0; j < numOfChecks; j++) {
                try {
                    logger.debug("running search with all types");
                    SearchResponse response = client().prepareSearch("test").get();
                    if (response.getHits().getTotalHits().value() != numOfDocs) {
                        final String message = "Count is "
                            + response.getHits().getTotalHits().value()
                            + " but "
                            + numOfDocs
                            + " was expected. "
                            + OpenSearchAssertions.formatShardStatus(response);
                        logger.error("{}. search response: \n{}", message, response);
                        fail(message);
                    }
                } catch (Exception e) {
                    logger.error("search for all docs types failed", e);
                    if (firstError == null) {
                        firstError = e;
                    }
                }
                try {
                    logger.debug("running search with a specific type");
                    SearchResponse response = client().prepareSearch("test").get();
                    if (response.getHits().getTotalHits().value() != numOfDocs) {
                        final String message = "Count is "
                            + response.getHits().getTotalHits().value()
                            + " but "
                            + numOfDocs
                            + " was expected. "
                            + OpenSearchAssertions.formatShardStatus(response);
                        logger.error("{}. search response: \n{}", message, response);
                        fail(message);
                    }
                } catch (Exception e) {
                    logger.error("search for all docs of a specific type failed", e);
                    if (firstError == null) {
                        firstError = e;
                    }
                }
            }
            if (firstError != null) {
                fail(firstError.getMessage());
            }
            internalCluster().wipeIndices("test");
        }
    }

    public void testCreatedFlag() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test").setId("1").setSource("field1", "value1_1").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = client().prepareIndex("test").setId("1").setSource("field1", "value1_2").execute().actionGet();
        assertEquals(DocWriteResponse.Result.UPDATED, indexResponse.getResult());

        client().prepareDelete("test", "1").execute().actionGet();

        indexResponse = client().prepareIndex("test").setId("1").setSource("field1", "value1_2").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

    }

    public void testCreatedFlagWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test").setId("1").setSource("field1", "value1_1").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        client().prepareDelete("test", "1").execute().actionGet();

        flush();

        indexResponse = client().prepareIndex("test").setId("1").setSource("field1", "value1_2").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreatedFlagParallelExecution() throws Exception {
        createIndex("test");
        ensureGreen();

        int threadCount = 20;
        final int docCount = 300;
        int taskCount = docCount * threadCount;

        final AtomicIntegerArray createdCounts = new AtomicIntegerArray(docCount);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        List<Callable<Void>> tasks = new ArrayList<>(taskCount);
        final Random random = random();
        for (int i = 0; i < taskCount; i++) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int docId = random.nextInt(docCount);
                    IndexResponse indexResponse = index("test", "type", Integer.toString(docId), "field1", "value");
                    if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                        createdCounts.incrementAndGet(docId);
                    }
                    return null;
                }
            });
        }

        threadPool.invokeAll(tasks);

        for (int i = 0; i < docCount; i++) {
            assertThat(createdCounts.get(i), lessThanOrEqualTo(1));
        }
        terminate(threadPool);
    }

    public void testCreatedFlagWithExternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test")
            .setId("1")
            .setSource("field1", "value1_1")
            .setVersion(123)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreateFlagWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
            .add(client().prepareIndex("test").setId("1").setSource("field1", "value1_1"))
            .execute()
            .actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreateIndexWithLongName() {
        int min = MetadataCreateIndexService.MAX_INDEX_NAME_BYTES + 1;
        int max = MetadataCreateIndexService.MAX_INDEX_NAME_BYTES * 2;
        try {
            createIndex(randomAlphaOfLengthBetween(min, max).toLowerCase(Locale.ROOT));
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name too long: " + e.getMessage(),
                e.getMessage().contains("index name is too long,"),
                equalTo(true)
            );
        }

        try {
            client().prepareIndex(randomAlphaOfLengthBetween(min, max).toLowerCase(Locale.ROOT)).setSource("foo", "bar").get();
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name too long: " + e.getMessage(),
                e.getMessage().contains("index name is too long,"),
                equalTo(true)
            );
        }

        try {
            // Catch chars that are more than a single byte
            client().prepareIndex(
                randomAlphaOfLength(MetadataCreateIndexService.MAX_INDEX_NAME_BYTES - 1).toLowerCase(Locale.ROOT) + "Ϟ".toLowerCase(
                    Locale.ROOT
                )
            ).setSource("foo", "bar").get();
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name too long: " + e.getMessage(),
                e.getMessage().contains("index name is too long,"),
                equalTo(true)
            );
        }

        // we can create an index of max length
        createIndex(randomAlphaOfLength(MetadataCreateIndexService.MAX_INDEX_NAME_BYTES).toLowerCase(Locale.ROOT));
    }

    public void testInvalidIndexName() {
        try {
            createIndex(".");
            fail("exception should have been thrown on dot index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name is dot " + e.getMessage(),
                e.getMessage().contains("Invalid index name [.], must not be \'.\' or '..'"),
                equalTo(true)
            );
        }

        try {
            createIndex("..");
            fail("exception should have been thrown on dot index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name is dot " + e.getMessage(),
                e.getMessage().contains("Invalid index name [..], must not be \'.\' or '..'"),
                equalTo(true)
            );
        }
    }

    public void testDocumentWithBlankFieldName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            client().prepareIndex("test").setId("1").setSource("", "value1_2").execute().actionGet();
        });
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getRootCause().getMessage(), containsString("field name cannot be an empty string"));
    }

    public void testDeriveSourceMapperValidation() {
        // Test 1: Validate basic derive source mapping
        String basicMapping = """
            {
                "properties": {
                    "numeric_field": {
                        "type": "long"
                    },
                    "keyword_field": {
                        "type": "keyword"
                    }
                }
            }""";

        // Should succeed with derive source enabled and doc values enabled (default)
        assertAcked(
            prepareCreate("test_derive_1").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(basicMapping)
        );

        // Test 2: Validate mapping with doc values disabled
        String docValuesDisabledMapping = """
            {
                "properties": {
                    "numeric_field": {
                        "type": "long",
                        "doc_values": false
                    }
                }
            }""";

        // Should fail because doc values and stored are both disabled
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_2").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(docValuesDisabledMapping)
                .get()
        );

        // Test 3: Validate mapping with stored enabled but doc values disabled
        String storedEnabledMapping = """
            {
                "properties": {
                    "numeric_field": {
                        "type": "long",
                        "doc_values": false,
                        "store": true
                    }
                }
            }""";

        // Should succeed because stored is enabled
        assertAcked(
            prepareCreate("test_derive_3").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(storedEnabledMapping)
        );

        // Test 4: Validate keyword field with normalizer
        String normalizerMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword",
                        "normalizer": "lowercase"
                    }
                }
            }""";

        // Should fail because normalizer is not supported with derive source
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_4").setSettings(
                Settings.builder()
                    .put("analysis.normalizer.lowercase.type", "custom")
                    .put("index.derived_source.enabled", true)
                    .putList("analysis.normalizer.lowercase.filter", "lowercase")
            ).setMapping(normalizerMapping).get()
        );

        // Test 5: Validate keyword field with ignore_above
        String ignoreAboveMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            }""";

        // Should fail because ignore_above is not supported with derive source
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_5").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(ignoreAboveMapping)
                .get()
        );

        // Test 6: Validate object field with nested enabled
        String nestedMapping = """
            {
                "properties": {
                    "nested_field": {
                        "type": "nested",
                        "properties": {
                            "inner_field": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }""";

        // Should fail because nested fields are not supported with derive source
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_6").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(nestedMapping)
                .get()
        );

        // Test 7: Validate field with copy_to
        String copyToMapping = """
            {
                "properties": {
                    "field1": {
                        "type": "keyword",
                        "copy_to": "field2"
                    },
                    "field2": {
                        "type": "keyword"
                    }
                }
            }""";

        // Should fail because copy_to is not supported with derive source
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_7").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(copyToMapping)
                .get()
        );

        // Test 8: Validate multiple field types
        String multiTypeMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword"
                    },
                    "numeric_field": {
                        "type": "long"
                    },
                    "date_field": {
                        "type": "date"
                    },
                    "boolean_field": {
                        "type": "boolean"
                    },
                    "ip_field": {
                        "type": "ip"
                    },
                    "constant_keyword": {
                        "type": "constant_keyword",
                        "value": "1"
                    },
                    "geo_point_field": {
                        "type": "geo_point"
                    },
                    "text_field": {
                        "type": "text",
                        "store": true
                    },
                    "text_keyword_field": {
                        "type": "text",
                        "fields": {
                            "keyword_field": {
                                "type": "keyword"
                            }
                        }
                    },
                    "wildcard_field": {
                        "type": "wildcard",
                        "doc_values": true
                    }
                }
            }""";

        // Should succeed because all field types support derive source
        assertAcked(
            prepareCreate("test_derive_8").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(multiTypeMapping)
        );

        // Test 9: Validate with both doc_values and stored disabled
        String bothDisabledMapping = """
            {
                "properties": {
                    "keyword_field": {
                        "type": "keyword",
                        "doc_values": false,
                        "store": false
                    }
                }
            }""";

        // Should fail because both doc_values and stored are disabled
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_9").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(bothDisabledMapping)
                .get()
        );

        // Test 10: Validate for the field type, for which derived source is not implemented
        String unsupportedFieldType = """
            {
                "properties": {
                    "geo_shape_field": {
                        "type": "geo_shape",
                        "doc_values": true
                    }
                }
            }""";

        // Should fail because for geo_shape, derived source feature is not supported
        expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("test_derive_10").setSettings(Settings.builder().put("index.derived_source.enabled", true))
                .setMapping(unsupportedFieldType)
                .get()
        );
    }
}
