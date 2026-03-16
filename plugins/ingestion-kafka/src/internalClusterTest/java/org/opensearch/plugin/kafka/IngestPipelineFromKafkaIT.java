/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.ingest.PipelineConfiguration;
import org.opensearch.ingest.common.IngestCommonModulePlugin;
import org.opensearch.painless.PainlessModulePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for ingest pipeline execution in pull-based Kafka ingestion.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IngestPipelineFromKafkaIT extends KafkaIngestionBaseIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(KafkaPlugin.class, IngestCommonModulePlugin.class, PainlessModulePlugin.class);
    }

    @After
    public void cleanUpPipelines() {
        try {
            GetPipelineResponse response = client().admin().cluster().getPipeline(new GetPipelineRequest("*")).actionGet();
            for (PipelineConfiguration pipeline : response.pipelines()) {
                client().admin().cluster().deletePipeline(new DeletePipelineRequest(pipeline.getId())).actionGet();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Test that a final_pipeline adds a field to documents ingested from Kafka.
     * Uses built-in "set" processor.
     */
    public void testFinalPipelineAddsField() throws Exception {
        createPipeline("add_field_pipeline", "{\"processors\": [{\"set\": {\"field\": \"processed\", \"value\": true}}]}");

        produceData("1", "alice", "25");
        produceData("2", "bob", "30");

        createIndexWithPipeline("add_field_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).get();
            if (response.getHits().getTotalHits().value() < 2) return false;
            Map<String, Object> source1 = response.getHits().getHits()[0].getSourceAsMap();
            Map<String, Object> source2 = response.getHits().getHits()[1].getSourceAsMap();
            return Boolean.TRUE.equals(source1.get("processed")) && Boolean.TRUE.equals(source2.get("processed"));
        });
    }

    /**
     * Test that a pipeline that drops documents results in no indexed documents.
     * Uses built-in "drop" processor.
     */
    public void testFinalPipelineDropsDocument() throws Exception {
        createPipeline("drop_pipeline", "{\"processors\": [{\"drop\": {}}]}");

        produceData("1", "alice", "25");
        produceData("2", "bob", "30");

        createIndexWithPipeline("drop_pipeline", 1, 0);

        Thread.sleep(5000);
        refresh(indexName);
        SearchResponse response = client().prepareSearch(indexName).get();
        assertThat(response.getHits().getTotalHits().value(), is(0L));
    }

    /**
     * Test that documents are indexed normally when no pipeline is configured.
     */
    public void testNoPipelineConfigured() throws Exception {
        produceData("1", "alice", "25");
        produceData("2", "bob", "30");

        // Create index without pipeline (default settings)
        createIndexWithDefaultSettings(1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).get();
            return response.getHits().getTotalHits().value() == 2;
        });

        // Verify documents have original fields and no pipeline-added fields
        SearchResponse response = client().prepareSearch(indexName).get();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            Map<String, Object> source = response.getHits().getHits()[i].getSourceAsMap();
            assertFalse("Document should not have 'processed' field", source.containsKey("processed"));
        }
    }

    /**
     * Test that pipeline does NOT execute for delete operations.
     */
    public void testPipelineNotCalledForDeletes() throws Exception {
        createPipeline("add_field_pipeline", "{\"processors\": [{\"set\": {\"field\": \"processed\", \"value\": true}}]}");

        // Produce an index message, then a delete message
        produceData("1", "alice", "25", defaultMessageTimestamp, "index");
        createIndexWithPipeline("add_field_pipeline", 1, 0);

        // Wait for the document to be indexed
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            return response.getHits().getTotalHits().value() == 1;
        });

        // Now delete the document
        produceData("1", "alice", "25", defaultMessageTimestamp, "delete");

        // Verify document is deleted
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            return response.getHits().getTotalHits().value() == 0;
        });
    }

    /**
     * Test pipeline that renames a field.
     * Uses built-in "rename" processor.
     */
    public void testPipelineRenamesField() throws Exception {
        createPipeline("rename_pipeline", "{\"processors\": [{\"rename\": {\"field\": \"name\", \"target_field\": \"full_name\"}}]}");

        produceData("1", "alice", "25");
        createIndexWithPipeline("rename_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "alice".equals(source.get("full_name")) && !source.containsKey("name");
        });
    }

    /**
     * Test that guardrails block a pipeline attempting to change _id.
     * The message should fail and be handled by the error strategy (retried then dropped with DROP strategy).
     */
    public void testPipelineMutatingIdIsBlocked() throws Exception {
        createPipeline("mutate_id_pipeline", "{\"processors\": [{\"script\": {\"source\": \"ctx._id = 'mutated_id'\"}}]}");

        produceData("1", "alice", "25");
        createIndexWithPipeline("mutate_id_pipeline", 1, 0);

        // With DROP error strategy, the message should eventually be dropped after retries
        // Verify no document is indexed (pipeline fails, message dropped)
        Thread.sleep(10000);
        refresh(indexName);
        SearchResponse response = client().prepareSearch(indexName).get();
        assertThat(response.getHits().getTotalHits().value(), is(0L));
    }

    /**
     * Test that pipeline execution works with multiple documents.
     * Verifies that the pipeline independently processes each document.
     */
    public void testPipelineWithMultipleDocuments() throws Exception {
        createPipeline("add_field_pipeline", "{\"processors\": [{\"set\": {\"field\": \"pipeline_version\", \"value\": \"v1\"}}]}");

        // Produce multiple documents
        for (int i = 0; i < 10; i++) {
            produceData(String.valueOf(i), "user" + i, String.valueOf(20 + i));
        }

        createIndexWithPipeline("add_field_pipeline", 1, 0);

        // Verify all documents are indexed with the pipeline field
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).get();
            if (response.getHits().getTotalHits().value() < 10) return false;
            // Verify all docs have the pipeline field
            for (int i = 0; i < response.getHits().getHits().length; i++) {
                if (!"v1".equals(response.getHits().getHits()[i].getSourceAsMap().get("pipeline_version"))) {
                    return false;
                }
            }
            return true;
        });
    }

    /**
     * Test dynamic update of the final_pipeline setting.
     * Documents ingested before the update should use the old pipeline,
     * documents after should use the new pipeline.
     */
    public void testDynamicPipelineUpdate() throws Exception {
        createPipeline("pipeline_v1", "{\"processors\": [{\"set\": {\"field\": \"version\", \"value\": \"v1\"}}]}");
        createPipeline("pipeline_v2", "{\"processors\": [{\"set\": {\"field\": \"version\", \"value\": \"v2\"}}]}");

        // Produce the first batch and create an index with v1 pipeline
        produceData("1", "Alice", "25");
        createIndexWithPipeline("pipeline_v1", 1, 0);

        // Wait for the first document to be indexed with v1
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            return "v1".equals(response.getHits().getHits()[0].getSourceAsMap().get("version"));
        });

        // Dynamically update pipeline to v2
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "pipeline_v2"))
                .get()
        );

        // Produce second batch
        produceData("2", "bob", "30");

        // Verify second document uses v2 pipeline
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "2")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            return "v2".equals(response.getHits().getHits()[0].getSourceAsMap().get("version"));
        });
    }

    /**
     * Test removing final_pipeline dynamically (set to _none).
     * Documents after removal should not have pipeline-added fields.
     */
    public void testRemovePipelineDynamically() throws Exception {
        createPipeline("add_field_pipeline", "{\"processors\": [{\"set\": {\"field\": \"processed\", \"value\": true}}]}");

        produceData("1", "alice", "25");
        createIndexWithPipeline("add_field_pipeline", 1, 0);

        // Wait for first document with pipeline
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            return Boolean.TRUE.equals(response.getHits().getHits()[0].getSourceAsMap().get("processed"));
        });

        // Remove pipeline
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "_none"))
                .get()
        );

        // Produce document after pipeline removal
        produceData("2", "bob", "30");

        // Verify the second document does NOT have the pipeline field
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "2")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            return !response.getHits().getHits()[0].getSourceAsMap().containsKey("processed");
        });
    }

    /**
     * Test pipeline with field_mapping mapper type combined.
     * Raw Kafka message (no envelope) → field_mapping extracts _id → pipeline transforms → Lucene.
     */
    public void testPipelineWithFieldMappingMapper() throws Exception {
        createPipeline("enrich_pipeline", "{\"processors\": [{\"set\": {\"field\": \"enriched\", \"value\": true}}]}");

        // Produce raw messages (no _id/_source envelope — field_mapping mapper handles it)
        String rawPayload1 = "{\"user_id\": \"user1\", \"name\": \"alice\", \"age\": 25}";
        String rawPayload2 = "{\"user_id\": \"user2\", \"name\": \"bob\", \"age\": 30}";
        produceData(rawPayload1);
        produceData(rawPayload2);

        // Create index with field_mapping mapper + final_pipeline
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .put("ingestion_source.mapper_type", "field_mapping")
                .put("ingestion_source.mapper_settings.id_field", "user_id")
                .put(IndexSettings.FINAL_PIPELINE.getKey(), "enrich_pipeline")
                .build(),
            "{\"properties\":{\"name\":{\"type\": \"text\"},\"age\":{\"type\": \"integer\"}}}"
        );

        // Verify documents are indexed with field_mapping-extracted ID and pipeline-added field
        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "user1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return Boolean.TRUE.equals(source.get("enriched")) && "alice".equals(source.get("name"));
        });

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "user2")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return Boolean.TRUE.equals(source.get("enriched")) && "bob".equals(source.get("name"));
        });
    }

    // --- Transformation-specific tests ---

    /**
     * RENAME: Rename a field from one name to another.
     * Uses built-in "rename" processor.
     */
    public void testTransformRename() throws Exception {
        createPipeline("rename_pipeline2", "{\"processors\": [{\"rename\": {\"field\": \"name\", \"target_field\": \"full_name\"}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"age\":25},\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("rename_pipeline2", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "alice".equals(source.get("full_name")) && !source.containsKey("name");
        });
    }

    /**
     * COPY: Copy a field value to a new field, keeping the original.
     * Uses built-in "copy" processor.
     */
    public void testTransformCopy() throws Exception {
        createPipeline("copy_pipeline", "{\"processors\": [{\"copy\": {\"source_field\": \"name\", \"target_field\": \"name_copy\"}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"age\":25},\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("copy_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "alice".equals(source.get("name")) && "alice".equals(source.get("name_copy"));
        });
    }

    /**
     * STRING_TO_JSON: Parse a JSON string field into a structured object.
     * Uses built-in "json" processor.
     */
    public void testTransformStringToJson() throws Exception {
        createPipeline("string_to_json_pipeline", "{\"processors\": [{\"json\": {\"field\": \"metadata\"}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"metadata\":\"{\\\"key\\\":\\\"value\\\",\\\"count\\\":42}\"},"
            + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("string_to_json_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            Object metadata = source.get("metadata");
            if (!(metadata instanceof Map)) return false;
            @SuppressWarnings("unchecked")
            Map<String, Object> metadataMap = (Map<String, Object>) metadata;
            return "value".equals(metadataMap.get("key")) && Integer.valueOf(42).equals(metadataMap.get("count"));
        });
    }

    /**
     * STRING_TO_LONG: Convert a string field to a long value.
     * Uses built-in "convert" processor with type "long".
     */
    public void testTransformStringToLong() throws Exception {
        createPipeline("string_to_long_pipeline", "{\"processors\": [{\"convert\": {\"field\": \"timestamp\", \"type\": \"long\"}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"timestamp\":\"1739459500000\"},\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("string_to_long_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            Object ts = source.get("timestamp");
            return ts instanceof Number && ((Number) ts).longValue() == 1739459500000L;
        });
    }

    /**
     * TYPE_CONVERSION: Convert a string field to integer.
     * Uses built-in "convert" processor with type "integer".
     */
    public void testTransformTypeConversion() throws Exception {
        createPipeline("type_conversion_pipeline", "{\"processors\": [{\"convert\": {\"field\": \"age\", \"type\": \"integer\"}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"age\":\"25\"},\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("type_conversion_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return source.get("age") instanceof Integer && (Integer) source.get("age") == 25;
        });
    }

    /**
     * GEO: Combine lat/lon fields into a geo_point object.
     * Uses built-in "script" processor with Painless.
     */
    public void testTransformGeo() throws Exception {
        createPipeline(
            "geo_pipeline",
            "{\"processors\": [{\"script\": {\"source\": "
                + "\"ctx.location = ['lat': ctx.lat, 'lon': ctx.lon]; ctx.remove('lat'); ctx.remove('lon')\"}}]}"
        );

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"lat\":40.7128,\"lon\":-74.006},\"_op_type\":\"index\"}";
        produceData(payload);

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .put("ingestion_source.error_strategy", "drop")
                .put(IndexSettings.FINAL_PIPELINE.getKey(), "geo_pipeline")
                .build(),
            "{\"properties\":{\"name\":{\"type\":\"text\"},\"location\":{\"type\":\"geo_point\"}}}"
        );

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            Object location = source.get("location");
            if (!(location instanceof Map)) return false;
            @SuppressWarnings("unchecked")
            Map<String, Object> geoPoint = (Map<String, Object>) location;
            return geoPoint.containsKey("lat") && geoPoint.containsKey("lon") && !source.containsKey("lat") && !source.containsKey("lon");
        });
    }

    /**
     * EXCLUDE_EMPTY_VALUES: Remove fields with null or empty string values.
     * Uses built-in "script" processor with Painless.
     */
    public void testTransformExcludeEmptyValues() throws Exception {
        createPipeline(
            "exclude_empty_pipeline",
            "{\"processors\": [{\"script\": {\"source\": "
                + "\"ctx.entrySet().removeIf(e -> e.getValue() == null "
                + "|| (e.getValue() instanceof String && e.getValue().isEmpty()))\"}}]}"
        );

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"nickname\":\"\",\"bio\":null,\"age\":25},"
            + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("exclude_empty_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "alice".equals(source.get("name"))
                && !source.containsKey("nickname")
                && !source.containsKey("bio")
                && Integer.valueOf(25).equals(source.get("age"));
        });
    }

    /**
     * MAP_KEYS: Rename keys in a nested map object.
     * Uses built-in "script" processor with Painless.
     */
    public void testTransformMapKeys() throws Exception {
        createPipeline(
            "map_keys_pipeline",
            "{\"processors\": [{\"script\": {\"source\": "
                + "\"def map = ctx.props; if (map.containsKey('old_key')) { map.put('new_key', map.remove('old_key')); }\"}}]}"
        );

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"props\":{\"old_key\":\"value1\",\"keep_key\":\"value2\"}},"
            + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("map_keys_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> props = (Map<String, Object>) source.get("props");
            if (props == null) return false;
            return "value1".equals(props.get("new_key")) && !props.containsKey("old_key") && "value2".equals(props.get("keep_key"));
        });
    }

    /**
     * DROP_FIELD: Remove specific fields from the document.
     * Uses built-in "remove" processor.
     */
    public void testTransformDropField() throws Exception {
        createPipeline("drop_field_pipeline", "{\"processors\": [{\"remove\": {\"field\": [\"internal_id\", \"debug\"]}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"age\":25,\"internal_id\":\"xyz\",\"debug\":true},"
            + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("drop_field_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "alice".equals(source.get("name"))
                && Integer.valueOf(25).equals(source.get("age"))
                && !source.containsKey("internal_id")
                && !source.containsKey("debug");
        });
    }

    /**
     * NESTED_FIELD_EXTRACT: Extract a value from a nested object to a top-level field.
     * Uses built-in "script" processor with Painless.
     */
    public void testTransformNestedFieldExtract() throws Exception {
        createPipeline("nested_extract_pipeline", "{\"processors\": [{\"script\": {\"source\": \"ctx.city = ctx.address.city\"}}]}");

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"address\":{\"city\":\"New York\",\"zip\":\"10001\"}},"
            + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("nested_extract_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "New York".equals(source.get("city")) && source.containsKey("address");
        });
    }

    /**
     * FLATTEN_MAP: Flatten a nested map into dot-notation top-level fields.
     * Uses built-in "script" processor with Painless.
     */
    public void testTransformFlattenMap() throws Exception {
        createPipeline(
            "flatten_pipeline",
            "{\"processors\": [{\"script\": {\"source\": \""
                + "def map = ctx.remove('metadata'); "
                + "for (entry in map.entrySet()) { "
                + "  if (entry.getValue() instanceof Map) { "
                + "    for (inner in entry.getValue().entrySet()) { "
                + "      ctx['metadata.' + entry.getKey() + '.' + inner.getKey()] = inner.getValue(); "
                + "    } "
                + "  } else { "
                + "    ctx['metadata.' + entry.getKey()] = entry.getValue(); "
                + "  } "
                + "}\"}}]}"
        );

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\","
            + "\"metadata\":{\"source\":\"kafka\",\"env\":\"prod\",\"nested\":{\"deep\":\"value\"}}},"
            + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("flatten_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "kafka".equals(source.get("metadata.source"))
                && "prod".equals(source.get("metadata.env"))
                && "value".equals(source.get("metadata.nested.deep"))
                && !source.containsKey("metadata");
        });
    }

    /**
     * Combined transformations: RENAME + DROP_FIELD + ADD_FIELD in a single pipeline.
     * Uses built-in "rename", "remove", and "set" processors.
     */
    public void testCombinedTransformations() throws Exception {
        createPipeline(
            "combined_pipeline",
            "{\"processors\": ["
                + "{\"rename\": {\"field\": \"name\", \"target_field\": \"full_name\"}},"
                + "{\"remove\": {\"field\": \"internal_id\"}},"
                + "{\"set\": {\"field\": \"processed_by\", \"value\": \"opensearch\"}}"
                + "]}"
        );

        String payload = "{\"_id\":\"1\",\"_source\":{\"name\":\"alice\",\"age\":25,\"internal_id\":\"xyz\"}," + "\"_op_type\":\"index\"}";
        produceData(payload);
        createIndexWithPipeline("combined_pipeline", 1, 0);

        waitForState(() -> {
            refresh(indexName);
            SearchResponse response = client().prepareSearch(indexName).setQuery(new TermQueryBuilder("_id", "1")).get();
            if (response.getHits().getTotalHits().value() < 1) return false;
            Map<String, Object> source = response.getHits().getHits()[0].getSourceAsMap();
            return "alice".equals(source.get("full_name"))
                && !source.containsKey("name")
                && !source.containsKey("internal_id")
                && "opensearch".equals(source.get("processed_by"))
                && Integer.valueOf(25).equals(source.get("age"));
        });
    }

    // --- Helper methods ---

    private void createPipeline(String pipelineId, String pipelineJson) {
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest(pipelineId, new BytesArray(pipelineJson), MediaTypeRegistry.JSON))
            .actionGet();
    }

    private void createIndexWithPipeline(String pipelineId, int numShards, int numReplicas) {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put("ingestion_source.type", "kafka")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.topic", topicName)
                .put("ingestion_source.param.bootstrap_servers", kafka.getBootstrapServers())
                .put("index.replication.type", "SEGMENT")
                .put("ingestion_source.error_strategy", "drop")
                .put(IndexSettings.FINAL_PIPELINE.getKey(), pipelineId)
                .build(),
            mapping
        );
    }

}
