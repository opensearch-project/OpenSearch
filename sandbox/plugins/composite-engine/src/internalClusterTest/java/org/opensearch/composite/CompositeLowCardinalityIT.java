/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end tests for the {@code low_cardinality} mapping parameter contributed by the Parquet data-format plugin.
 *
 * <p>Exercises the full path in a live cluster: the plugin contributes the parameter through
 * {@code DataFormatRegistry} / {@code CompositeDataFormatPlugin} fan-out, the core keyword/text mappers accept it,
 * its build-time side effect flips {@code index} to {@code false}, and {@code ParquetIndexCreationValidator} gates
 * where it may be used. Also verifies it is unavailable on indices that do not use the Parquet format.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeLowCardinalityIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-low-cardinality";

    /**
     * On a parquet-primary composite index, {@code low_cardinality: true} on keyword and text fields is accepted,
     * round-trips in the mapping, and its side effect sets {@code index: false}. A field without the parameter is
     * left untouched. Documents still index successfully through the write path.
     */
    public void testLowCardinalityAppliedOnParquetPrimary() throws IOException {
        createLowCardinalityIndex("parquet", List.of("lucene"));
        ensureGreen(INDEX_NAME);

        Map<String, Object> properties = getMappingProperties(INDEX_NAME);

        Map<String, Object> city = fieldMapping(properties, "city");
        assertEquals("keyword", city.get("type"));
        assertEquals("low_cardinality should round-trip", Boolean.TRUE, city.get("low_cardinality"));
        assertEquals("side effect should disable indexing", Boolean.FALSE, city.get("index"));

        Map<String, Object> message = fieldMapping(properties, "message");
        assertEquals("text", message.get("type"));
        assertEquals(Boolean.TRUE, message.get("low_cardinality"));
        assertEquals(Boolean.FALSE, message.get("index"));

        // Control field: no low_cardinality → parameter absent and indexing not disabled.
        Map<String, Object> userId = fieldMapping(properties, "user_id");
        assertNull("control field must not expose low_cardinality", userId.get("low_cardinality"));
        assertNotEquals("control field indexing must not be disabled", Boolean.FALSE, userId.get("index"));

        // Write path still works with the suppressed-indexing fields.
        indexLowCardinalityDocs(INDEX_NAME, 5);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);
    }

    /**
     * {@code low_cardinality} works the same when the mapping value is supplied without the field being disabled by
     * the user: omitting the parameter leaves the keyword field indexed as usual.
     */
    public void testLowCardinalityDefaultsToDisabledParameter() throws IOException {
        Settings.Builder settings = compositeSettings("parquet", List.of("lucene"));
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("city")
            .field("type", "keyword")
            .field("low_cardinality", false)
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping(mapping).get();
        ensureGreen(INDEX_NAME);

        Map<String, Object> city = fieldMapping(getMappingProperties(INDEX_NAME), "city");
        // low_cardinality=false → no side effect, indexing not disabled.
        assertNotEquals(Boolean.FALSE, city.get("index"));
    }

    /**
     * When Parquet is only a secondary format (primary is lucene), the parameter parses but the index-creation
     * validator rejects it, because field-level Parquet settings require Parquet to be the primary format.
     */
    public void testLowCardinalityRejectedWhenParquetNotPrimary() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createLowCardinalityIndex("lucene", List.of("parquet"))
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("does not use parquet data format"));
    }

    /**
     * On a plain index that does not use the pluggable data format at all, the parameter is never contributed, so the
     * mapping fails to parse with an "unknown parameter" error.
     */
    public void testLowCardinalityUnknownOnPlainIndex() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(settings)
                .setMapping("city", "type=keyword,low_cardinality=true")
                .get()
        );
        assertTrue("unexpected message: " + e.getMessage(), e.getMessage().contains("unknown parameter [low_cardinality]"));
    }

    // --- Helpers ---

    private Settings.Builder compositeSettings(String primary, List<String> secondaries) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", primary);
        if (secondaries.isEmpty()) {
            builder.putList("index.composite.secondary_data_formats");
        } else {
            builder.putList("index.composite.secondary_data_formats", secondaries.toArray(new String[0]));
        }
        return builder;
    }

    private void createLowCardinalityIndex(String primary, List<String> secondaries) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("city")
            .field("type", "keyword")
            .field("low_cardinality", true)
            .endObject()
            .startObject("message")
            .field("type", "text")
            .field("low_cardinality", true)
            .endObject()
            .startObject("user_id")
            .field("type", "keyword")
            .endObject()
            .startObject("value")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(compositeSettings(primary, secondaries)).setMapping(mapping).get();
    }

    private void indexLowCardinalityDocs(String indexName, int count) {
        for (int i = 0; i < count; i++) {
            RestStatus status = client().prepareIndex()
                .setIndex(indexName)
                .setSource("city", "city_" + i, "message", "message body " + i, "user_id", "user_" + i, "value", i)
                .get()
                .status();
            assertEquals(RestStatus.CREATED, status);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMappingProperties(String indexName) {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(indexName).get();
        MappingMetadata mappingMetadata = response.getMappings().get(indexName);
        assertNotNull("mapping metadata must exist for " + indexName, mappingMetadata);
        Map<String, Object> source = mappingMetadata.getSourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) source.get("properties");
        assertNotNull("mapping must contain properties", properties);
        return properties;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fieldMapping(Map<String, Object> properties, String field) {
        Map<String, Object> mapping = (Map<String, Object>) properties.get(field);
        assertNotNull("field [" + field + "] must be present in mapping", mapping);
        return mapping;
    }
}
