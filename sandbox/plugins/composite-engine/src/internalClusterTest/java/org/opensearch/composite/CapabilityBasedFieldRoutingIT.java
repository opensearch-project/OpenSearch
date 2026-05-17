/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Integration tests verifying capability-based field routing for composite indices.
 * <p>
 * Validation rule: a field's full set of requested capabilities must be served by exactly one
 * configured data format. The system walks the index's configured formats in priority-walk order
 * (primary first, then secondaries by priority ascending) and selects the first format whose
 * {@code supportedFields()} declares support for every requested capability for the field's type.
 * If no single configured format covers the full set, {@link MapperParsingException} is thrown
 * at index creation time.
 * <p>
 * Each test creates an index, then either
 * <ul>
 *   <li>Asserts on the {@link MappedFieldType#getCapabilityMap()} structure for the covering
 *       format, or
 *   <li>Asserts that index creation fails with the expected message when no single format covers
 *       the field's requested capabilities.
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CapabilityBasedFieldRoutingIT extends OpenSearchIntegTestCase {

    private static final String PARQUET = "parquet";
    private static final String LUCENE = "lucene";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings parquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", PARQUET)
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings parquetWithLuceneSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", PARQUET)
            .putList("index.composite.secondary_data_formats", LUCENE)
            .build();
    }

    /**
     * Helper to get the capability map for a field from the index's MapperService.
     */
    private Map<DataFormat, Set<Capability>> getCapabilityMap(String indexName, String fieldName) {
        Set<String> dataNodes = internalCluster().getDataNodeNames();
        assertFalse("Should have at least one data node", dataNodes.isEmpty());
        String node = dataNodes.iterator().next();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        MappedFieldType fieldType = indexService.mapperService().fieldType(fieldName);
        assertNotNull("Field [" + fieldName + "] should exist in mapping", fieldType);
        return fieldType.getCapabilityMap();
    }

    /**
     * Asserts that the capability map contains exactly one entry, owned by the named format,
     * and that the owned capability set equals the expected set.
     */
    private void assertSingleFormatOwns(Map<DataFormat, Set<Capability>> capMap, String expectedFormatName, Set<Capability> expectedCaps) {
        assertEquals("Capability map should contain exactly one format entry", 1, capMap.size());
        DataFormat owner = capMap.keySet().iterator().next();
        assertEquals("Capability map should be owned by [" + expectedFormatName + "]", expectedFormatName, owner.name());
        assertEquals("Owned capabilities should match requested", expectedCaps, capMap.get(owner));
    }

    // ---- Parquet-only configured: full default mapping rejected for unsupported capabilities ----

    /**
     * Parquet supports {@code COLUMNAR_STORAGE, BLOOM_FILTER} for keyword. With default mapping
     * ({@code index:true, doc_values:true}) the field requests {@code FULL_TEXT_SEARCH +
     * COLUMNAR_STORAGE}. Parquet doesn't cover {@code FULL_TEXT_SEARCH} and Lucene isn't a
     * configured secondary, so index creation must fail.
     */
    public void testKeywordFieldRejectedOnParquetOnly() {
        String idx = "test-kw-parquet-rejected";
        Exception ex = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(idx)
                .setSettings(parquetOnlySettings())
                .setMapping("status", "type=keyword")
                .get()
        );
        assertTrue(
            "expected coverage error mentioning the field, got: " + ex.getMessage(),
            ex.getMessage().contains("status") && ex.getMessage().contains("no single configured data format")
        );
    }

    /**
     * Parquet doesn't support {@code POINT_RANGE} for integer, and Lucene isn't configured.
     * Default integer mapping requests {@code POINT_RANGE + COLUMNAR_STORAGE}, so index creation
     * must fail.
     */
    public void testIntegerFieldRejectedOnParquetOnly() {
        String idx = "test-int-parquet-rejected";
        Exception ex = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareCreate(idx).setSettings(parquetOnlySettings()).setMapping("count", "type=integer").get()
        );
        assertTrue(
            "expected coverage error mentioning the field, got: " + ex.getMessage(),
            ex.getMessage().contains("count") && ex.getMessage().contains("no single configured data format")
        );
    }

    /**
     * Parquet doesn't support {@code FULL_TEXT_SEARCH} for text. Default text mapping requests
     * {@code FULL_TEXT_SEARCH}, so index creation must fail when only Parquet is configured.
     */
    public void testTextFieldRejectedOnParquetOnly() {
        String idx = "test-text-parquet-rejected";
        Exception ex = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareCreate(idx)
                .setSettings(parquetOnlySettings())
                .setMapping("description", "type=text")
                .get()
        );
        assertTrue(
            "expected coverage error mentioning the field, got: " + ex.getMessage(),
            ex.getMessage().contains("description") && ex.getMessage().contains("no single configured data format")
        );
    }

    // ---- Parquet-only configured: doc_values-only mappings should pass and route to Parquet ----

    public void testKeywordDocValuesOnlyOnParquetOnly() {
        String idx = "test-kw-dv-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "status", "type=keyword,index=false,doc_values=true");

        assertSingleFormatOwns(getCapabilityMap(idx, "status"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testIntegerDocValuesOnlyOnParquetOnly() {
        String idx = "test-int-dv-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "count", "type=integer,index=false,doc_values=true");

        assertSingleFormatOwns(getCapabilityMap(idx, "count"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testDateDocValuesOnlyOnParquetOnly() {
        String idx = "test-date-dv-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "timestamp", "type=date,index=false,doc_values=true");

        assertSingleFormatOwns(getCapabilityMap(idx, "timestamp"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    // ---- Parquet + Lucene configured: full mapping passes, routes to single covering format ----

    /**
     * Both formats are configured. {@code keyword} with defaults requests
     * {@code FULL_TEXT_SEARCH + COLUMNAR_STORAGE}. Walk: Parquet (primary) doesn't cover
     * {@code FULL_TEXT_SEARCH} → reject; Lucene covers both → wins. Single-format coverage
     * means Lucene owns both capabilities.
     */
    public void testKeywordRoutedToLuceneOnParquetWithLucene() {
        String idx = "test-kw-routed-lucene";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "status", "type=keyword");

        assertSingleFormatOwns(getCapabilityMap(idx, "status"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE));
    }

    /**
     * {@code text} with defaults requests {@code FULL_TEXT_SEARCH}. Parquet doesn't cover it;
     * Lucene does. Lucene wins.
     */
    public void testTextRoutedToLuceneOnParquetWithLucene() {
        String idx = "test-text-routed-lucene";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "description", "type=text");

        assertSingleFormatOwns(getCapabilityMap(idx, "description"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH));
    }

    /**
     * {@code integer} with defaults requests {@code POINT_RANGE + COLUMNAR_STORAGE}. Parquet
     * doesn't cover {@code POINT_RANGE}; Lucene covers both. Lucene wins.
     */
    public void testIntegerRoutedToLuceneOnParquetWithLucene() {
        String idx = "test-int-routed-lucene";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "count", "type=integer");

        assertSingleFormatOwns(getCapabilityMap(idx, "count"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    // ---- Different fields can pick different formats in the same index ----

    /**
     * Verifies that within a single composite index, fields with different capability needs
     * route to different formats: a doc_values-only field stays on Parquet (primary, lower
     * priority), while a fully-indexed field falls through to Lucene.
     */
    public void testDifferentFieldsPickDifferentFormats() {
        String idx = "test-mixed-routing";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(parquetWithLuceneSettings())
            .setMapping("tag", "type=keyword,index=false,doc_values=true", "title", "type=keyword,index=true,doc_values=true")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(idx);

        // tag → only COLUMNAR_STORAGE requested → Parquet (primary) covers → Parquet wins.
        assertSingleFormatOwns(getCapabilityMap(idx, "tag"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));

        // title → FULL_TEXT_SEARCH + COLUMNAR_STORAGE requested → Parquet rejects → Lucene wins.
        assertSingleFormatOwns(getCapabilityMap(idx, "title"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE));
    }

    /**
     * Doc-values-only keyword on Parquet+Lucene goes to Parquet because Parquet has lower
     * priority and covers the requested set. A field is never split — Lucene does not also
     * appear in the capability map.
     */
    public void testKeywordDocValuesOnlyPicksPrimary() {
        String idx = "test-kw-dv-primary";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "tag", "type=keyword,index=false,doc_values=true");

        assertSingleFormatOwns(getCapabilityMap(idx, "tag"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    // ---- Negative settings: empty requested capabilities yields empty map ----

    /**
     * A field with all of {@code index:false, doc_values:false, store:false} requests no
     * capabilities. Validation is skipped and the capability map is empty.
     */
    public void testEmptyRequestedCapabilitiesYieldsEmptyMap() {
        String idx = "test-empty-caps";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "ignored", "type=keyword,index=false,doc_values=false,store=false");

        assertTrue("Capability map should be empty when no capabilities are requested", getCapabilityMap(idx, "ignored").isEmpty());
    }

    // ---- Non-composite index: capability map should be empty ----

    public void testNonCompositeIndexHasEmptyCapabilityMap() {
        String idx = "test-standard";
        Settings standardSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(standardSettings)
            .setMapping("name", "type=keyword")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(idx);

        assertTrue("Non-composite index should have empty capability map", getCapabilityMap(idx, "name").isEmpty());
    }

    private void createAndVerifyIndex(String indexName, Settings settings, String... mapping) {
        CreateIndexResponse response = client().admin().indices().prepareCreate(indexName).setSettings(settings).setMapping(mapping).get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());
        ensureGreen(indexName);
    }
}
