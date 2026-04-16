/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;

/**
 * Integration tests verifying that capability maps are correctly assigned to field types
 * when creating composite indices with pluggable data formats.
 * <p>
 * Each test creates an index, then inspects the actual {@link MappedFieldType#getCapabilityMap()}
 * to verify that the right data format owns the right capabilities for each field.
 */
@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/TBD - DataFormatRegistry not wired to DocumentMapper.Builder in composite path")
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
        // Get a data node that holds the index
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
     * Helper to extract format names from a capability map.
     */
    private Set<String> formatNames(Map<DataFormat, Set<Capability>> capMap) {
        return capMap.keySet().stream().map(DataFormat::name).collect(Collectors.toSet());
    }

    /**
     * Helper to get capabilities owned by a specific format name.
     */
    private Set<Capability> capsForFormat(Map<DataFormat, Set<Capability>> capMap, String formatName) {
        return capMap.entrySet()
            .stream()
            .filter(e -> e.getKey().name().equals(formatName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(Set.of());
    }

    // ---- Parquet-only tests ----

    public void testKeywordFieldParquetOnly() {
        String idx = "test-kw-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "status", "type=keyword");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "status");

        // Parquet is the only format — it should own all capabilities it supports for keyword
        // keyword defaults: isSearchable=true (FULL_TEXT_SEARCH), hasDocValues=true (COLUMNAR_STORAGE)
        // Parquet declares: COLUMNAR_STORAGE, BLOOM_FILTER for keyword
        // So Parquet wins COLUMNAR_STORAGE. FULL_TEXT_SEARCH is requested but Parquet doesn't support it — no one gets it.
        assertEquals("Only parquet should be in the map", Set.of(PARQUET), formatNames(capMap));
        Set<Capability> parquetCaps = capsForFormat(capMap, PARQUET);
        assertTrue("Parquet should own COLUMNAR_STORAGE", parquetCaps.contains(Capability.COLUMNAR_STORAGE));
        assertFalse(
            "FULL_TEXT_SEARCH should not be assigned (parquet doesn't support it)",
            parquetCaps.contains(Capability.FULL_TEXT_SEARCH)
        );
    }

    public void testIntegerFieldParquetOnly() {
        String idx = "test-int-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "count", "type=integer");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "count");

        // integer defaults: isSearchable=true (POINT_RANGE via override), hasDocValues=true (COLUMNAR_STORAGE)
        // Parquet declares: COLUMNAR_STORAGE, BLOOM_FILTER for integer
        assertEquals(Set.of(PARQUET), formatNames(capMap));
        Set<Capability> parquetCaps = capsForFormat(capMap, PARQUET);
        assertTrue("Parquet should own COLUMNAR_STORAGE", parquetCaps.contains(Capability.COLUMNAR_STORAGE));
        // POINT_RANGE is requested but Parquet doesn't declare it — should not be in the map
        assertFalse("POINT_RANGE should not be assigned", parquetCaps.contains(Capability.POINT_RANGE));
    }

    public void testDateFieldParquetOnly() {
        String idx = "test-date-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "timestamp", "type=date");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "timestamp");

        assertEquals(Set.of(PARQUET), formatNames(capMap));
        Set<Capability> parquetCaps = capsForFormat(capMap, PARQUET);
        assertTrue("Parquet should own COLUMNAR_STORAGE", parquetCaps.contains(Capability.COLUMNAR_STORAGE));
        assertFalse("POINT_RANGE should not be assigned", parquetCaps.contains(Capability.POINT_RANGE));
    }

    public void testTextFieldParquetOnly() {
        String idx = "test-text-parquet";
        createAndVerifyIndex(idx, parquetOnlySettings(), "description", "type=text");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "description");

        // text defaults: isSearchable=true (FULL_TEXT_SEARCH), hasDocValues=false, isStored=false
        // Parquet declares: COLUMNAR_STORAGE, BLOOM_FILTER for text — but FULL_TEXT_SEARCH is not declared
        // requestedCapabilities = {FULL_TEXT_SEARCH} — Parquet doesn't support it, so map may be empty
        // or Parquet gets COLUMNAR_STORAGE if it's in requested (it's not — text has no docValues)
        // Actually: text isSearchable=true, hasDocValues=false → requested = {FULL_TEXT_SEARCH}
        // Parquet doesn't support FULL_TEXT_SEARCH for text → empty map
        assertTrue("Text field with parquet-only should have empty capability map (no format supports FULL_TEXT_SEARCH)", capMap.isEmpty());
    }

    // ---- Parquet + Lucene tests: verify only one format wins each capability ----

    public void testKeywordFieldParquetWithLucene() {
        String idx = "test-kw-both";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "status", "type=keyword");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "status");

        // keyword requested: FULL_TEXT_SEARCH (isSearchable=true), COLUMNAR_STORAGE (hasDocValues=true)
        // Parquet (priority 0): supports COLUMNAR_STORAGE, BLOOM_FILTER
        // Lucene (priority 50): supports COLUMNAR_STORAGE, STORED_FIELDS
        // COLUMNAR_STORAGE: both support it, Parquet wins (lower priority)
        // FULL_TEXT_SEARCH: only Lucene supports it for keyword? Let's check...
        // Actually Lucene declares COLUMNAR_STORAGE + STORED_FIELDS for keyword, not FULL_TEXT_SEARCH
        // So FULL_TEXT_SEARCH is requested but no format supports it → not assigned
        assertThat("Parquet should be in the map", formatNames(capMap), hasItem(PARQUET));
        Set<Capability> parquetCaps = capsForFormat(capMap, PARQUET);
        assertTrue("Parquet should own COLUMNAR_STORAGE (lower priority wins)", parquetCaps.contains(Capability.COLUMNAR_STORAGE));

        // Lucene should NOT get COLUMNAR_STORAGE since Parquet won it
        Set<Capability> luceneCaps = capsForFormat(capMap, LUCENE);
        assertFalse("Lucene should NOT own COLUMNAR_STORAGE (Parquet won it)", luceneCaps.contains(Capability.COLUMNAR_STORAGE));
    }

    public void testTextFieldParquetWithLucene() {
        String idx = "test-text-both";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "description", "type=text");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "description");

        // text requested: FULL_TEXT_SEARCH (isSearchable=true)
        // Parquet: supports COLUMNAR_STORAGE, BLOOM_FILTER for text — not FULL_TEXT_SEARCH
        // Lucene: supports FULL_TEXT_SEARCH, STORED_FIELDS for text
        // FULL_TEXT_SEARCH → Lucene wins (only supporter)
        assertThat("Lucene should be in the map", formatNames(capMap), hasItem(LUCENE));
        Set<Capability> luceneCaps = capsForFormat(capMap, LUCENE);
        assertTrue("Lucene should own FULL_TEXT_SEARCH", luceneCaps.contains(Capability.FULL_TEXT_SEARCH));

        // Parquet should NOT be in the map — text doesn't request COLUMNAR_STORAGE (no docValues)
        assertFalse("Parquet should not be in the map for text", formatNames(capMap).contains(PARQUET));
    }

    public void testIntegerFieldParquetWithLucene() {
        String idx = "test-int-both";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "count", "type=integer");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "count");

        // integer requested: POINT_RANGE (isSearchable=true, override), COLUMNAR_STORAGE (hasDocValues=true)
        // Parquet: supports COLUMNAR_STORAGE, BLOOM_FILTER for integer
        // Lucene: does NOT declare support for integer at all
        // COLUMNAR_STORAGE → Parquet wins
        // POINT_RANGE → no format supports it → not assigned
        assertEquals(Set.of(PARQUET), formatNames(capMap));
        assertTrue("Parquet should own COLUMNAR_STORAGE", capsForFormat(capMap, PARQUET).contains(Capability.COLUMNAR_STORAGE));
    }

    // ---- Verify only one format wins when both support the same capability ----

    public void testOnlyOneFormatWinsSharedCapability() {
        String idx = "test-one-winner";
        createAndVerifyIndex(idx, parquetWithLuceneSettings(), "tag", "type=keyword");

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "tag");

        // Both Parquet and Lucene support COLUMNAR_STORAGE for keyword
        // Parquet (priority 0) should win — verify Lucene does NOT also get it
        for (Map.Entry<DataFormat, Set<Capability>> entry : capMap.entrySet()) {
            if (entry.getKey().name().equals(LUCENE)) {
                assertFalse(
                    "Lucene should NOT own COLUMNAR_STORAGE when Parquet has higher priority",
                    entry.getValue().contains(Capability.COLUMNAR_STORAGE)
                );
            }
        }
    }

    // ---- Negative: field with index:false should NOT get FULL_TEXT_SEARCH ----

    public void testFieldWithIndexFalseNoSearchCapability() {
        String idx = "test-no-index";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(parquetWithLuceneSettings())
            .setMapping("tag", "type=keyword,index=false")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(idx);

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "tag");

        // keyword with index:false → requestedCapabilities = {COLUMNAR_STORAGE} (no FULL_TEXT_SEARCH)
        for (Map.Entry<DataFormat, Set<Capability>> entry : capMap.entrySet()) {
            assertFalse("No format should own FULL_TEXT_SEARCH when index=false", entry.getValue().contains(Capability.FULL_TEXT_SEARCH));
        }
        // COLUMNAR_STORAGE should still be assigned (hasDocValues defaults to true)
        assertTrue("COLUMNAR_STORAGE should be assigned", capsForFormat(capMap, PARQUET).contains(Capability.COLUMNAR_STORAGE));
    }

    public void testFieldWithDocValuesFalseNoColumnarCapability() {
        String idx = "test-no-dv";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(parquetWithLuceneSettings())
            .setMapping("tag", "type=keyword,doc_values=false")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(idx);

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "tag");

        // keyword with doc_values:false → no COLUMNAR_STORAGE requested
        for (Map.Entry<DataFormat, Set<Capability>> entry : capMap.entrySet()) {
            assertFalse(
                "No format should own COLUMNAR_STORAGE when doc_values=false",
                entry.getValue().contains(Capability.COLUMNAR_STORAGE)
            );
        }
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

        Map<DataFormat, Set<Capability>> capMap = getCapabilityMap(idx, "name");
        assertTrue("Non-composite index should have empty capability map", capMap.isEmpty());
    }

    private void createAndVerifyIndex(String indexName, Settings settings, String... mapping) {
        CreateIndexResponse response = client().admin().indices().prepareCreate(indexName).setSettings(settings).setMapping(mapping).get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());
        ensureGreen(indexName);
    }
}
