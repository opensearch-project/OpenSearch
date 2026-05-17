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
 * Comprehensive matrix coverage for capability-based field routing across every user-facing
 * field type that Parquet declares support for. Each test fixes a (field type, mapping config,
 * composite config) cell and asserts either:
 * <ul>
 *   <li>The capability map after index creation contains exactly one entry, owned by the expected
 *       format, with the expected capability set; or</li>
 *   <li>Index creation fails with {@link org.opensearch.index.mapper.MapperParsingException}
 *       when no single configured format covers the field's requested capabilities.</li>
 * </ul>
 *
 * <p>Two composite configurations are exercised:
 * <ul>
 *   <li><b>Parquet-only</b>: {@code primary=parquet, secondaries=[]}. Parquet supports
 *       {@code COLUMNAR_STORAGE, BLOOM_FILTER} for every declared field type. Any mapping
 *       requesting {@code FULL_TEXT_SEARCH}, {@code POINT_RANGE}, or {@code STORED_FIELDS} fails.</li>
 *   <li><b>Parquet + Lucene</b>: {@code primary=parquet, secondaries=[lucene]}. Walk: parquet first
 *       (priority 0), then lucene (priority 50). The first format whose {@code supportedFields()}
 *       covers the field's full requested capability set wins.</li>
 * </ul>
 *
 * <p>Field types covered: text, match_only_text, keyword, integer, long, short, byte, float,
 * half_float, double, unsigned_long, date, date_nanos, boolean, ip, binary. Skipped:
 * {@code scaled_float} and {@code token_count} live in the {@code mapper-extras} module which is
 * not loaded into this composite-engine test cluster.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class FieldTypeCapabilityMatrixIT extends OpenSearchIntegTestCase {

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

    private Settings parquetOnly() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", PARQUET)
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings parquetWithLucene() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", PARQUET)
            .putList("index.composite.secondary_data_formats", LUCENE)
            .build();
    }

    private Map<DataFormat, Set<Capability>> capMap(String indexName, String fieldName) {
        Set<String> dataNodes = internalCluster().getDataNodeNames();
        assertFalse("Should have at least one data node", dataNodes.isEmpty());
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNodes.iterator().next());
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        MappedFieldType ft = indexService.mapperService().fieldType(fieldName);
        assertNotNull("Field [" + fieldName + "] should exist", ft);
        return ft.getCapabilityMap();
    }

    private void assertOwnedBy(Map<DataFormat, Set<Capability>> cm, String formatName, Set<Capability> expectedCaps) {
        assertEquals("Capability map should contain exactly one format entry", 1, cm.size());
        DataFormat owner = cm.keySet().iterator().next();
        assertEquals("Capability map owner mismatch", formatName, owner.name());
        assertEquals("Owned capability set mismatch", expectedCaps, cm.get(owner));
    }

    private void create(String idx, Settings settings, String fieldName, String typeSpec) {
        CreateIndexResponse r = client().admin().indices().prepareCreate(idx).setSettings(settings).setMapping(fieldName, typeSpec).get();
        assertTrue("Index creation should be acknowledged", r.isAcknowledged());
        ensureGreen(idx);
    }

    private void expectCoverageFailure(String idx, Settings settings, String fieldName, String typeSpec) {
        Exception ex = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareCreate(idx).setSettings(settings).setMapping(fieldName, typeSpec).get()
        );
        assertTrue(
            "Expected coverage error mentioning [" + fieldName + "], got: " + ex.getMessage(),
            ex.getMessage().contains(fieldName) && ex.getMessage().contains("no single configured data format")
        );
    }

    // ---------------------------------------------------------------------
    // Text family
    // ---------------------------------------------------------------------

    public void testTextDefaultsParquetOnlyRejected() {
        // text defaults: index=true → {FTS}. Parquet doesn't cover FTS for text.
        expectCoverageFailure("t-text-def-po", parquetOnly(), "f", "type=text");
    }

    public void testTextDefaultsParquetWithLucene() {
        // text defaults: {FTS}. Parquet rejects, Lucene covers → Lucene wins.
        create("t-text-def-pwl", parquetWithLucene(), "f", "type=text");
        assertOwnedBy(capMap("t-text-def-pwl", "f"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH));
    }

    public void testTextStoredParquetWithLucene() {
        // text + store=true: {FTS, SF}. Lucene declares {FTS, SF} for text → covers.
        create("t-text-st-pwl", parquetWithLucene(), "f", "type=text,store=true");
        assertOwnedBy(capMap("t-text-st-pwl", "f"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH, Capability.STORED_FIELDS));
    }

    public void testMatchOnlyTextDefaultsParquetWithLucene() {
        // match_only_text: index=true → {FTS}. Lucene declares {FTS} only for this type.
        create("t-mot-def-pwl", parquetWithLucene(), "f", "type=match_only_text");
        assertOwnedBy(capMap("t-mot-def-pwl", "f"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH));
    }

    public void testMatchOnlyTextStoreNotCovered() {
        // match_only_text + store=true: requests {FTS, SF}. Lucene declares only {FTS} for this
        // type (no SF), Parquet has no FTS → no single format covers → reject.
        expectCoverageFailure("t-mot-st-pwl", parquetWithLucene(), "f", "type=match_only_text,store=true");
    }

    public void testKeywordDefaultsParquetOnlyRejected() {
        // keyword defaults: {FTS, CS}. Parquet covers CS but not FTS.
        expectCoverageFailure("t-kw-def-po", parquetOnly(), "f", "type=keyword");
    }

    public void testKeywordDefaultsParquetWithLucene() {
        // keyword defaults: {FTS, CS}. Lucene declares {FTS, CS, SF} → covers.
        create("t-kw-def-pwl", parquetWithLucene(), "f", "type=keyword");
        assertOwnedBy(capMap("t-kw-def-pwl", "f"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE));
    }

    public void testKeywordDocValuesOnlyParquetOnly() {
        // keyword + index=false, doc_values=true: {CS}. Parquet covers.
        create("t-kw-dv-po", parquetOnly(), "f", "type=keyword,index=false");
        assertOwnedBy(capMap("t-kw-dv-po", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testKeywordDocValuesOnlyParquetWithLucene() {
        // Same mapping with both formats → Parquet (lower priority) wins.
        create("t-kw-dv-pwl", parquetWithLucene(), "f", "type=keyword,index=false");
        assertOwnedBy(capMap("t-kw-dv-pwl", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testKeywordAllParquetWithLucene() {
        // keyword + index=true, doc_values=true, store=true: {FTS, CS, SF}. Lucene covers.
        create("t-kw-all-pwl", parquetWithLucene(), "f", "type=keyword,store=true");
        assertOwnedBy(
            capMap("t-kw-all-pwl", "f"),
            LUCENE,
            Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE, Capability.STORED_FIELDS)
        );
    }

    // ---------------------------------------------------------------------
    // Numeric family — defaults route via POINT_RANGE
    // ---------------------------------------------------------------------

    public void testIntegerDefaultsParquetOnlyRejected() {
        expectCoverageFailure("t-int-def-po", parquetOnly(), "f", "type=integer");
    }

    public void testIntegerDefaultsParquetWithLucene() {
        create("t-int-def-pwl", parquetWithLucene(), "f", "type=integer");
        assertOwnedBy(capMap("t-int-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testIntegerDocValuesOnlyParquetOnly() {
        create("t-int-dv-po", parquetOnly(), "f", "type=integer,index=false");
        assertOwnedBy(capMap("t-int-dv-po", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testLongDefaultsParquetWithLucene() {
        create("t-long-def-pwl", parquetWithLucene(), "f", "type=long");
        assertOwnedBy(capMap("t-long-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testShortDefaultsParquetWithLucene() {
        create("t-short-def-pwl", parquetWithLucene(), "f", "type=short");
        assertOwnedBy(capMap("t-short-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testByteDefaultsParquetWithLucene() {
        create("t-byte-def-pwl", parquetWithLucene(), "f", "type=byte");
        assertOwnedBy(capMap("t-byte-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testFloatDefaultsParquetWithLucene() {
        create("t-float-def-pwl", parquetWithLucene(), "f", "type=float");
        assertOwnedBy(capMap("t-float-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testHalfFloatDefaultsParquetWithLucene() {
        create("t-hf-def-pwl", parquetWithLucene(), "f", "type=half_float");
        assertOwnedBy(capMap("t-hf-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testDoubleDefaultsParquetWithLucene() {
        create("t-dbl-def-pwl", parquetWithLucene(), "f", "type=double");
        assertOwnedBy(capMap("t-dbl-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testUnsignedLongDefaultsParquetWithLucene() {
        create("t-ul-def-pwl", parquetWithLucene(), "f", "type=unsigned_long");
        assertOwnedBy(capMap("t-ul-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testNumericDocValuesOnlyPicksParquet() {
        // doc_values=true alone → {CS}. Parquet (primary) covers → wins for every numeric type.
        create("t-numdv-pwl", parquetWithLucene(), "f", "type=long,index=false");
        assertOwnedBy(capMap("t-numdv-pwl", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testNumericStoredOnlyPicksLucene() {
        // long with index=false, doc_values=false, store=true → {SF}. Parquet doesn't have SF
        // for long; Lucene declares {PR, CS, SF} → covers.
        create("t-numst-pwl", parquetWithLucene(), "f", "type=long,index=false,doc_values=false,store=true");
        assertOwnedBy(capMap("t-numst-pwl", "f"), LUCENE, Set.of(Capability.STORED_FIELDS));
    }

    public void testNumericAllSettingsPicksLucene() {
        // long with all on: {PR, CS, SF}. Parquet doesn't cover PR; Lucene covers all.
        create("t-numall-pwl", parquetWithLucene(), "f", "type=long,store=true");
        assertOwnedBy(
            capMap("t-numall-pwl", "f"),
            LUCENE,
            Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE, Capability.STORED_FIELDS)
        );
    }

    // ---------------------------------------------------------------------
    // Date family
    // ---------------------------------------------------------------------

    public void testDateDefaultsParquetOnlyRejected() {
        expectCoverageFailure("t-date-def-po", parquetOnly(), "f", "type=date");
    }

    public void testDateDefaultsParquetWithLucene() {
        create("t-date-def-pwl", parquetWithLucene(), "f", "type=date");
        assertOwnedBy(capMap("t-date-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testDateNanosDefaultsParquetWithLucene() {
        create("t-dn-def-pwl", parquetWithLucene(), "f", "type=date_nanos");
        assertOwnedBy(capMap("t-dn-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testDateDocValuesOnlyParquetOnly() {
        create("t-date-dv-po", parquetOnly(), "f", "type=date,index=false");
        assertOwnedBy(capMap("t-date-dv-po", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    // ---------------------------------------------------------------------
    // Boolean
    // ---------------------------------------------------------------------

    public void testBooleanDefaultsParquetOnlyRejected() {
        // Boolean defaults: {FTS, CS}. Parquet has no FTS for boolean.
        expectCoverageFailure("t-bool-def-po", parquetOnly(), "f", "type=boolean");
    }

    public void testBooleanDefaultsParquetWithLucene() {
        create("t-bool-def-pwl", parquetWithLucene(), "f", "type=boolean");
        assertOwnedBy(capMap("t-bool-def-pwl", "f"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE));
    }

    public void testBooleanDocValuesOnlyPicksParquet() {
        create("t-bool-dv-pwl", parquetWithLucene(), "f", "type=boolean,index=false");
        assertOwnedBy(capMap("t-bool-dv-pwl", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    // ---------------------------------------------------------------------
    // IP
    // ---------------------------------------------------------------------

    public void testIpDefaultsParquetOnlyRejected() {
        expectCoverageFailure("t-ip-def-po", parquetOnly(), "f", "type=ip");
    }

    public void testIpDefaultsParquetWithLucene() {
        create("t-ip-def-pwl", parquetWithLucene(), "f", "type=ip");
        assertOwnedBy(capMap("t-ip-def-pwl", "f"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
    }

    public void testIpDocValuesOnlyPicksParquet() {
        create("t-ip-dv-pwl", parquetWithLucene(), "f", "type=ip,index=false");
        assertOwnedBy(capMap("t-ip-dv-pwl", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    // ---------------------------------------------------------------------
    // Binary — index=false by default; Lucene declares only {SF}
    // ---------------------------------------------------------------------

    public void testBinaryAllOffEmptyMap() {
        // binary defaults: index=false (forced), doc_values=false, store=false → {} requested.
        // No validation runs; cap map stays empty.
        create("t-bin-off-pwl", parquetWithLucene(), "f", "type=binary");
        assertTrue("Capability map should be empty for binary with no caps", capMap("t-bin-off-pwl", "f").isEmpty());
    }

    public void testBinaryDocValuesPicksParquet() {
        // binary + doc_values=true: {CS}. Parquet covers; Lucene declares only {SF} for binary.
        create("t-bin-dv-pwl", parquetWithLucene(), "f", "type=binary,doc_values=true");
        assertOwnedBy(capMap("t-bin-dv-pwl", "f"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    public void testBinaryStorePicksLucene() {
        // binary + store=true: {SF}. Parquet has no SF; Lucene covers.
        create("t-bin-st-pwl", parquetWithLucene(), "f", "type=binary,store=true");
        assertOwnedBy(capMap("t-bin-st-pwl", "f"), LUCENE, Set.of(Capability.STORED_FIELDS));
    }

    public void testBinaryDocValuesAndStoreNotCovered() {
        // binary + doc_values=true, store=true: {CS, SF}. Parquet covers CS but not SF for binary;
        // Lucene covers SF but not CS for binary. No single format covers both → reject.
        expectCoverageFailure("t-bin-dvst-pwl", parquetWithLucene(), "f", "type=binary,doc_values=true,store=true");
    }

    // ---------------------------------------------------------------------
    // Multi-field index — different fields can pick different formats
    // ---------------------------------------------------------------------

    public void testMultiFieldRoutingMixed() {
        String idx = "t-multi-pwl";
        CreateIndexResponse r = client().admin()
            .indices()
            .prepareCreate(idx)
            .setSettings(parquetWithLucene())
            .setMapping(
                "tag",
                "type=keyword,index=false,doc_values=true",
                "title",
                "type=keyword,index=true,doc_values=true",
                "ts",
                "type=date,index=true,doc_values=true",
                "tag_dv",
                "type=long,index=false,doc_values=true"
            )
            .get();
        assertTrue(r.isAcknowledged());
        ensureGreen(idx);

        // tag → {CS} → Parquet wins.
        assertOwnedBy(capMap(idx, "tag"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
        // title → {FTS, CS} → Lucene wins.
        assertOwnedBy(capMap(idx, "title"), LUCENE, Set.of(Capability.FULL_TEXT_SEARCH, Capability.COLUMNAR_STORAGE));
        // ts → {PR, CS} → Lucene wins.
        assertOwnedBy(capMap(idx, "ts"), LUCENE, Set.of(Capability.POINT_RANGE, Capability.COLUMNAR_STORAGE));
        // tag_dv → {CS} on long → Parquet wins.
        assertOwnedBy(capMap(idx, "tag_dv"), PARQUET, Set.of(Capability.COLUMNAR_STORAGE));
    }

    // ---------------------------------------------------------------------
    // Empty requested capabilities — no validation, empty map regardless of config
    // ---------------------------------------------------------------------

    public void testKeywordAllOffEmptyMap() {
        create("t-kw-off-pwl", parquetWithLucene(), "f", "type=keyword,index=false,doc_values=false");
        assertTrue("Capability map should be empty when no capabilities requested", capMap("t-kw-off-pwl", "f").isEmpty());
    }

    // ---------------------------------------------------------------------
    // Non-composite index — capability map remains empty
    // ---------------------------------------------------------------------

    public void testNonCompositeIndexEmptyMap() {
        String idx = "t-nc";
        Settings s = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        CreateIndexResponse r = client().admin().indices().prepareCreate(idx).setSettings(s).setMapping("f", "type=keyword").get();
        assertTrue(r.isAcknowledged());
        ensureGreen(idx);
        assertTrue("Non-composite index should have empty capability map", capMap(idx, "f").isEmpty());
    }
}
