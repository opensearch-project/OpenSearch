/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Integration tests verifying that {@code index.parquet.row_group_max_rows} and
 * {@code index.parquet.row_group_max_bytes} settings are accepted, persisted, and
 * honoured by the native Parquet writer.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeParquetRowGroupSettingsIT extends AbstractCompositeEngineIT {

    private static final String INDEX_NAME = "test-row-group-settings";

    // ── Tests ──────────────────────────────────────────────────────────────

    /**
     * Verifies that {@code index.parquet.row_group_max_rows} is persisted in index
     * settings and that the written parquet file contains exactly the expected number
     * of rows (i.e. the writer did not silently discard data).
     */
    public void testRowGroupMaxRowsSettingIsApplied() throws IOException {
        int maxRows = 5;
        int totalDocs = 10;

        createCompositeIndex(INDEX_NAME, Settings.builder()
            .put(ParquetSettings.ROW_GROUP_MAX_ROWS.getKey(), maxRows)
            .build());

        assertSettingValue(ParquetSettings.ROW_GROUP_MAX_ROWS.getKey(), String.valueOf(maxRows));

        indexDocs(INDEX_NAME, totalDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        // All rows must be present across the written parquet files
        assertTotalParquetRows(INDEX_NAME, totalDocs);
    }

    /**
     * Verifies that {@code index.parquet.row_group_max_bytes} is persisted in index
     * settings and that the index operates correctly with a small byte limit.
     */
    public void testRowGroupMaxBytesSettingIsApplied() throws IOException {
        long maxBytes = new ByteSizeValue(1, ByteSizeUnit.MB).getBytes();
        int totalDocs = 20;

        createCompositeIndex(INDEX_NAME, Settings.builder()
            .put(ParquetSettings.ROW_GROUP_MAX_BYTES.getKey(), maxBytes + "b")
            .build());

        assertSettingValue(ParquetSettings.ROW_GROUP_MAX_BYTES.getKey(), maxBytes + "b");

        indexDocs(INDEX_NAME, totalDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        assertTotalParquetRows(INDEX_NAME, totalDocs);
    }

    /**
     * Verifies that both row group settings can be set together and the index
     * functions correctly with both limits active.
     */
    public void testRowGroupMaxRowsAndBytesSetTogether() throws IOException {
        int maxRows = 3;
        long maxBytes = new ByteSizeValue(512, ByteSizeUnit.KB).getBytes();
        int totalDocs = 15;

        createCompositeIndex(INDEX_NAME, Settings.builder()
            .put(ParquetSettings.ROW_GROUP_MAX_ROWS.getKey(), maxRows)
            .put(ParquetSettings.ROW_GROUP_MAX_BYTES.getKey(), maxBytes + "b")
            .build());

        assertSettingValue(ParquetSettings.ROW_GROUP_MAX_ROWS.getKey(), String.valueOf(maxRows));
        assertSettingValue(ParquetSettings.ROW_GROUP_MAX_BYTES.getKey(), maxBytes + "b");

        indexDocs(INDEX_NAME, totalDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        assertTotalParquetRows(INDEX_NAME, totalDocs);
    }

    /**
     * Verifies that with {@code row_group_max_rows=1} and 5 docs indexed,
     * each parquet file has exactly 1 row group per row — i.e. total row groups == total docs.
     */
    public void testRowGroupMaxRowsOfOneProducesValidFile() throws IOException {
        int totalDocs = 5;

        createCompositeIndex(INDEX_NAME, Settings.builder()
            .put(ParquetSettings.ROW_GROUP_MAX_ROWS.getKey(), 1)
            .build());

        indexDocs(INDEX_NAME, totalDocs, 0);
        refreshIndex(INDEX_NAME);
        flushIndex(INDEX_NAME);

        assertTotalParquetRows(INDEX_NAME, totalDocs);
        assertTotalRowGroups(INDEX_NAME, totalDocs);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private void createCompositeIndex(String indexName, Settings extraSettings) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .put(extraSettings);

        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(builder)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(indexName);
    }

    private void assertSettingValue(String key, String expected) {
        GetSettingsResponse resp = client().admin().indices().prepareGetSettings(INDEX_NAME).get();
        Settings actual = resp.getIndexToSettings().get(INDEX_NAME);
        assertEquals("Setting " + key + " should be persisted", expected, actual.get(key));
    }

    private void assertTotalParquetRows(String indexName, int expectedTotal) throws IOException {
        CatalogSnapshot snapshot = acquireAndGetSnapshot(indexName);
        Path parquetDir = getPrimaryShard(indexName).shardPath().getDataPath().resolve("parquet");

        long totalRows = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            assertNotNull("Segment should have parquet files", wfs);
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata meta = RustBridge.getFileMetadata(filePath.toString());
                totalRows += meta.numRows();
            }
        }
        assertEquals("Total rows across all parquet files should match indexed docs", expectedTotal, totalRows);
    }

    private void assertTotalRowGroups(String indexName, int expectedTotal) throws IOException {
        CatalogSnapshot snapshot = acquireAndGetSnapshot(indexName);
        Path parquetDir = getPrimaryShard(indexName).shardPath().getDataPath().resolve("parquet");

        long totalRowGroups = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            assertNotNull("Segment should have parquet files", wfs);
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata meta = RustBridge.getFileMetadata(filePath.toString());
                totalRowGroups += meta.numRowGroups();
            }
        }
        assertEquals("Total row groups should equal total docs when max_rows=1", expectedTotal, totalRowGroups);
    }
}
