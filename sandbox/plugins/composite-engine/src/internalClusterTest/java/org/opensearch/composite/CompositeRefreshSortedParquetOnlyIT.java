/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.composite.framework.ParquetOnlyDataFormatPlugin;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public class CompositeRefreshSortedParquetOnlyIT extends AbstractSortedRefreshIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // ParquetDataFormatPlugin sources its allocator from the unified native-allocator
        // framework's ingest pool, so the framework plugin must be installed.
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetOnlyDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    /**
     * Verifies that a single flush with sort columns produces a Parquet file
     * sorted by age DESC (nulls first), name ASC (nulls last).
     */
    public void testSortedRefreshProducesSortedParquet() throws Exception {
        createIndex(sortedParquetOnlySettings());

        // Index documents in deliberately unsorted order
        indexDoc("charlie", 30);
        indexDoc("alice", 50);
        indexDoc("bob", 10);
        indexDoc("dave", 50);
        indexDoc("eve", 30);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
        verifyParquetRowCount(snapshot, 5);
        verifyParquetSortOrder(snapshot);
        verifyParquetRowIdSequential(snapshot);
    }

    /**
     * Verifies that multiple flush cycles produce independently sorted segments.
     */
    public void testMultipleSortedRefreshesProduceIndependentlySortedSegments() throws Exception {
        createIndex(sortedParquetOnlySettings());

        // First batch
        indexDoc("zara", 5);
        indexDoc("alice", 100);
        indexDoc("bob", 50);
        flushAndRefresh();

        // Second batch
        indexDoc("xavier", 200);
        indexDoc("yolanda", 1);
        indexDoc("wendy", 75);
        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals("Should have 2 segments", 2, snapshot.getSegments().size());
        verifyParquetRowCount(snapshot, 6);
        // Each segment should be independently sorted
        verifyParquetSortOrder(snapshot);
    }

    /**
     * Parquet-only refresh without any sort configuration. Confirms the
     * non-sort flush path still emits sequential {@code __row_id__} values
     * and the expected row count. There is no permutation produced by Parquet,
     * so this exercises the {@code FlushInput.EMPTY} flow end-to-end.
     */
    public void testUnsortedRefreshParquetOnly() throws Exception {
        createIndex(unsortedParquetOnlySettings());

        // Insertion order is preserved end-to-end since no sort is configured.
        indexDoc("charlie", 30);
        indexDoc("alice", 50);
        indexDoc("bob", 10);
        indexDoc("dave", 50);
        indexDoc("eve", 30);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
        verifyParquetRowCount(snapshot, 5);
        verifyParquetRowIdSequential(snapshot);
    }

    private Settings sortedParquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .putList("index.sort.field", "age", "name")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.missing", "_first", "_last")
            .build();
    }

    private Settings unsortedParquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

}
