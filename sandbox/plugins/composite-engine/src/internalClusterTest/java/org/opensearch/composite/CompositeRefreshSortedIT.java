/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Integration test for composite refresh (flush) with sort columns configured.
 * Verifies that sort-on-close in Parquet and reorder in Lucene produce correct results
 * at the individual segment level (pre-merge).
 *
 * @opensearch.experimental
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeRefreshSortedIT extends AbstractSortedRefreshIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // ParquetDataFormatPlugin sources its allocator from the unified native-allocator
        // framework's ingest pool, so the framework plugin must be installed.
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    // ══════════════════════════════════════════════════════════════════════
    // Tests
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies sorted refresh with Lucene secondary:
     * - Parquet is sorted
     * - Lucene __row_id__ is sequential (RowIdMapping applied)
     * - Cross-format consistency: reading Parquet and Lucene in physical order produces
     *   the same {@code name} and {@code tag} values at every position. {@code tag} is
     *   a keyword field NOT in the sort key — it confirms non-sort fields are also
     *   correctly co-located across formats. Numeric fields like {@code age} live only
     *   in Parquet, so cannot be compared across formats.
     */
    public void testSortedRefreshWithLuceneSecondary() throws Exception {
        createIndex(sortedParquetWithLuceneSettings());

        indexDoc("charlie", 30, "blue");
        indexDoc("alice", 50, "red");
        indexDoc("bob", 10, "green");
        indexDoc("dave", 50, "yellow");
        indexDoc("eve", 30, "purple");

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Should have parquet format", formats.contains("parquet"));
        assertTrue("Should have lucene format", formats.contains("lucene"));

        verifyParquetRowCount(snapshot, 5);
        verifyParquetSortOrder(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
        verifyParquetAndLuceneRowsAlignedSequentially(snapshot);
    }

    /**
     * Verifies null handling in sorted output: age DESC with nulls first,
     * name ASC with nulls last. Runs against Parquet primary + Lucene secondary
     * to also confirm that the row ID rewrite (driven by Parquet's sort
     * permutation) yields a sequential {@code __row_id__} in Lucene even when
     * the sort key contains nulls.
     */
    public void testSortedRefreshWithNulls() throws Exception {
        createIndex(sortedParquetWithLuceneSettings());

        // Mix of null and non-null values
        indexDoc("alice", 50);
        indexDocNullAge("bob");       // null age → should sort first (nulls first for age)
        indexDoc("charlie", 30);
        indexDocNullAge("dave");      // null age → should sort first
        indexDoc("eve", 50);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Should have parquet format", formats.contains("parquet"));
        assertTrue("Should have lucene format", formats.contains("lucene"));

        verifyParquetRowCount(snapshot, 5);
        verifyParquetSortOrder(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
    }

    /**
     * Verifies correctness with a randomized number of documents and fields.
     * Uses random(100, 10000) docs and random(3, 10) integer sort fields to
     * exercise the sort path with varying data shapes.
     */
    public void testSortedRefreshWithRandomizedData() throws Exception {
        int numFields = randomIntBetween(3, 10);
        String[] fieldNames = new String[numFields];
        String[] sortFields = new String[numFields];
        String[] sortOrders = new String[numFields];
        String[] sortMissing = new String[numFields];
        for (int f = 0; f < numFields; f++) {
            fieldNames[f] = "field_" + f;
            sortFields[f] = fieldNames[f];
            sortOrders[f] = randomBoolean() ? "asc" : "desc";
            sortMissing[f] = sortOrders[f].equals("asc") ? "_last" : "_first";
        }

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .putList("index.sort.field", sortFields)
            .putList("index.sort.order", sortOrders)
            .putList("index.sort.missing", sortMissing)
            .build();

        // Build mapping: all fields are integers for sort compatibility
        String[] mappingArgs = new String[numFields * 2];
        for (int f = 0; f < numFields; f++) {
            mappingArgs[f * 2] = fieldNames[f];
            mappingArgs[f * 2 + 1] = "type=integer";
        }
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping(mappingArgs).get();
        ensureGreen(INDEX_NAME);

        int totalDocs = randomIntBetween(100, 10000);
        logger.info(
            "testSortedRefreshWithRandomizedData: {} docs, {} fields, sort orders: {}",
            totalDocs,
            numFields,
            Arrays.toString(sortOrders)
        );

        for (int i = 0; i < totalDocs; i++) {
            Map<String, Object> source = new HashMap<>();
            for (int f = 0; f < numFields; f++) {
                // Occasionally emit null values to exercise null handling in sort
                if (randomIntBetween(0, 9) == 0) {
                    continue; // skip field → null
                }
                source.put(fieldNames[f], randomIntBetween(0, 1000));
            }
            IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource(source).get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        verifyParquetRowCount(snapshot, totalDocs);
        verifyParquetSortOrderMultiField(snapshot, fieldNames, sortOrders, sortMissing);
        verifyLuceneDocCount(totalDocs);
        verifyLuceneRowIdSequential();
    }

    /**
     * Parquet primary + Lucene secondary without sort. Without a sort
     * permutation, Parquet does not produce a {@code RowIdMapping} and the
     * Lucene secondary writer takes the unsorted path. Both formats should:
     *   - hold the same row count,
     *   - expose sequential {@code __row_id__} (Parquet rows and Lucene docs),
     *   - align position-for-position on shared keyword fields, since both
     *     follow insertion order with no reorder applied.
     */
    public void testUnsortedRefreshWithLuceneSecondary() throws Exception {
        createIndex(unsortedParquetWithLuceneSettings());

        // Deliberately unsorted insertion: confirms no sort is applied (otherwise
        // verifyParquetAndLuceneRowsAlignedSequentially would still pass but only
        // because both sides reordered the same way; a position-wise match against
        // insertion order is the stronger check below).
        indexDoc("charlie", 30, "blue");
        indexDoc("alice", 50, "red");
        indexDoc("bob", 10, "green");
        indexDoc("dave", 50, "yellow");
        indexDoc("eve", 30, "purple");

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Should have parquet format", formats.contains("parquet"));
        assertTrue("Should have lucene format", formats.contains("lucene"));

        verifyParquetRowCount(snapshot, 5);
        verifyParquetRowIdSequential(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
        // Position-wise alignment of Parquet rows and Lucene docs on shared
        // keyword fields (name, tag). Without sort, both must be in insertion order.
        verifyParquetAndLuceneRowsAlignedSequentially(snapshot);

        // Stronger check: insertion order is preserved end-to-end. We assert the
        // exact name sequence to rule out any silent reordering.
        verifyLuceneNamesInOrder(new String[] { "charlie", "alice", "bob", "dave", "eve" });
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: settings
    // ══════════════════════════════════════════════════════════════════════

    private Settings sortedParquetWithLuceneSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .putList("index.sort.field", "age", "name")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.missing", "_first", "_last")
            .build();
    }

    private Settings unsortedParquetWithLuceneSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }
}
