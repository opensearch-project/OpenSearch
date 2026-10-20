/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Regression guard for issue #22417: a document that fails partway through {@code VSRManager.addDocument}
 * (an Arrow {@link org.apache.arrow.memory.OutOfMemoryException} mid-admission) must not leak the columns it
 * partially wrote into the next document that reuses the same VSR row slot.
 *
 * <p>Background: {@code VSRManager.addDocument} writes a document's fields into the active Arrow
 * {@code VectorSchemaRoot} one vector at a time. Each {@code setSafe} sets the value and the validity bit at
 * the current uncommitted row index; the row is only "counted" after every field write succeeds.
 * {@code ParquetWriter.addDoc} catches Arrow's {@code OutOfMemoryException} and returns a recoverable
 * {@code WriteResult.Failure}; the composite then drives {@code rollbackTo(lastGoodRowCount)}, which no-ops
 * against the VSR (the row count never advanced), the writer stays ACTIVE, and the same VSR is reused. Before
 * the fix, the fields written before the failure survived with their validity bits set, so the next document
 * that lands on that same row index and does not overwrite them inherited the stale value.
 *
 * <p>The fix makes {@code addDocument} all-or-nothing: on a mid-document failure it scrubs the partially
 * written row before rethrowing, so the reused row starts clean.
 *
 * <p>Reproduction for each test:
 * <ol>
 *   <li><b>doc1</b> {@code {clean:"d1", a_leak:<warmup>}} — succeeds at row 0, allocating the {@code a_leak}
 *       vector's buffers so a later write to row 1 reuses them.</li>
 *   <li>Throttle the ingest pool via {@code native.allocator.pool.ingest.max} above the shard footprint but
 *       below the large {@code z_big} value.</li>
 *   <li><b>doc2</b> {@code {a_leak:<leak>, z_big:<2MB>}} — fields are processed in name order: {@code _id}
 *       (small realloc, fits), {@code a_leak} (written at row 1), then {@code z_big} (large allocation that
 *       OOMs). The doc fails as a recoverable per-doc failure; the row count stays 1, leaving {@code a_leak}
 *       as a partial write.</li>
 *   <li>Reset the ingest pool limit.</li>
 *   <li><b>doc3</b> {@code {clean:"d3"}} — reuses row 1 and does not set {@code a_leak}.</li>
 *   <li>Flush + refresh, then read the raw Parquet columns and assert doc3's row has {@code a_leak == null}.</li>
 * </ol>
 *
 * <p>The two tests cover the two Arrow vector families the scrub must handle: a fixed-width {@code long}
 * ({@code BigIntVector}) and a variable-width {@code text} ({@code VarCharVector}).
 *
 * <p>The rebalancer is disabled so the ingest pool limit stays exactly where the test sets it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeParquetRollbackCorruptionIT extends AbstractCompositeEngineIT {

    private static final String INGEST_MAX_SETTING = "native.allocator.pool.ingest.max";

    // Fixed-width (long) leak scenario.
    private static final long LEAKED_LONG = 999_999_999L;
    private static final long WARMUP_LONG = 111L;

    // Variable-width (text / VarCharVector) leak scenario. The warm-up value pre-grows the VarChar data
    // buffer so doc2's (shorter) write to row 1 reuses capacity without a large allocation, leaving a
    // partial write rather than OOM-ing on the leak field itself.
    private static final String LEAKED_TEXT = "LEAKED_FROM_DOC2";
    private static final String WARMUP_TEXT = "w".repeat(4096);

    // doc2's OOM trigger: a large value whose Arrow buffer allocation blows the ingest limit after `_id`
    // and `a_leak` (ordered before `z_big`) have already been written for the row.
    private static final String BIG_VALUE = "x".repeat(2 * 1024 * 1024);

    // Ingest pool ceiling while doc2 is indexed: above the warm-up shard footprint so the small `_id`
    // realloc and the `a_leak` write succeed, but below `z_big` so the large allocation OOMs mid-document.
    private static final String INGEST_LIMIT_BYTES = Long.toString(1024 * 1024);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Keep the ingest pool limit fixed at whatever the test sets — no background growth.
            .put("native.allocator.rebalancer.enabled", false)
            .build();
    }

    /** Fixed-width column: a mid-document OOM must not leak the failed doc's {@code long} into the reused row. */
    public void testMidDocOomDoesNotLeakNumericColumnIntoReusedRow() throws Exception {
        String index = "corruption-numeric-idx";
        createCorruptionIndex(index, "a_leak", "type=long");

        IndexResponse doc1 = client().prepareIndex(index).setSource("clean", "d1", "a_leak", WARMUP_LONG).get();
        assertEquals(RestStatus.CREATED, doc1.status());

        indexFailingDoc2(index, LEAKED_LONG);

        // Engine must still be open — an ingest OOM is a recoverable per-doc failure, not a tragic event.
        assertNotNull("engine must survive a per-doc ingest OOM", getEngine(index).commitStats());

        IndexResponse doc3 = client().prepareIndex(index).setSource("clean", "d3").get();
        assertEquals(RestStatus.CREATED, doc3.status());

        flushIndex(index);
        refreshIndex(index);

        List<Map<String, Object>> rows = readParquetRows(index);
        logger.info("[corruption-it] numeric parquet rows ({}): {}", rows.size(), rows);
        Map<String, Object> doc3Row = findRowByClean(rows, "d3");

        assertNull("doc3 never set a_leak; the OOM-failed doc2's value must not leak into the reused row", doc3Row.get("a_leak"));
    }

    /** Variable-width column: a mid-document OOM must not leak the failed doc's {@code text} into the reused row. */
    public void testMidDocOomDoesNotLeakTextColumnIntoReusedRow() throws Exception {
        String index = "corruption-text-idx";
        createCorruptionIndex(index, "a_leak", "type=text");

        IndexResponse doc1 = client().prepareIndex(index).setSource("clean", "d1", "a_leak", WARMUP_TEXT).get();
        assertEquals(RestStatus.CREATED, doc1.status());

        indexFailingDoc2(index, LEAKED_TEXT);

        assertNotNull("engine must survive a per-doc ingest OOM", getEngine(index).commitStats());

        IndexResponse doc3 = client().prepareIndex(index).setSource("clean", "d3").get();
        assertEquals(RestStatus.CREATED, doc3.status());

        flushIndex(index);
        refreshIndex(index);

        List<Map<String, Object>> rows = readParquetRows(index);
        logger.info("[corruption-it] text parquet rows ({}): {}", rows.size(), rows);
        Map<String, Object> doc3Row = findRowByClean(rows, "d3");

        assertNull("doc3 never set a_leak; the OOM-failed doc2's text value must not leak into the reused row", doc3Row.get("a_leak"));
    }

    /**
     * Indexes doc2 with {@code {a_leak:<leakValue>, z_big:<2MB>}} under a throttled ingest pool so it fails
     * mid-document (after {@code a_leak} is written for the row, when {@code z_big} allocation OOMs). Asserts
     * the doc was rejected.
     */
    private void indexFailingDoc2(String index, Object leakValue) {
        setIngestPoolLimit(INGEST_LIMIT_BYTES);
        try {
            BulkResponse bulk = client().prepareBulk()
                .add(client().prepareIndex(index).setSource("a_leak", leakValue, "z_big", BIG_VALUE))
                .get();
            assertEquals(1, bulk.getItems().length);
            String failure = bulk.getItems()[0].getFailureMessage();
            assertTrue("doc2 must fail with a mid-document ingest OOM: " + failure, bulk.getItems()[0].isFailed());
            logger.info("[corruption-it] doc2 failure message: {}", failure);
        } finally {
            // Relieve the limit so doc3, flush and teardown can allocate freely.
            resetIngestPoolLimit();
        }
    }

    private void createCorruptionIndex(String index, String leakField, String leakMapping) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(settings)
            .setMapping("clean", "type=keyword", leakField, leakMapping, "z_big", "type=keyword")
            .get();
        ensureGreen(index);
    }

    private void setIngestPoolLimit(String bytes) {
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(INGEST_MAX_SETTING, bytes))
                .get()
                .isAcknowledged()
        );
    }

    private void resetIngestPoolLimit() {
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(INGEST_MAX_SETTING))
                .get()
                .isAcknowledged()
        );
    }

    private static Map<String, Object> findRowByClean(List<Map<String, Object>> rows, String cleanValue) {
        return rows.stream()
            .filter(r -> cleanValue.equals(r.get("clean")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find row (clean=" + cleanValue + ") in parquet output: " + rows));
    }

    /** Reads every row of every parquet file backing the primary shard as a list of column maps. */
    private List<Map<String, Object>> readParquetRows(String indexName) throws IOException {
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> allRows = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot()) {
            for (Segment segment : snapshot.get().getSegments()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
                if (wfs == null) {
                    continue;
                }
                for (String file : wfs.files()) {
                    allRows.addAll(parseJsonRows(RustBridge.readAsJson(parquetDir.resolve(file).toString())));
                }
            }
        }
        return allRows;
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> parseJsonRows(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(o -> (Map<String, Object>) o).collect(Collectors.toList());
        }
    }
}
