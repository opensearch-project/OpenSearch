/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Bits;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.RustBridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Invariant checks for update and delete on a composite (parquet primary + lucene secondary) index.
 *
 * The {@code docs.count} / {@code docs.deleted} accounting checks are deliberately absent. On this
 * branch {@code docs.deleted} is always zero for a composite index and {@code docs.count} reports rows
 * on disk rather than reachable documents, so an accounting assertion here would either fail or bake
 * in the wrong numbers. They belong with the stats work.
 */
final class CompositeUpdateDeleteInvariants {

    /** Stamped onto every lucene segment by {@code LuceneWriterCodec}; joins a leaf to a catalog generation. */
    private static final String WRITER_GENERATION_ATTRIBUTE = "writer_generation";

    private static final String SEQ_NO = "_seq_no";
    private static final String PARQUET_FORMAT = "parquet";

    private CompositeUpdateDeleteInvariants() {}

    /** One position of a published generation, as lucene sees it. */
    record LuceneRow(long generation, int position, long seqNo, boolean live) {
    }

    /**
     * One position of a published generation, as parquet holds it.
     *
     * <p>There is no {@code _id} here even though parquet does store one. {@code RustBridge.readAsJson}
     * renders a Binary column as the literal string {@code <unsupported:Binary>}, and {@code _id} is a
     * Binary column, so every row would come back with the same unusable key. Identity therefore comes
     * from {@code _seq_no}, which is unique per shard and which the caller already knows for every
     * write from {@code IndexResponse.getSeqNo()}.
     */
    record ParquetRow(long generation, int position, long seqNo, Map<String, Object> columns) {
    }

    /**
     * Asserts parquet row {@code i} and lucene doc {@code i} are the same document, in every generation.
     *
     */
    static void assertParquetLuceneAligned(String context, IndexShard shard, CatalogSnapshot snapshot) throws IOException {
        Map<Long, List<ParquetRow>> parquet = parquetRowsByGeneration(shard, snapshot);
        Map<Long, List<LuceneRow>> lucene = luceneRowsByGeneration(shard);

        assertEquals(
            context + ": the generations parquet holds and the generations lucene holds must be the same set",
            new TreeMap<>(parquet).keySet().toString(),
            new TreeMap<>(lucene).keySet().toString()
        );

        for (Map.Entry<Long, List<ParquetRow>> entry : new TreeMap<>(parquet).entrySet()) {
            long generation = entry.getKey();
            List<ParquetRow> parquetRows = entry.getValue();
            List<LuceneRow> luceneRows = lucene.get(generation);

            assertEquals(
                context + ": generation " + generation + " must hold the same number of rows in both formats",
                parquetRows.size(),
                luceneRows.size()
            );

            for (int position = 0; position < parquetRows.size(); position++) {
                ParquetRow parquetRow = parquetRows.get(position);
                LuceneRow luceneRow = luceneRows.get(position);
                if (parquetRow.seqNo() != luceneRow.seqNo()) {
                    fail(
                        context
                            + ": positions drifted in generation "
                            + generation
                            + " at position "
                            + position
                            + " — parquet holds _seq_no="
                            + parquetRow.seqNo()
                            + " but lucene holds _seq_no="
                            + luceneRow.seqNo()
                            + ". Every position from here on refers to the wrong document."
                    );
                }
            }
        }
    }

    /**
     * Asserts that exactly the expected documents are reachable, each at exactly the expected version.
     *
     */
    static void assertReachableDocuments(String context, IndexShard shard, CatalogSnapshot snapshot, Map<String, Long> expectedReachable)
        throws IOException {
        Map<Long, String> expectedBySeqNo = new TreeMap<>();
        for (Map.Entry<String, Long> entry : expectedReachable.entrySet()) {
            expectedBySeqNo.put(entry.getValue(), entry.getKey());
        }

        List<Long> found = new ArrayList<>();
        for (ParquetRow row : liveRows(shard, snapshot)) {
            found.add(row.seqNo());
        }

        List<String> missing = new ArrayList<>();
        for (Map.Entry<Long, String> entry : expectedBySeqNo.entrySet()) {
            long occurrences = found.stream().filter(seqNo -> seqNo.equals(entry.getKey())).count();
            if (occurrences != 1) {
                missing.add(entry.getValue() + " (_seq_no=" + entry.getKey() + ") reachable " + occurrences + " times, expected once");
            }
        }

        List<Long> unexpected = new ArrayList<>();
        for (long seqNo : found) {
            if (expectedBySeqNo.containsKey(seqNo) == false) {
                unexpected.add(seqNo);
            }
        }

        if (missing.isEmpty() == false || unexpected.isEmpty() == false) {
            fail(
                context
                    + ": the reachable documents are wrong. Expected "
                    + expectedReachable.size()
                    + " reachable rows at _seq_no="
                    + expectedBySeqNo
                    + ", found "
                    + found.size()
                    + " at _seq_no="
                    + found
                    + "."
                    + (missing.isEmpty() ? "" : " Not reachable exactly once: " + missing + ".")
                    + (unexpected.isEmpty()
                        ? ""
                        : " Reachable but expected by no document, so either a superseded row that was never hidden"
                            + " or a row hidden at the wrong position: _seq_no="
                            + unexpected
                            + ".")
            );
        }
    }

    /**
     * Asserts the row carrying {@code seqNo} is still in a parquet file but is not reachable.
     *
     */
    static void assertHiddenRowStillPresent(String context, IndexShard shard, CatalogSnapshot snapshot, long seqNo) throws IOException {
        ParquetRow onDisk = null;
        for (List<ParquetRow> rows : parquetRowsByGeneration(shard, snapshot).values()) {
            for (ParquetRow row : rows) {
                if (row.seqNo() == seqNo) {
                    onDisk = row;
                    break;
                }
            }
        }
        assertNotNull(
            context + ": no parquet row carries _seq_no=" + seqNo + ", so the old copy was overwritten rather than hidden",
            onDisk
        );

        for (ParquetRow live : liveRows(shard, snapshot)) {
            if (live.seqNo() == seqNo) {
                fail(context + ": the row at _seq_no=" + seqNo + " is still reachable, so the old copy was never hidden");
            }
        }
    }

    /** Asserts how many rows are reachable in total, summed across generations. */
    static void assertReachableRowCount(String context, IndexShard shard, CatalogSnapshot snapshot, int expected) throws IOException {
        List<ParquetRow> live = liveRows(shard, snapshot);
        assertEquals(context + ": reachable rows, found " + describe(live), expected, live.size());
    }

    /** Asserts how many rows are physically present, summed across generations. Includes hidden rows. */
    static void assertRowsOnDisk(String context, IndexShard shard, CatalogSnapshot snapshot, int expected) throws IOException {
        int total = 0;
        for (List<ParquetRow> rows : parquetRowsByGeneration(shard, snapshot).values()) {
            total += rows.size();
        }
        assertEquals(context + ": rows physically present on disk", expected, total);
    }

    /**
     * Every reachable row, taken as the parquet rows at the positions lucene reports as live.
     */
    static List<ParquetRow> liveRows(IndexShard shard, CatalogSnapshot snapshot) throws IOException {
        Map<Long, List<ParquetRow>> parquet = parquetRowsByGeneration(shard, snapshot);
        List<ParquetRow> live = new ArrayList<>();
        for (Map.Entry<Long, List<LuceneRow>> entry : new TreeMap<>(luceneRowsByGeneration(shard)).entrySet()) {
            List<ParquetRow> parquetRows = parquet.get(entry.getKey());
            if (parquetRows == null) {
                continue;
            }
            for (LuceneRow luceneRow : entry.getValue()) {
                if (luceneRow.live() && luceneRow.position() < parquetRows.size()) {
                    live.add(parquetRows.get(luceneRow.position()));
                }
            }
        }
        return live;
    }

    /**
     * The parquet rows of every published generation, in file order, keyed by writer generation.
     *
     */
    @SuppressForbidden(reason = "reads the parquet files written by the shard under test")
    static Map<Long, List<ParquetRow>> parquetRowsByGeneration(IndexShard shard, CatalogSnapshot snapshot) throws IOException {
        Path parquetDir = shard.shardPath().getDataPath().resolve(PARQUET_FORMAT);
        Map<Long, List<ParquetRow>> byGeneration = new HashMap<>();

        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet fileSet = segment.dfGroupedSearchableFiles().get(PARQUET_FORMAT);
            if (fileSet == null || fileSet.files().isEmpty()) {
                continue;
            }
            assertEquals(
                "generation "
                    + segment.generation()
                    + " holds "
                    + fileSet.files().size()
                    + " parquet files "
                    + fileSet.files()
                    + "; the position comparison needs a defined row order across files before it can handle more than one",
                1,
                fileSet.files().size()
            );

            String file = fileSet.files().iterator().next();
            Path path = parquetDir.resolve(file);
            assertTrue("parquet file named by the catalog is missing from disk: " + path, Files.exists(path));

            List<ParquetRow> rows = new ArrayList<>();
            String json = RustBridge.readAsJson(path.toString());
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    json
                )
            ) {
                int position = 0;
                for (Object o : parser.list()) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> columns = (Map<String, Object>) o;
                    Object seqNo = columns.get(SEQ_NO);
                    assertNotNull(
                        "parquet row " + position + " of generation " + segment.generation() + " has no " + SEQ_NO + " column: " + columns,
                        seqNo
                    );
                    rows.add(new ParquetRow(segment.generation(), position, ((Number) seqNo).longValue(), columns));
                    position++;
                }
            }
            byGeneration.put(segment.generation(), rows);
        }
        return byGeneration;
    }

    /**
     * Every lucene doc of every committed segment, in position order, keyed by writer generation.
     *
     */
    static Map<Long, List<LuceneRow>> luceneRowsByGeneration(IndexShard shard) throws IOException {
        Path luceneDir = shard.shardPath().resolveIndex();
        Map<Long, List<LuceneRow>> byGeneration = new HashMap<>();
        Set<String> seenGenerations = new HashSet<>();

        try (Directory directory = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(directory)) {
            for (LeafReaderContext leafContext : reader.leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(leafContext.reader());
                String attribute = segmentReader.getSegmentInfo().info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
                assertNotNull(
                    "lucene segment ["
                        + segmentReader.getSegmentInfo().info.name
                        + "] carries no "
                        + WRITER_GENERATION_ATTRIBUTE
                        + " attribute, so it cannot be joined to a catalog generation",
                    attribute
                );
                assertTrue(
                    "two lucene segments claim writer generation " + attribute + "; the generation to segment join must be one to one",
                    seenGenerations.add(attribute)
                );

                long generation = Long.parseLong(attribute);
                NumericDocValues seqNos = segmentReader.getNumericDocValues(SEQ_NO);
                assertNotNull(
                    "lucene segment ["
                        + segmentReader.getSegmentInfo().info.name
                        + "] has no "
                        + SEQ_NO
                        + " doc values, so positions cannot be compared against parquet",
                    seqNos
                );
                Bits liveDocs = segmentReader.getLiveDocs();

                List<LuceneRow> rows = new ArrayList<>(segmentReader.maxDoc());
                for (int position = 0; position < segmentReader.maxDoc(); position++) {
                    assertTrue(
                        "lucene doc " + position + " of generation " + generation + " has no " + SEQ_NO + " value",
                        seqNos.advanceExact(position)
                    );
                    boolean live = liveDocs == null || liveDocs.get(position);
                    rows.add(new LuceneRow(generation, position, seqNos.longValue(), live));
                }
                byGeneration.put(generation, rows);
            }
        }
        return byGeneration;
    }

    /** Renders reachable rows as position and {@code _seq_no} per generation, for a failure message. */
    private static String describe(List<ParquetRow> rows) {
        Map<Long, List<String>> byGeneration = new TreeMap<>();
        for (ParquetRow row : rows) {
            byGeneration.computeIfAbsent(row.generation(), k -> new ArrayList<>()).add(row.position() + ":" + row.seqNo());
        }
        return byGeneration.toString();
    }
}
