/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drives the collectors of a shard's slices directly, to interleave them deterministically around the shard's row limit.
 */
public class ArrowScanCollectorTests extends OpenSearchTestCase {

    private static final Column COLUMN = new Column("k", ColumnType.UTF8, Set.of("keyword"));
    /** Rows per batch, equal to the first chunk a slice buffers, so that chunk fills the slice's first batch. */
    private static final int BATCH_ROWS = ArrowScanCollector.SEED_CHUNK;

    private final AtomicInteger slotsHeld = new AtomicInteger();
    private BufferAllocator allocator;

    @Before
    public void createAllocator() {
        allocator = new RootAllocator();
    }

    @After
    public void closeAllocator() {
        assertEquals("compute slots are released", 0, slotsHeld.get());
        assertEquals("batches are released", 0, allocator.getAllocatedMemory());
        allocator.close();
    }

    /**
     * Slice a sends a batch of every doc it buffered while slice b takes the shard's last rows. When a resumes, it holds no
     * compute slot to look for a match past the limit, so it stops at once instead of scoring the rest of its segment outside
     * the node's compute slots and its budget. The rest of its segment may hold matches the limit cut.
     */
    public void testSliceResumingAfterTheRowLimitWasReachedStopsScoring() throws Exception {
        assertSliceResumingAfterTheRowLimitWasReached(BATCH_ROWS + randomIntBetween(1, 100), true);
    }

    /** As above, but the batch slice a sends ends its segment, so no doc of the segment is left out. */
    public void testSliceResumingAfterTheRowLimitWasReachedAtTheEndOfItsSegmentLeavesNoMatchOut() throws Exception {
        assertSliceResumingAfterTheRowLimitWasReached(BATCH_ROWS, false);
    }

    private void assertSliceResumingAfterTheRowLimitWasReached(int aDocs, boolean terminatedEarly) throws Exception {
        int bDocs = randomIntBetween(1, BATCH_ROWS - 1);
        withSegments(new int[] { aDocs, bDocs }, leaves -> {
            assertEquals(aDocs, leaves.get(0).reader().maxDoc());
            CountDownLatch sending = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            ScanState state = state(BATCH_ROWS + bDocs, -1, batch -> {
                batch.close();
                sending.countDown();
                assertTrue(resume.await(30, TimeUnit.SECONDS));
            });
            try (state) {
                ArrowScanCollector a = new ArrowScanCollector(state, context());
                ArrowScanCollector b = new ArrowScanCollector(state, context());
                AtomicInteger stoppedAt = new AtomicInteger(-1);
                AtomicReference<Exception> failure = new AtomicReference<>();
                Thread sliceA = Thread.ofVirtual().start(() -> {
                    try {
                        LeafCollector leaf = a.getLeafCollector(leaves.get(0));
                        leaf.setScorer(null);
                        int doc = 0;
                        try {
                            for (; doc < aDocs; doc++) {
                                leaf.collect(doc);
                            }
                        } catch (CollectionTerminatedException e) {
                            stoppedAt.set(doc);
                        }
                        leaf.finish();
                        a.finish();
                    } catch (Exception e) {
                        failure.set(e);
                    }
                });
                try {
                    assertTrue("slice a sends its first batch", sending.await(30, TimeUnit.SECONDS));
                    // Slice b reads every doc of its segment, which are the shard's last rows: it leaves no match out.
                    LeafCollector leaf = b.getLeafCollector(leaves.get(1));
                    leaf.setScorer(null);
                    for (int doc = 0; doc < bDocs; doc++) {
                        leaf.collect(doc);
                    }
                    leaf.finish();
                    b.finish();
                    assertTrue(state.rowBudget.exhausted());
                    assertFalse(state.terminatedEarly);
                } finally {
                    resume.countDown();
                    sliceA.join();
                }
                assertNull(failure.get());
                assertEquals("slice a stops at the doc whose batch it sent", BATCH_ROWS - 1, stoppedAt.get());
                assertEquals(terminatedEarly, state.terminatedEarly);
                assertFalse(state.timedOut);
                assertEquals(BATCH_ROWS, state.rowsSent.get());
                assertRowsUnsent(state, bDocs);
            }
        });
    }

    /**
     * A slice that reaches the shard's row limit with every doc it read keeps scoring, to find a match past the limit. If its
     * compute budget runs out meanwhile, the wait ends, but the scan does not time out: it kept every row the shard may stream.
     */
    public void testComputeBudgetSpentWhileAwaitingAMatchPastTheRowLimitIsNoTimeout() throws Exception {
        int limit = randomIntBetween(1, BATCH_ROWS - 1);
        withSegments(new int[] { 100 }, leaves -> {
            ScanState state = state(limit, TimeUnit.MILLISECONDS.toNanos(100), batch -> {
                batch.close();
                fail("no batch is full");
            });
            try (state) {
                ArrowScanCollector collector = new ArrowScanCollector(state, context());
                LeafCollector leaf = collector.getLeafCollector(leaves.get(0));
                leaf.setScorer(null);
                for (int doc = 0; doc < limit; doc++) {
                    leaf.collect(doc);
                }
                assertTrue(state.rowBudget.exhausted());
                Runnable budgetCheck = ArrowScanCollector.budgetCheck(state);
                assertBusy(() -> expectThrows(CollectionTerminatedException.class, budgetCheck::run));
                assertFalse("the row limit, not the budget, ended the scan", state.timedOut);
                assertTrue("the rest of the segment may match", state.terminatedEarly);
                leaf.finish();
                collector.finish();
                assertRowsUnsent(state, limit);
            }
        });
    }

    /**
     * A slice whose compute budget runs out after another slice reached the shard's row limit does not time the scan out. The
     * shard streamed every row it may, and the rest of the slice's segment may hold matches the limit cut.
     */
    public void testComputeBudgetSpentAfterAnotherSliceReachedTheRowLimitIsNoTimeout() throws Exception {
        int limit = randomIntBetween(1, BATCH_ROWS - 1);
        long budgetNanos = TimeUnit.MILLISECONDS.toNanos(100);
        withSegments(new int[] { limit, 100 }, leaves -> {
            ScanState state = state(limit, budgetNanos, batch -> {
                batch.close();
                fail("no batch is full");
            });
            try (state) {
                ArrowScanCollector a = new ArrowScanCollector(state, context());
                ArrowScanCollector b = new ArrowScanCollector(state, context());
                // Slice b scores its segment, without a match so far, until its compute budget is spent.
                LeafCollector leafB = b.getLeafCollector(leaves.get(1));
                long computing = System.nanoTime();
                leafB.setScorer(null);
                assertBusy(() -> assertTrue(System.nanoTime() - computing > budgetNanos));
                // Meanwhile slice a reads the shard's last rows.
                LeafCollector leafA = a.getLeafCollector(leaves.get(0));
                leafA.setScorer(null);
                for (int doc = 0; doc < limit; doc++) {
                    leafA.collect(doc);
                }
                leafA.finish();
                a.finish();
                assertTrue(state.rowBudget.exhausted());

                expectThrows(CollectionTerminatedException.class, b::checkBudgetWhileScoring);
                assertFalse("the row limit, not the budget, ended the scan", state.timedOut);
                assertTrue("the rest of slice b's segment may match", state.terminatedEarly);
                leafB.finish();
                b.finish();
                assertRowsUnsent(state, limit);
            }
        });
    }

    /**
     * Looking up list values grows a slice's scratch buffers to a few batches while it computes. A slice that sends a batch
     * may wait for as long as its consumer does not read, so it keeps little of that heap.
     */
    public void testSliceWaitingToSendKeepsLittleScratchHeap() throws Exception {
        Column tags = new Column(COLUMN.name(), ColumnType.UTF8, Set.of("keyword"), true);
        int docs = 500;
        long budget = 64 << 10;
        // 200 distinct values of 8 bytes per doc, so a batch looks up thousands of them.
        withSegments(new int[] { docs }, doc -> {
            List<BytesRef> values = new ArrayList<>();
            for (int v = 0; v < 200; v++) {
                values.add(new BytesRef(String.format(Locale.ROOT, "%08d", doc * 200 + v)));
            }
            return values;
        }, leaves -> {
            AtomicReference<ArrowScanCollector> slice = new AtomicReference<>();
            List<Long> scratchOnSend = new ArrayList<>();
            ScanState state = state(List.of(tags), budget, Long.MAX_VALUE, -1, batch -> {
                batch.close();
                scratchOnSend.add(slice.get().scratchBytes());
            });
            try (state) {
                slice.set(new ArrowScanCollector(state, context()));
                LeafCollector leaf = slice.get().getLeafCollector(leaves.get(0));
                leaf.setScorer(null);
                for (int doc = 0; doc < docs; doc++) {
                    leaf.collect(doc);
                }
                leaf.finish();
                slice.get().finish();
                assertThat(scratchOnSend.size(), greaterThan(5));
                for (long scratch : scratchOnSend) {
                    assertThat(scratchOnSend.toString(), scratch, lessThan(budget / 4));
                }
                int unsent = 0;
                for (VectorSchemaRoot batch : state.takeRemainingRows()) {
                    try (batch) {
                        unsent += batch.getRowCount();
                    }
                }
                assertEquals(docs, state.rowsSent.get() + unsent);
            }
        });
    }

    private ScanState state(long rowLimit, long computeBudgetNanos, ScanState.BatchSender sender) {
        ArrowScanSpec spec = new ArrowScanSpec(
            List.of(COLUMN),
            1 << 20,
            1 << 20,
            BATCH_ROWS,
            3 << 20,
            rowLimit,
            TimeValue.timeValueMinutes(1)
        );
        return state(spec, computeBudgetNanos, sender);
    }

    /** A scan of {@code columns} in batches of {@code batchBytes}, with Arrow search's default rows per batch. */
    private ScanState state(List<Column> columns, long batchBytes, long rowLimit, long computeBudgetNanos, ScanState.BatchSender sender) {
        ArrowScanSpec spec = new ArrowScanSpec(columns, batchBytes, batchBytes, 65536, 3 << 20, rowLimit, TimeValue.timeValueMinutes(1));
        return state(spec, computeBudgetNanos, sender);
    }

    private ScanState state(ArrowScanSpec spec, long computeBudgetNanos, ScanState.BatchSender sender) {
        return new ScanState(spec, allocator, field -> true, sender, waitNanos -> {
            slotsHeld.incrementAndGet();
            return slotsHeld::decrementAndGet;
        }, () -> false, computeBudgetNanos, null);
    }

    /** Checks the rows the scan left for the end of its stream, and releases them. */
    private static void assertRowsUnsent(ScanState state, int rowsUnsent) {
        int rows = 0;
        for (VectorSchemaRoot batch : state.takeRemainingRows()) {
            try (batch) {
                rows += batch.getRowCount();
            }
        }
        assertEquals(rowsUnsent, rows);
    }

    private static QueryShardContext context() {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(COLUMN.name())).thenReturn(new KeywordFieldMapper.KeywordFieldType(COLUMN.name()));
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getMapperService()).thenReturn(mapperService);
        return context;
    }

    @FunctionalInterface
    private interface ReaderTest {
        void run(List<LeafReaderContext> leaves) throws Exception;
    }

    /** Runs {@code test} on one segment per entry of {@code docsPerSegment}, with that many docs, each with a value of the column. */
    private static void withSegments(int[] docsPerSegment, ReaderTest test) throws Exception {
        withSegments(docsPerSegment, doc -> List.of(new BytesRef("v" + doc)), test);
    }

    /** As above, with {@code values} giving the column's values for each doc of a segment. */
    private static void withSegments(int[] docsPerSegment, IntFunction<List<BytesRef>> values, ReaderTest test) throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int docs : docsPerSegment) {
                    for (int doc = 0; doc < docs; doc++) {
                        Document document = new Document();
                        for (BytesRef value : values.apply(doc)) {
                            document.add(new SortedSetDocValuesField(COLUMN.name(), value));
                        }
                        writer.addDocument(document);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertEquals(docsPerSegment.length, reader.leaves().size());
                test.run(reader.leaves());
            }
        }
    }
}
