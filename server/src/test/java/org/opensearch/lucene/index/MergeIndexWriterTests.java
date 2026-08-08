/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PreparableOneMerge;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.IOFunction;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

/**
 * Unit tests for {@link MergeIndexWriter}'s two-phase prepare/execute merge flow and
 * {@link PreparableOneMerge}.
 */
public class MergeIndexWriterTests extends OpenSearchTestCase {

    private Directory directory;
    private MergeIndexWriter writer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        writer = new MergeIndexWriter(directory, newConfig());
    }

    @Override
    public void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }

    private IndexWriterConfig newConfig() {
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergeScheduler(new SerialMergeScheduler());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        return iwc;
    }

    // ========== prepareMerge: frozen live-docs snapshot ==========

    /** prepareMerge exposes the frozen hardLiveDocs of segments with deletes; delete-free segments are absent. */
    public void testPrepareMergeExposesFrozenLiveDocs() throws IOException {
        addSegment("a", 3); // docids 0..2, ids a_0..a_2
        addSegment("b", 2); // no deletes
        writer.deleteDocuments(new Term("id", "a_1"));
        writer.commit(); // applies the delete: doc 1 of segment a is dead

        List<SegmentCommitInfo> segments = liveSegments();
        assertEquals(2, segments.size());

        PreparableOneMerge oneMerge = new PreparableOneMerge(segments);
        long gen = 42L;
        writer.prepareMerge(oneMerge, gen);
        try {
            assertTrue(oneMerge.readersInitialized());

            Map<Long, long[]> packed = oneMerge.packLiveDocsByGeneration(bySegmentOrdinal(segments));
            assertEquals("only the segment with deletes contributes a bitmap", 1, packed.size());

            long[] bits = packed.get(0L);
            assertNotNull("segment a (ordinal 0) has deletes", bits);
            assertEquals(1, bits.length);
            assertEquals("docs 0 and 2 alive, doc 1 dead", 0b101L, bits[0]);
        } finally {
            writer.abortPreparedMerge(gen);
        }
    }

    /** The snapshot captured by prepareMerge does not observe deletes applied after preparation. */
    public void testPrepareMergeSnapshotIsFrozenAgainstLaterDeletes() throws IOException {
        addSegment("a", 3);
        addSegment("b", 2);
        writer.deleteDocuments(new Term("id", "a_1"));
        writer.commit();

        List<SegmentCommitInfo> segments = liveSegments();
        PreparableOneMerge oneMerge = new PreparableOneMerge(segments);
        long gen = 42L;
        writer.prepareMerge(oneMerge, gen);

        Map<Long, long[]> before = oneMerge.packLiveDocsByGeneration(bySegmentOrdinal(segments));

        // A delete arriving between prepare and execute must not alter the frozen snapshot.
        writer.deleteDocuments(new Term("id", "b_0"));
        DirectoryReader.open(writer).close(); // force-apply buffered deletes

        Map<Long, long[]> after = oneMerge.packLiveDocsByGeneration(bySegmentOrdinal(segments));
        assertEquals(before.keySet(), after.keySet());
        for (Long key : before.keySet()) {
            assertArrayEquals("frozen snapshot must not change for ordinal " + key, before.get(key), after.get(key));
        }
        assertNull("segment b's late delete must not appear in the frozen snapshot", after.get(1L));

        // Execute: mergeMiddle physically drops only the frozen-dead doc; the late delete is
        // carried over onto the merged segment as a live-docs delete, not a physical drop.
        PreparableOneMerge taken = writer.takePreparedMerge(gen);
        assertSame(oneMerge, taken);
        writer.executeMerge(taken, gen);
        writer.commit();

        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertEquals("merged maxDoc drops only the frozen-dead doc", 4, reader.maxDoc());
            assertEquals("carried-over delete leaves 3 live docs", 3, reader.numDocs());
        }
    }

    /** Double-prepare for the same generation trips the guard assertion. */
    public void testPrepareMergeRejectsDoublePrepare() throws IOException {
        assumeTrue("requires assertions enabled", MergeIndexWriterTests.class.desiredAssertionStatus());
        addSegment("a", 2);
        addSegment("b", 2);
        writer.commit();

        long gen = 7L;
        writer.prepareMerge(new PreparableOneMerge(liveSegments()), gen);
        try {
            AssertionError err = expectThrows(AssertionError.class, () -> writer.prepareMerge(new PreparableOneMerge(liveSegments()), gen));
            assertTrue(err.getMessage(), err.getMessage().contains("already has a prepared merge"));
        } finally {
            writer.abortPreparedMerge(gen);
        }
    }

    // ========== takePreparedMerge ==========

    /** takePreparedMerge drains the entry: the second call returns null; unknown generations return null. */
    public void testTakePreparedMergeDrainsEntry() throws IOException {
        addSegment("a", 2);
        addSegment("b", 2);
        writer.commit();

        assertNull("nothing prepared yet", writer.takePreparedMerge(5L));

        PreparableOneMerge oneMerge = new PreparableOneMerge(liveSegments());
        writer.prepareMerge(oneMerge, 5L);

        PreparableOneMerge taken = writer.takePreparedMerge(5L);
        assertSame(oneMerge, taken);
        assertNull("entry must be drained", writer.takePreparedMerge(5L));

        // Finish the merge to release reader-pool and deleter refs taken by prepareMerge.
        writer.executeMerge(taken, 5L);
        assertEquals("segments merged into one", 1, liveSegments().size());
    }

    // ========== abortPreparedMerge ==========

    /** abortPreparedMerge releases prepared state; source segments stay intact; unknown generations are a no-op. */
    public void testAbortPreparedMergeReleasesStateAndKeepsSegments() throws IOException {
        addSegment("a", 2);
        addSegment("b", 2);
        writer.commit();

        long gen = 9L;
        writer.prepareMerge(new PreparableOneMerge(liveSegments()), gen);
        writer.abortPreparedMerge(gen);

        assertNull(writer.takePreparedMerge(gen));
        assertEquals("aborted merge must not touch source segments", 2, liveSegments().size());

        // Idempotent / unknown generation: no-op.
        writer.abortPreparedMerge(gen);
        writer.abortPreparedMerge(12345L);

        // The writer must still be fully usable after an abort.
        addSegment("c", 1);
        writer.commit();
        assertEquals(3, liveSegments().size());
    }

    // ========== executeMerge ==========

    /** executeMerge with a fresh (unprepared) OneMerge registers it inline and merges. */
    public void testExecuteMergeWithFreshOneMerge() throws IOException {
        addSegment("a", 3);
        addSegment("b", 2);
        writer.commit();

        PreparableOneMerge oneMerge = new PreparableOneMerge(liveSegments());
        assertFalse(oneMerge.readersInitialized());

        writer.executeMerge(oneMerge, 11L);
        writer.commit();

        assertEquals(1, liveSegments().size());
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertEquals(5, reader.numDocs());
        }
    }

    /** executeMerge after prepareMerge physically drops the frozen-dead docs from the merged segment. */
    public void testExecuteMergeDropsFrozenDeadDocs() throws IOException {
        addSegment("a", 3);
        addSegment("b", 2);
        writer.deleteDocuments(new Term("id", "a_0"), new Term("id", "b_1"));
        writer.commit();

        long gen = 13L;
        writer.prepareMerge(new PreparableOneMerge(liveSegments()), gen);
        writer.executeMerge(writer.takePreparedMerge(gen), gen);
        writer.commit();

        assertEquals(1, liveSegments().size());
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            assertEquals("both deleted docs must be physically dropped", 3, reader.maxDoc());
            assertEquals(3, reader.numDocs());
        }
    }

    // ========== flushLiveDocsForMergedSegment ==========

    /** No pooled reader state for the segment means no-op. */
    public void testFlushLiveDocsForMergedSegmentIsNoOpWithoutPooledState() throws IOException {
        addSegment("a", 2);
        writer.commit();

        SegmentCommitInfo sci = liveSegments().get(0);
        writer.flushLiveDocsForMergedSegment(sci); // must not throw
        assertFalse(sci.hasDeletions());
    }

    /** In-memory deletes on a pooled segment are forced to a .liv file so files() includes it. */
    public void testFlushLiveDocsForMergedSegmentWritesLivFile() throws IOException {
        addSegment("a", 2);
        writer.commit();

        SegmentCommitInfo sci = liveSegments().get(0);
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            // tryDeleteDocument records the delete directly on the pooled ReadersAndUpdates
            // without writing the .liv file — mirroring carryOverHardDeletes after a merge.
            assertTrue(writer.tryDeleteDocument(reader, 0) >= 0);

            assertTrue("no .liv file before the forced flush", sci.files().stream().noneMatch(f -> f.endsWith(".liv")));

            writer.flushLiveDocsForMergedSegment(sci);

            assertTrue("forced flush must persist the .liv file", sci.files().stream().anyMatch(f -> f.endsWith(".liv")));
            assertTrue(sci.hasDeletions());
        }
    }

    // ========== PreparableOneMerge ==========

    /** initMergeReaders is idempotent once prepared — the factory must not be invoked again. */
    public void testPreparableOneMergeInitMergeReadersIsIdempotent() throws Exception {
        addSegment("a", 2);
        addSegment("b", 2);
        writer.commit();

        PreparableOneMerge oneMerge = new PreparableOneMerge(liveSegments());
        long gen = 21L;
        writer.prepareMerge(oneMerge, gen);
        try {
            assertTrue(oneMerge.readersInitialized());
            // A second init must be a no-op: the factory below would fail the test if invoked.
            // (MergePolicy.MergeReader is package-private; erasure lets us pass Object here
            // since the factory unconditionally throws and never produces a value.)
            IOFunction<SegmentCommitInfo, Object> throwingFactory = sci -> {
                throw new IOException("factory must not be re-invoked after prepare");
            };
            invokeInitMergeReaders(oneMerge, throwingFactory);
        } finally {
            writer.abortPreparedMerge(gen);
        }
    }

    /** Invokes the package-private {@code initMergeReaders} via reflection. */
    @SuppressForbidden(reason = "Reflection needed to invoke package-private initMergeReaders from outside org.apache.lucene.index")
    private static void invokeInitMergeReaders(PreparableOneMerge oneMerge, IOFunction<SegmentCommitInfo, Object> factory)
        throws Exception {
        java.lang.reflect.Method m = MergePolicy.OneMerge.class.getDeclaredMethod("initMergeReaders", IOFunction.class);
        m.setAccessible(true);
        try {
            m.invoke(oneMerge, factory);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    /** Before initialization, readersInitialized is false and packLiveDocsByGeneration is empty. */
    public void testPreparableOneMergeBeforeInitialization() throws IOException {
        addSegment("a", 2);
        writer.commit();

        PreparableOneMerge oneMerge = new PreparableOneMerge(liveSegments());
        assertFalse(oneMerge.readersInitialized());
        assertTrue(oneMerge.packLiveDocsByGeneration(sci -> 0L).isEmpty());
    }

    /** Segments whose readers report no deletes are omitted from the packed map. */
    public void testPackLiveDocsByGenerationOmitsDeleteFreeSegments() throws IOException {
        addSegment("a", 2);
        addSegment("b", 2);
        writer.commit();

        PreparableOneMerge oneMerge = new PreparableOneMerge(liveSegments());
        long gen = 33L;
        writer.prepareMerge(oneMerge, gen);
        try {
            assertTrue(oneMerge.packLiveDocsByGeneration(bySegmentOrdinal(liveSegments())).isEmpty());
        } finally {
            writer.abortPreparedMerge(gen);
        }
    }

    // ========== Helpers ==========

    /** Adds {@code numDocs} docs with ids {@code prefix_0..} and flushes them into their own segment. */
    private void addSegment(String prefix, int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", prefix + "_" + i, Field.Store.YES));
            writer.addDocument(doc);
        }
        writer.flush();
    }

    /** Returns the writer's live segments. */
    @SuppressForbidden(reason = "Reflection needed to read IndexWriter's private live SegmentInfos for test assertions")
    private List<SegmentCommitInfo> liveSegments() throws IOException {
        try {
            java.lang.reflect.Field segInfosField = IndexWriter.class.getDeclaredField("segmentInfos");
            segInfosField.setAccessible(true);
            SegmentInfos segmentInfos = (SegmentInfos) segInfosField.get(writer);
            synchronized (writer) {
                return new ArrayList<>(segmentInfos.asList());
            }
        } catch (ReflectiveOperationException e) {
            throw new IOException("Failed to access segmentInfos via reflection", e);
        }
    }

    /** Keys segments by their ordinal in {@code segments}, mirroring a generation lookup. */
    private static ToLongFunction<SegmentCommitInfo> bySegmentOrdinal(List<SegmentCommitInfo> segments) {
        Map<String, Long> byName = new HashMap<>();
        for (int i = 0; i < segments.size(); i++) {
            byName.put(segments.get(i).info.name, (long) i);
        }
        return sci -> byName.getOrDefault(sci.info.name, -1L);
    }
}
