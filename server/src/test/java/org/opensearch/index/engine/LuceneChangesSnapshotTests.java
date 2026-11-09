/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.SnapshotMatchers;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class LuceneChangesSnapshotTests extends EngineTestCase {
    private MapperService mapperService;

    @Before
    public void createMapper() throws Exception {
        mapperService = createMapperService();
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) // always enable soft-deletes
            .build();
    }

    public void testBasics() throws Exception {
        long fromSeqNo = randomNonNegativeLong();
        long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        // Empty engine
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, true, randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(
                error.getMessage(),
                containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
            );
        }
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, false, randomBoolean())) {
            assertThat(snapshot, SnapshotMatchers.size(0));
        }
        int numOps = between(1, 100);
        int refreshedSeqNo = -1;
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(i, i + 5));
            ParsedDocument doc = createParsedDoc(id, null, randomBoolean());
            if (randomBoolean()) {
                engine.index(indexForDoc(doc));
            } else {
                engine.delete(new Engine.Delete(doc.id(), newUid(doc.id()), primaryTerm.get()));
            }
            if (rarely()) {
                if (randomBoolean()) {
                    engine.flush();
                } else {
                    engine.refresh("test");
                }
                refreshedSeqNo = i;
            }
        }
        if (refreshedSeqNo == -1) {
            fromSeqNo = between(0, numOps);
            toSeqNo = randomLongBetween(fromSeqNo, numOps * 2);

            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    false,
                    randomBoolean()
                )
            ) {
                searcher = null;
                assertThat(snapshot, SnapshotMatchers.size(0));
            } finally {
                IOUtils.close(searcher);
            }

            searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    true,
                    randomBoolean()
                )
            ) {
                searcher = null;
                IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
                assertThat(
                    error.getMessage(),
                    containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
                );
            } finally {
                IOUtils.close(searcher);
            }
        } else {
            fromSeqNo = randomLongBetween(0, refreshedSeqNo);
            toSeqNo = randomLongBetween(refreshedSeqNo + 1, numOps * 2);
            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    false,
                    randomBoolean()
                )
            ) {
                searcher = null;
                assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(fromSeqNo, refreshedSeqNo));
            } finally {
                IOUtils.close(searcher);
            }
            searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    true,
                    randomBoolean()
                )
            ) {
                searcher = null;
                IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
                assertThat(
                    error.getMessage(),
                    containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
                );
            } finally {
                IOUtils.close(searcher);
            }
            toSeqNo = randomLongBetween(fromSeqNo, refreshedSeqNo);
            searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                Translog.Snapshot snapshot = new LuceneChangesSnapshot(
                    searcher,
                    between(1, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE),
                    fromSeqNo,
                    toSeqNo,
                    true,
                    randomBoolean()
                )
            ) {
                searcher = null;
                assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(fromSeqNo, toSeqNo));
            } finally {
                IOUtils.close(searcher);
            }
        }
        // Get snapshot via engine will auto refresh
        fromSeqNo = randomLongBetween(0, numOps - 1);
        toSeqNo = randomLongBetween(fromSeqNo, numOps - 1);
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, randomBoolean(), randomBoolean())) {
            assertThat(snapshot, SnapshotMatchers.containsSeqNoRange(fromSeqNo, toSeqNo));
        }
    }

    /**
     * A nested document is indexed into Lucene as multiple documents. While the root document has both sequence number and primary term,
     * non-root documents don't have primary term but only sequence numbers. This test verifies that {@link LuceneChangesSnapshot}
     * correctly skip non-root documents and returns at most one operation per sequence number.
     */
    public void testSkipNonRootOfNestedDocuments() throws Exception {
        Map<Long, Long> seqNoToTerm = new HashMap<>();
        List<Engine.Operation> operations = generateHistoryOnReplica(between(1, 100), randomBoolean(), randomBoolean(), randomBoolean());
        for (Engine.Operation op : operations) {
            if (engine.getLocalCheckpointTracker().hasProcessed(op.seqNo()) == false) {
                seqNoToTerm.put(op.seqNo(), op.primaryTerm());
            }
            applyOperation(engine, op);
            if (rarely()) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.translogManager().rollTranslogGeneration();
            }
            if (rarely()) {
                engine.flush();
            }
        }
        long maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        final boolean accurateCount = randomBoolean();
        try (Translog.Snapshot snapshot = new LuceneChangesSnapshot(searcher, between(1, 100), 0, maxSeqNo, false, accurateCount)) {
            if (accurateCount == true) {
                assertThat(snapshot.totalOperations(), equalTo(seqNoToTerm.size()));
            }
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                assertThat(op.toString(), op.primaryTerm(), equalTo(seqNoToTerm.get(op.seqNo())));
            }
            assertThat(snapshot.skippedOperations(), equalTo(0));
        }
    }

    public void testUpdateAndReadChangesConcurrently() throws Exception {
        Follower[] followers = new Follower[between(1, 3)];
        CountDownLatch readyLatch = new CountDownLatch(followers.length + 1);
        AtomicBoolean isDone = new AtomicBoolean();
        for (int i = 0; i < followers.length; i++) {
            followers[i] = new Follower(engine, isDone, readyLatch);
            followers[i].start();
        }
        boolean onPrimary = randomBoolean();
        List<Engine.Operation> operations = new ArrayList<>();
        int numOps = scaledRandomIntBetween(1, 1000);
        for (int i = 0; i < numOps; i++) {
            String id = Integer.toString(randomIntBetween(1, 10));
            ParsedDocument doc = createParsedDoc(id, randomAlphaOfLengthBetween(1, 5), randomBoolean());
            final Engine.Operation op;
            if (onPrimary) {
                if (randomBoolean()) {
                    op = new Engine.Index(newUid(doc), primaryTerm.get(), doc);
                } else {
                    op = new Engine.Delete(doc.id(), newUid(doc.id()), primaryTerm.get());
                }
            } else {
                if (randomBoolean()) {
                    op = replicaIndexForDoc(doc, randomNonNegativeLong(), i, randomBoolean());
                } else {
                    op = replicaDeleteForDoc(doc.id(), randomNonNegativeLong(), i, randomNonNegativeLong());
                }
            }
            operations.add(op);
        }
        readyLatch.countDown();
        readyLatch.await();
        concurrentlyApplyOps(operations, engine);
        assertThat(engine.getLocalCheckpointTracker().getProcessedCheckpoint(), equalTo(operations.size() - 1L));
        isDone.set(true);
        for (Follower follower : followers) {
            follower.join();
            IOUtils.close(follower.engine, follower.engine.store);
        }
    }

    class Follower extends Thread {
        private final InternalEngine leader;
        private final InternalEngine engine;
        private final TranslogHandler translogHandler;
        private final AtomicBoolean isDone;
        private final CountDownLatch readLatch;

        Follower(InternalEngine leader, AtomicBoolean isDone, CountDownLatch readLatch) throws IOException {
            this.leader = leader;
            this.isDone = isDone;
            this.readLatch = readLatch;
            this.engine = createEngine(createStore(), createTempDir());
            this.translogHandler = new TranslogHandler(
                xContentRegistry(),
                IndexSettingsModule.newIndexSettings(shardId.getIndexName(), leader.engineConfig.getIndexSettings().getSettings()),
                engine
            );
        }

        void pullOperations(InternalEngine follower) throws IOException {
            long leaderCheckpoint = leader.getLocalCheckpointTracker().getProcessedCheckpoint();
            long followerCheckpoint = follower.getLocalCheckpointTracker().getProcessedCheckpoint();
            if (followerCheckpoint < leaderCheckpoint) {
                long fromSeqNo = followerCheckpoint + 1;
                long batchSize = randomLongBetween(0, 100);
                long toSeqNo = Math.min(fromSeqNo + batchSize, leaderCheckpoint);
                try (Translog.Snapshot snapshot = leader.newChangesSnapshot("test", fromSeqNo, toSeqNo, true, randomBoolean())) {
                    translogHandler.run(snapshot);
                }
            }
        }

        @Override
        public void run() {
            try {
                readLatch.countDown();
                readLatch.await();
                while (isDone.get() == false
                    || engine.getLocalCheckpointTracker().getProcessedCheckpoint() < leader.getLocalCheckpointTracker()
                        .getProcessedCheckpoint()) {
                    pullOperations(engine);
                }
                assertConsistentHistoryBetweenTranslogAndLuceneIndex(engine);
                // have to verify without source since we are randomly testing without _source
                List<DocIdSeqNoAndSource> docsWithoutSourceOnFollower = getDocIds(engine, true).stream()
                    .map(d -> new DocIdSeqNoAndSource(d.getId(), null, d.getSeqNo(), d.getPrimaryTerm(), d.getVersion()))
                    .collect(Collectors.toList());
                List<DocIdSeqNoAndSource> docsWithoutSourceOnLeader = getDocIds(leader, true).stream()
                    .map(d -> new DocIdSeqNoAndSource(d.getId(), null, d.getSeqNo(), d.getPrimaryTerm(), d.getVersion()))
                    .collect(Collectors.toList());
                assertThat(docsWithoutSourceOnFollower, equalTo(docsWithoutSourceOnLeader));
            } catch (Exception ex) {
                throw new AssertionError(ex);
            }
        }
    }

    private List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
        List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            final Translog.Operation newOp = op;
            logger.error("Reading [{}]", op);
            assert operations.stream().allMatch(o -> o.seqNo() < newOp.seqNo()) : "Operations [" + operations + "], op [" + op + "]";
            operations.add(newOp);
        }
        return operations;
    }

    /**
     * Verifies that routing values round-trip correctly through Translog.Delete serialization.
     */
    public void testDeleteRoutingSerialization() throws Exception {
        final String routingValue = "tenant-abc";

        // Delete WITH routing
        Translog.Delete deleteWithRouting = new Translog.Delete("doc-1", 1, 1, 1, routingValue);
        assertThat(deleteWithRouting.routing(), equalTo(routingValue));
        assertThat(deleteWithRouting.id(), equalTo("doc-1"));

        // Round-trip through Engine.Delete → Translog.Delete
        Engine.Delete engineDelete = new Engine.Delete(
            "doc-2",
            newUid("doc-2"),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0,
            routingValue
        );
        assertThat("Engine.Delete should carry routing", engineDelete.routing(), equalTo(routingValue));

        // Delete WITHOUT routing (backward compatibility)
        Translog.Delete deleteNoRouting = new Translog.Delete("doc-3", 2, 1, 1);
        assertNull("Delete without routing should have null routing", deleteNoRouting.routing());

        // Verify toString includes routing
        assertThat(deleteWithRouting.toString(), containsString("routing=" + routingValue));
        assertThat(deleteNoRouting.toString(), not(containsString("routing=")));
    }

    public void testOverFlow() throws Exception {
        long fromSeqNo = randomLongBetween(0, 5);
        long toSeqNo = randomLongBetween(Long.MAX_VALUE - 5, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", fromSeqNo, toSeqNo, true, randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(
                error.getMessage(),
                containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
            );
        }
    }

    public void testOutOfOrderSeqNoUsesDefaultStoredFieldsReader() throws Exception {
        final int numOps = between(LuceneChangesSnapshot.MIN_SEQUENTIAL_ACCESS_BATCH_SIZE, 100);
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int i = 0; i < numOps; i++) {
            final long seqNo = numOps - 1 - i;
            final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
            engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
            expectedDocs.put(seqNo, doc);
        }
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try (LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(searcher, numOps, 0, numOps - 1, true, true)) {
            searcher = null;
            assertThat(snapshot.totalOperations(), equalTo(numOps));
            // the flag is computed for the first batch by the constructor; a single batch holds every op here
            assertFalse("descending docIDs must not use the sequential reader", snapshot.useSequentialStoredFieldsReader());
            assertOpsMatch(drainAll(snapshot), expectedDocs);
        } finally {
            IOUtils.close(searcher);
        }
    }

    public void testInterleavedSeqNosAcrossSegmentsUseDefaultStoredFieldsReader() throws Exception {
        final int numOps = 2 * between(LuceneChangesSnapshot.MIN_SEQUENTIAL_ACCESS_BATCH_SIZE, 50);
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int step = 0; step < 2; step++) {
            for (long seqNo = step; seqNo < numOps; seqNo += 2) {
                final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
                engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
                expectedDocs.put(seqNo, doc);
            }
            engine.flush();
        }
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try (LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(searcher, numOps, 0, numOps - 1, true, true)) {
            searcher = null;
            assertThat(snapshot.totalOperations(), equalTo(numOps));
            assertFalse("interleaved segments must not use the sequential reader", snapshot.useSequentialStoredFieldsReader());
            assertOpsMatch(drainAll(snapshot), expectedDocs);
        } finally {
            IOUtils.close(searcher);
        }
    }

    public void testContiguousReadsReuseSequentialStoredFieldsReader() throws Exception {
        final int batchSize = LuceneChangesSnapshot.MIN_SEQUENTIAL_ACCESS_BATCH_SIZE;
        final int numOps = batchSize * between(3, 10);
        final Map<Long, ParsedDocument> expectedDocs = indexAppendOnly(numOps);
        final AtomicInteger acquisitions = new AtomicInteger();
        final AtomicInteger documentsRead = new AtomicInteger();
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try {
            final int numLeaves = searcher.getDirectoryReader().leaves().size();
            // a batch size well below the op count forces multiple batches, exercising reader reuse across them
            final Engine.Searcher countingSearcher = countingSearcher(searcher, acquisitions, documentsRead);
            try (LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(countingSearcher, batchSize, 0, numOps - 1, true, true)) {
                searcher = null; // closed by the snapshot through the counting searcher
                assertTrue("contiguous docIDs must use the sequential reader", snapshot.useSequentialStoredFieldsReader());
                assertOpsMatch(drainAll(snapshot), expectedDocs);
                assertThat("sequential reader must be acquired once per leaf and reused", acquisitions.get(), equalTo(numLeaves));
                assertThat("every read must go through the SequentialStoredFieldsLeafReader", documentsRead.get(), equalTo(numOps));
            }
        } finally {
            IOUtils.close(searcher);
        }
    }

    public void testSequentialStoredFieldsReaderIsReacquiredOnThreadChange() throws Exception {
        final int numOps = between(LuceneChangesSnapshot.MIN_SEQUENTIAL_ACCESS_BATCH_SIZE, 50);
        final Map<Long, ParsedDocument> expectedDocs = indexAppendOnly(numOps);
        final AtomicInteger acquisitions = new AtomicInteger();
        final AtomicInteger documentsRead = new AtomicInteger();
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try {
            final Engine.Searcher countingSearcher = countingSearcher(searcher, acquisitions, documentsRead);
            try (LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(countingSearcher, numOps, 0, numOps - 1, true, true)) {
                searcher = null; // closed by the snapshot through the counting searcher
                assertTrue(snapshot.useSequentialStoredFieldsReader());
                final List<Translog.Operation> ops = new ArrayList<>();
                // every next() runs on a fresh thread; join() gives the happens-before that consumers get from locking
                Translog.Operation op = callOnNewThread(snapshot::next);
                while (op != null) {
                    ops.add(op);
                    op = callOnNewThread(snapshot::next);
                }
                assertOpsMatch(ops, expectedDocs);
                assertThat("reader must be re-acquired for every reading thread", acquisitions.get(), equalTo(numOps));
                assertThat(documentsRead.get(), equalTo(numOps));
            }
        } finally {
            IOUtils.close(searcher);
        }
    }

    private Map<Long, ParsedDocument> indexAppendOnly(int numOps) throws IOException {
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int i = 0; i < numOps; i++) {
            final ParsedDocument doc = createParsedDoc("id-" + i, null);
            final Engine.IndexResult result = engine.index(indexForDoc(doc));
            expectedDocs.put(result.getSeqNo(), doc);
        }
        engine.refresh("test");
        return expectedDocs;
    }

    private void assertOpsMatch(List<Translog.Operation> ops, Map<Long, ParsedDocument> expectedDocs) {
        assertThat(ops, hasSize(expectedDocs.size()));
        for (Translog.Operation op : ops) {
            assertThat(op.toString(), op, instanceOf(Translog.Index.class));
            final Translog.Index index = (Translog.Index) op;
            final ParsedDocument expected = expectedDocs.get(op.seqNo());
            assertNotNull("unexpected seqNo [" + op.seqNo() + "]", expected);
            assertThat(index.id(), equalTo(expected.id()));
            assertThat(index.source(), equalTo(expected.source()));
        }
    }

    private static <T> T callOnNewThread(CheckedSupplier<T, Exception> supplier) throws Exception {
        final AtomicReference<T> result = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                result.set(supplier.get());
            } catch (Exception e) {
                failure.set(e);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw failure.get();
        }
        return result.get();
    }

    /**
     * Wraps the searcher's reader so that every leaf sits behind a counting {@link SequentialStoredFieldsLeafReader}.
     * The returned searcher takes ownership of {@code searcher}.
     */
    private static Engine.Searcher countingSearcher(Engine.Searcher searcher, AtomicInteger acquisitions, AtomicInteger documentsRead)
        throws IOException {
        final DirectoryReader reader = new CountingSequentialDirectoryReader(searcher.getDirectoryReader(), acquisitions, documentsRead);
        return new Engine.Searcher(
            searcher.source(),
            reader,
            searcher.getSimilarity(),
            searcher.getQueryCache(),
            searcher.getQueryCachingPolicy(),
            searcher
        );
    }

    /**
     * Wraps every leaf in a pass-through {@link SequentialStoredFieldsLeafReader} that counts how many times its
     * sequential stored fields reader is acquired, and how many documents that reader serves.
     */
    private static final class CountingSequentialDirectoryReader extends FilterDirectoryReader {
        private final AtomicInteger acquisitions;
        private final AtomicInteger documentsRead;

        CountingSequentialDirectoryReader(DirectoryReader in, AtomicInteger acquisitions, AtomicInteger documentsRead) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new SequentialStoredFieldsLeafReader(reader) {
                        @Override
                        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader storedFieldsReader) {
                            acquisitions.incrementAndGet();
                            return new CountingStoredFieldsReader(storedFieldsReader, documentsRead);
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return reader.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return reader.getReaderCacheHelper();
                        }
                    };
                }
            });
            this.acquisitions = acquisitions;
            this.documentsRead = documentsRead;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new CountingSequentialDirectoryReader(in, acquisitions, documentsRead);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static final class CountingStoredFieldsReader extends StoredFieldsReader {
        private final StoredFieldsReader delegate;
        private final AtomicInteger documentsRead;

        CountingStoredFieldsReader(StoredFieldsReader delegate, AtomicInteger documentsRead) {
            this.delegate = delegate;
            this.documentsRead = documentsRead;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            documentsRead.incrementAndGet();
            delegate.document(docID, visitor);
        }

        @Override
        public StoredFieldsReader clone() {
            return new CountingStoredFieldsReader(delegate.clone(), documentsRead);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
