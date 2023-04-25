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

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
}
