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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.repositories.fs;

import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class FsRepositoryTests extends OpenSearchTestCase {

    public void testSnapshotAndRestore() throws IOException, InterruptedException {
        ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
        try (Directory directory = newDirectory()) {
            Path repo = createTempDir();
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
                .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
                .put("location", repo)
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                .put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path")
                .build();

            int numDocs = indexDocs(directory);
            RepositoryMetadata metadata = new RepositoryMetadata("test", "fs", settings);
            FsRepository repository = new FsRepository(
                metadata,
                new Environment(settings, null),
                NamedXContentRegistry.EMPTY,
                BlobStoreTestUtil.mockClusterService(),
                new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
            );
            repository.start();
            final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_INDEX_UUID, "myindexUUID").build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("myindex", indexSettings);
            ShardId shardId = new ShardId(idxSettings.getIndex(), 1);
            Store store = new Store(shardId, idxSettings, directory, new DummyShardLock(shardId));
            SnapshotId snapshotId = new SnapshotId("test", "test");
            IndexId indexId = new IndexId(idxSettings.getIndex().getName(), idxSettings.getUUID());

            IndexCommit indexCommit = Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory());
            final PlainActionFuture<String> future1 = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> {
                IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(null);
                repository.snapshotShard(
                    store,
                    null,
                    snapshotId,
                    indexId,
                    indexCommit,
                    null,
                    snapshotStatus,
                    Version.CURRENT,
                    Collections.emptyMap(),
                    future1
                );
                future1.actionGet();
                IndexShardSnapshotStatus.Copy copy = snapshotStatus.asCopy();
                assertEquals(copy.getTotalFileCount(), copy.getIncrementalFileCount());
            });
            final String shardGeneration = future1.actionGet();
            Lucene.cleanLuceneIndex(directory);
            expectThrows(org.apache.lucene.index.IndexNotFoundException.class, () -> Lucene.readSegmentInfos(directory));
            DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
            ShardRouting routing = ShardRouting.newUnassigned(
                shardId,
                true,
                new RecoverySource.SnapshotRecoverySource("test", new Snapshot("foo", snapshotId), Version.CURRENT, indexId),
                new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, "")
            );
            routing = ShardRoutingHelper.initialize(routing, localNode.getId(), 0);
            RecoveryState state = new RecoveryState(routing, localNode, null);
            final PlainActionFuture<Void> futureA = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> repository.restoreShard(store, snapshotId, indexId, shardId, state, futureA));
            futureA.actionGet();
            assertTrue(state.getIndex().recoveredBytes() > 0);
            assertEquals(0, state.getIndex().reusedFileCount());
            assertEquals(indexCommit.getFileNames().size(), state.getIndex().recoveredFileCount());
            assertEquals(numDocs, Lucene.readSegmentInfos(directory).totalMaxDoc());
            deleteRandomDoc(store.directory());
            SnapshotId incSnapshotId = new SnapshotId("test1", "test1");
            IndexCommit incIndexCommit = Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory());
            Collection<String> commitFileNames = incIndexCommit.getFileNames();
            final PlainActionFuture<String> future2 = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> {
                IndexShardSnapshotStatus snapshotStatus = IndexShardSnapshotStatus.newInitializing(shardGeneration);
                repository.snapshotShard(
                    store,
                    null,
                    incSnapshotId,
                    indexId,
                    incIndexCommit,
                    null,
                    snapshotStatus,
                    Version.CURRENT,
                    Collections.emptyMap(),
                    future2
                );
                future2.actionGet();
                IndexShardSnapshotStatus.Copy copy = snapshotStatus.asCopy();
                assertEquals(2, copy.getIncrementalFileCount());
                assertEquals(commitFileNames.size(), copy.getTotalFileCount());
            });

            // roll back to the first snap and then incrementally restore
            RecoveryState firstState = new RecoveryState(routing, localNode, null);
            final PlainActionFuture<Void> futureB = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> repository.restoreShard(store, snapshotId, indexId, shardId, firstState, futureB));
            futureB.actionGet();
            assertEquals(
                "should reuse everything except of .liv and .si",
                commitFileNames.size() - 2,
                firstState.getIndex().reusedFileCount()
            );

            RecoveryState secondState = new RecoveryState(routing, localNode, null);
            final PlainActionFuture<Void> futureC = PlainActionFuture.newFuture();
            runGeneric(threadPool, () -> repository.restoreShard(store, incSnapshotId, indexId, shardId, secondState, futureC));
            futureC.actionGet();
            assertEquals(secondState.getIndex().reusedFileCount(), commitFileNames.size() - 2);
            assertEquals(secondState.getIndex().recoveredFileCount(), 2);
            List<ReplicationLuceneIndex.FileMetadata> recoveredFiles = secondState.getIndex()
                .fileDetails()
                .stream()
                .filter(f -> f.reused() == false)
                .collect(Collectors.toList());
            Collections.sort(recoveredFiles, Comparator.comparing(ReplicationLuceneIndex.FileMetadata::name));
            assertTrue(recoveredFiles.get(0).name(), recoveredFiles.get(0).name().endsWith(".liv"));
            assertTrue(recoveredFiles.get(1).name(), recoveredFiles.get(1).name().endsWith("segments_" + incIndexCommit.getGeneration()));
        } finally {
            terminate(threadPool);
        }
    }

    private void runGeneric(ThreadPool threadPool, Runnable runnable) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        threadPool.generic().submit(() -> {
            try {
                runnable.run();
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    private void deleteRandomDoc(Directory directory) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec())
                    .setMergePolicy(new FilterMergePolicy(NoMergePolicy.INSTANCE) {
                        @Override
                        public boolean keepFullyDeletedSegment(IOSupplier<CodecReader> readerIOSupplier) {
                            return true;
                        }

                    })
            )
        ) {
            final int numDocs = writer.getDocStats().numDocs;
            writer.deleteDocuments(new Term("id", "" + randomIntBetween(0, writer.getDocStats().numDocs - 1)));
            writer.commit();
            assertEquals(writer.getDocStats().numDocs, numDocs - 1);
        }
    }

    private int indexDocs(Directory directory) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                directory,
                newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec())
            )
        ) {
            int docs = 1 + random().nextInt(100);
            for (int i = 0; i < docs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                doc.add(
                    new TextField(
                        "body",
                        TestUtil.randomRealisticUnicodeString(random()),
                        random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                    )
                );
                doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
                writer.addDocument(doc);
            }
            writer.commit();
            return docs;
        }
    }

}
