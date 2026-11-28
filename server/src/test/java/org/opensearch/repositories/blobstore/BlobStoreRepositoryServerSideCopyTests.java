/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.ServerSideCopyBlobContainer;
import org.opensearch.common.blobstore.ServerSideCopySource;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * End-to-end cover for having the storage provider copy a shard's segments into the repository instead of uploading
 * their bytes, driven through {@link BlobStoreRepository#snapshotShard}.
 *
 * <p>The repository is a real {@link FsRepository} whose containers are made copy-capable, so the decision of what to
 * copy and what to upload is exercised for real. The copy itself is performed locally; the provider mechanics of
 * {@code CopyObject} are covered by the repository-s3 tests.
 */
public class BlobStoreRepositoryServerSideCopyTests extends OpenSearchTestCase {

    private static final String SOURCE_PREFIX = "remote/segments/data";
    private static final String BLOB_SUFFIX = "__u1";

    /** Records what the repository did, so a test can tell a copy from an upload. */
    private static final class CopyLedger {
        /** Contents of the fake remote store, keyed by resolved source key. */
        private final Map<String, byte[]> sourceObjects = new HashMap<>();
        /** Destination blob names created by a server-side copy. */
        private final Set<String> copied = new HashSet<>();
        /** Destination blob names created by an upload through this process. */
        private final Set<String> uploaded = new HashSet<>();
    }

    private static final class LedgerFsBlobContainer extends FsBlobContainer implements ServerSideCopyBlobContainer {

        private final CopyLedger ledger;

        LedgerFsBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, CopyLedger ledger) {
            super(blobStore, blobPath, path);
            this.ledger = ledger;
        }

        @Override
        public boolean isServerSideCopyConfigured() {
            return true;
        }

        @Override
        public long maxSingleRequestCopySizeInBytes() {
            return Long.MAX_VALUE;
        }

        @Override
        public ServerSideCopySource asCopySource() {
            throw new IllegalStateException("this container is a copy destination");
        }

        @Override
        public void copyBlobFrom(ServerSideCopySource source, String sourceBlobName, String destinationBlobName) throws IOException {
            final byte[] bytes = ledger.sourceObjects.get(source.resolveKey(sourceBlobName));
            if (bytes == null) {
                throw new NoSuchFileException(sourceBlobName);
            }
            ledger.copied.add(destinationBlobName);
            // super, so this does not register as an upload.
            super.writeBlob(destinationBlobName, new ByteArrayInputStream(bytes), bytes.length, true);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            ledger.uploaded.add(blobName);
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    private static final class LedgerFsBlobStore extends FsBlobStore {

        private final CopyLedger ledger;

        LedgerFsBlobStore(int bufferSizeInBytes, Path path, boolean readonly, CopyLedger ledger) throws IOException {
            super(bufferSizeInBytes, path, readonly);
            this.ledger = ledger;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            try {
                return new LedgerFsBlobContainer(this, path, buildAndCreate(path), ledger);
            } catch (IOException e) {
                throw new BlobStoreException("failed to create blob container", e);
            }
        }
    }

    private static final class LedgerFsRepository extends FsRepository {

        private final CopyLedger ledger;

        LedgerFsRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry registry,
            ClusterService clusterService,
            RecoverySettings recoverySettings,
            CopyLedger ledger
        ) {
            super(metadata, environment, registry, clusterService, recoverySettings);
            this.ledger = ledger;
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new LedgerFsBlobStore(
                Math.toIntExact(ByteSizeUnit.KB.toBytes(8)),
                environment.resolveRepoFile(REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings())),
                isReadOnly(),
                ledger
            );
        }
    }

    /**
     * A copy source describing the fixture's fake remote store. {@code excluded} names are left out of the mapping, as
     * the real remote store leaves out the segments file.
     */
    private RemoteStoreCopySource copySourceFor(Store store, IndexCommit commit, CopyLedger ledger, Set<String> excluded)
        throws IOException {
        final Map<String, UploadedSegmentMetadata> segmentsAtCommit = new HashMap<>();
        for (StoreFileMetadata md : store.getMetadata(commit)) {
            if (md.name().startsWith("segments") || excluded.contains(md.name())) {
                continue;
            }
            final String blobName = md.name() + BLOB_SUFFIX;
            // The remote store records the checksum in base 10; StoreFileMetadata holds base 36.
            final long rawChecksum = Long.parseLong(md.checksum(), Character.MAX_RADIX);
            segmentsAtCommit.put(
                md.name(),
                UploadedSegmentMetadata.fromString(
                    md.name()
                        + "::"
                        + blobName
                        + "::"
                        + rawChecksum
                        + "::"
                        + md.length()
                        + "::"
                        + org.apache.lucene.util.Version.LATEST.major
                )
            );
            ledger.sourceObjects.put(SOURCE_PREFIX + "/" + blobName, readAll(store, md.name(), md.length()));
        }
        return new RemoteStoreCopySource(new ServerSideCopySource("remote-bucket", SOURCE_PREFIX, null), segmentsAtCommit);
    }

    private static byte[] readAll(Store store, String name, long length) throws IOException {
        final byte[] bytes = new byte[Math.toIntExact(length)];
        try (org.apache.lucene.store.IndexInput in = store.directory().openInput(name, org.apache.lucene.store.IOContext.READONCE)) {
            in.readBytes(bytes, 0, bytes.length);
        }
        return bytes;
    }

    private static int indexDocs(Directory directory) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE);
        int numDocs = scaledRandomIntBetween(40, 120);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                doc.add(new TextField("body", randomUnicodeOfLength(32), Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();
        }
        return numDocs;
    }

    private static void runGeneric(ThreadPool threadPool, Runnable runnable) throws InterruptedException {
        final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        threadPool.generic().submit(() -> {
            try {
                runnable.run();
            } finally {
                latch.countDown();
            }
        });
        latch.await();
    }

    private interface SnapshotScenario {
        void run(ThreadPool threadPool, Store store, LedgerFsRepository repository, CopyLedger ledger, IndexCommit commit, int numDocs)
            throws Exception;
    }

    /** Stands up a repository over a freshly indexed directory with a deliberately tiny chunk size. */
    private void withFixture(SnapshotScenario scenario) throws Exception {
        final ThreadPool threadPool = new TestThreadPool(getClass().getSimpleName());
        try (Directory directory = newDirectory()) {
            final Path repo = createTempDir();
            final Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), repo.toAbsolutePath())
                .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
                .put("location", repo)
                // Small enough that anything uploaded would be split into .partN blobs, so a copied file being a
                // single object is a meaningful assertion rather than an accident.
                .put("chunk_size", 200, ByteSizeUnit.BYTES)
                .build();

            final int numDocs = indexDocs(directory);
            final CopyLedger ledger = new CopyLedger();
            final LedgerFsRepository repository = new LedgerFsRepository(
                new RepositoryMetadata("test", "fs", settings),
                new Environment(settings, null),
                NamedXContentRegistry.EMPTY,
                BlobStoreTestUtil.mockClusterService(),
                new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                ledger
            );
            repository.start();

            final IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
                "myindex",
                Settings.builder().put(IndexMetadata.SETTING_INDEX_UUID, "myindexUUID").build()
            );
            final ShardId shardId = new ShardId(idxSettings.getIndex(), 0);
            final Store store = new Store(shardId, idxSettings, directory, new DummyShardLock(shardId));
            final IndexCommit commit = Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory());

            scenario.run(threadPool, store, repository, ledger, commit, numDocs);
        } finally {
            terminate(threadPool);
        }
    }

    private String snapshot(
        ThreadPool threadPool,
        LedgerFsRepository repository,
        Store store,
        IndexCommit commit,
        SnapshotId snapshotId,
        IndexId indexId,
        RemoteStoreCopySource copySource
    ) throws Exception {
        final PlainActionFuture<String> future = PlainActionFuture.newFuture();
        runGeneric(threadPool, () -> {
            final IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(null);
            try {
                repository.snapshotShard(
                    store,
                    null,
                    snapshotId,
                    indexId,
                    store.toCatalogSnapshot(commit),
                    null,
                    status,
                    Version.CURRENT,
                    Collections.emptyMap(),
                    future,
                    null,
                    copySource
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        return future.actionGet();
    }

    /**
     * Data blobs the snapshot itself created, identified by the generated-name prefix the repository uses.
     *
     * <p>Filtering on the prefix rather than excluding the metadata blobs is deliberate: Lucene's {@code ExtrasFS}
     * test filesystem drops a stray {@code extra0} into every directory that gets created, including the shard
     * directory in the repository, and that is not something this test should see.
     */
    private static List<String> snapshotDataBlobs(BlobContainer shardContainer) throws IOException {
        final List<String> names = new ArrayList<>(shardContainer.listBlobs().keySet());
        names.removeIf(n -> n.startsWith("__") == false);
        Collections.sort(names);
        return names;
    }

    public void testSegmentsAreCopiedRatherThanUploadedAndStillRestore() throws Exception {
        withFixture((threadPool, store, repository, ledger, commit, numDocs) -> {
            final SnapshotId snapshotId = new SnapshotId("snap", "snap-uuid");
            final IndexId indexId = new IndexId("myindex", "myindexUUID");
            final RemoteStoreCopySource copySource = copySourceFor(store, commit, ledger, emptySet());

            snapshot(threadPool, repository, store, commit, snapshotId, indexId, copySource);

            final BlobContainer shardContainer = repository.shardContainer(indexId, 0);
            final List<String> blobs = snapshotDataBlobs(shardContainer);
            assertFalse("expected at least one data blob", blobs.isEmpty());

            // Every data blob arrived by copy, and none of them by upload.
            assertEquals("all data blobs should have been copied", new HashSet<>(blobs), ledger.copied);
            for (String blob : blobs) {
                assertFalse("copied blob should not also have been uploaded: " + blob, ledger.uploaded.contains(blob));
            }

            // One source object becomes one destination object, despite a 200 byte chunk size.
            for (String blob : blobs) {
                assertFalse("copied file must not be split into parts: " + blob, blob.contains(".part"));
            }

            // The only uploads are the shard's own metadata blobs.
            for (String uploaded : ledger.uploaded) {
                assertTrue("unexpected upload: " + uploaded, uploaded.startsWith("index-") || uploaded.startsWith("snap-"));
            }

            restoreAndAssert(threadPool, repository, store, snapshotId, indexId, numDocs);
        });
    }

    public void testFileMissingFromTheRemoteStoreIsUploadedInstead() throws Exception {
        withFixture((threadPool, store, repository, ledger, commit, numDocs) -> {
            final SnapshotId snapshotId = new SnapshotId("snap", "snap-uuid");
            final IndexId indexId = new IndexId("myindex", "myindexUUID");

            // Pick a real data file and withhold it from the mapping, as the remote store withholds segments_N.
            final String withheld = store.getMetadata(commit)
                .asMap()
                .keySet()
                .stream()
                .filter(n -> n.startsWith("segments") == false)
                .sorted()
                .findFirst()
                .orElseThrow();
            final RemoteStoreCopySource copySource = copySourceFor(store, commit, ledger, Set.of(withheld));

            snapshot(threadPool, repository, store, commit, snapshotId, indexId, copySource);

            assertFalse("something should still have been copied", ledger.copied.isEmpty());
            // The withheld file had to be uploaded, and with a 200 byte chunk size that means .partN blobs.
            final List<String> parts = new ArrayList<>(ledger.uploaded);
            parts.removeIf(n -> n.contains(".part") == false);
            assertFalse("the uploaded file should have been chunked", parts.isEmpty());

            restoreAndAssert(threadPool, repository, store, snapshotId, indexId, numDocs);
        });
    }

    public void testWithoutACopySourceEverythingIsUploaded() throws Exception {
        withFixture((threadPool, store, repository, ledger, commit, numDocs) -> {
            final SnapshotId snapshotId = new SnapshotId("snap", "snap-uuid");
            final IndexId indexId = new IndexId("myindex", "myindexUUID");

            snapshot(threadPool, repository, store, commit, snapshotId, indexId, null);

            assertTrue("nothing should have been copied", ledger.copied.isEmpty());
            assertFalse("everything should have been uploaded", ledger.uploaded.isEmpty());

            restoreAndAssert(threadPool, repository, store, snapshotId, indexId, numDocs);
        });
    }

    private void restoreAndAssert(
        ThreadPool threadPool,
        LedgerFsRepository repository,
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        int numDocs
    ) throws Exception {
        final Directory directory = store.directory();
        Lucene.cleanLuceneIndex(directory);
        expectThrows(org.apache.lucene.index.IndexNotFoundException.class, () -> Lucene.readSegmentInfos(directory));

        final DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        ShardRouting routing = ShardRouting.newUnassigned(
            store.shardId(),
            true,
            new RecoverySource.SnapshotRecoverySource("restore", new Snapshot("test", snapshotId), Version.CURRENT, indexId),
            new UnassignedInfo(UnassignedInfo.Reason.EXISTING_INDEX_RESTORED, "")
        );
        routing = ShardRoutingHelper.initialize(routing, localNode.getId(), 0);
        final RecoveryState state = new RecoveryState(routing, localNode, null);

        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        runGeneric(threadPool, () -> repository.restoreShard(store, snapshotId, indexId, store.shardId(), state, future));
        future.actionGet();

        // The point of the whole exercise: what the provider copied is restorable.
        assertEquals(numDocs, Lucene.readSegmentInfos(directory).totalMaxDoc());
    }
}
