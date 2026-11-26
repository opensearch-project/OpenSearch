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
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests the {@link IndexCommit}-based entry points on {@link BlobStoreRepository}, which now adapt onto the
 * {@link CatalogSnapshot}-based implementations.
 * <p>
 * These adapters are the compatibility path for callers that have not moved to {@link CatalogSnapshot}. They must
 * translate the commit faithfully, pass a {@code null} commit through unchanged for the closed-index path, and
 * report a commit that cannot be read as a snapshot failure rather than letting the {@link java.io.IOException}
 * escape the asynchronous API.
 */
public class BlobStoreRepositoryCatalogAdapterTests extends OpenSearchTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "index-uuid")
            .build()
    );

    private Directory directory;
    private Store store;
    private RecordingFsRepository repository;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
        directory = newDirectory();
        store = new Store(shardId, INDEX_SETTINGS, directory, new DummyShardLock(shardId));
        try (IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("id", "1", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
        }

        final Path repoPath = createTempDir();
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), repoPath.toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .put("location", repoPath)
            .build();
        repository = new RecordingFsRepository(
            new RepositoryMetadata("test", "fs", settings),
            new Environment(settings, null),
            NamedXContentRegistry.EMPTY,
            BlobStoreTestUtil.mockClusterService(),
            new RecoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
    }

    @Override
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
        super.tearDown();
    }

    /**
     * An {@link FsRepository} that intercepts the catalog-based implementations so that the adapters above them can
     * be observed without running a real snapshot.
     */
    private static class RecordingFsRepository extends FsRepository {
        private final AtomicReference<CatalogSnapshot> received = new AtomicReference<>();
        private final AtomicBoolean called = new AtomicBoolean();

        RecordingFsRepository(
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry registry,
            org.opensearch.cluster.service.ClusterService clusterService,
            RecoverySettings recoverySettings
        ) {
            super(metadata, environment, registry, clusterService, recoverySettings);
        }

        @Override
        public void snapshotShard(
            Store store,
            MapperService mapperService,
            SnapshotId snapshotId,
            IndexId indexId,
            CatalogSnapshot catalogSnapshot,
            String shardStateIdentifier,
            IndexShardSnapshotStatus snapshotStatus,
            Version repositoryMetaVersion,
            Map<String, Object> userMetadata,
            ActionListener<String> listener,
            IndexMetadata indexMetadata
        ) {
            called.set(true);
            received.set(catalogSnapshot);
            listener.onResponse("recorded");
        }

        @Override
        public void snapshotRemoteStoreIndexShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            CatalogSnapshot catalogSnapshot,
            String shardStateIdentifier,
            IndexShardSnapshotStatus snapshotStatus,
            long primaryTerm,
            long commitGeneration,
            long startTime,
            Map<String, Long> indexFilesToFileLengthMap,
            ActionListener<String> listener
        ) {
            called.set(true);
            received.set(catalogSnapshot);
            listener.onResponse("recorded");
        }
    }

    private SnapshotId snapshotId() {
        return new SnapshotId("snap", "snap-uuid");
    }

    private IndexId indexId() {
        return new IndexId("index", "index-uuid");
    }

    private IndexShardSnapshotStatus status() {
        return IndexShardSnapshotStatus.newInitializing(null);
    }

    private IndexCommit lastCommit() throws Exception {
        return Lucene.getIndexCommit(store.readLastCommittedSegmentsInfo(), store.directory());
    }

    /** Removes the commit file so that reading the commit point fails with a plain IOException. */
    private void deleteCommitFile() throws Exception {
        store.directory().deleteFile(store.readLastCommittedSegmentsInfo().getSegmentsFileName());
    }

    // ═══════════════════════════════════════════════════════════════
    // snapshotShard(IndexCommit) adapter
    // ═══════════════════════════════════════════════════════════════

    public void testSnapshotShardAdaptsIndexCommitToCatalogSnapshot() throws Exception {
        final IndexCommit commit = lastCommit();

        repository.snapshotShard(
            store,
            mock(MapperService.class),
            snapshotId(),
            indexId(),
            commit,
            null,
            status(),
            Version.CURRENT,
            Map.of(),
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e)),
            null
        );

        assertTrue("the catalog-based implementation must be reached", repository.called.get());
        final CatalogSnapshot forwarded = repository.received.get();
        assertThat(forwarded, notNullValue());
        assertEquals(commit.getGeneration(), forwarded.getGeneration());
        assertEquals(
            "the adapted catalog must describe exactly the files of the commit",
            new HashSet<>(commit.getFileNames()),
            new HashSet<>(forwarded.getFiles(true))
        );
    }

    public void testSnapshotShardReportsUnreadableCommitAsSnapshotFailure() throws Exception {
        final IndexCommit commit = lastCommit();
        deleteCommitFile();

        final AtomicReference<Exception> failure = new AtomicReference<>();
        repository.snapshotShard(
            store,
            mock(MapperService.class),
            snapshotId(),
            indexId(),
            commit,
            null,
            status(),
            Version.CURRENT,
            Map.of(),
            ActionListener.wrap(r -> fail("should not succeed, got " + r), failure::set),
            null
        );

        assertFalse("the catalog-based implementation must not be reached", repository.called.get());
        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("Failed to read commit point"));

        Lucene.cleanLuceneIndex(store.directory());
    }

    // ═══════════════════════════════════════════════════════════════
    // snapshotRemoteStoreIndexShard(IndexCommit) adapter
    // ═══════════════════════════════════════════════════════════════

    public void testSnapshotRemoteStoreShardAdaptsIndexCommitToCatalogSnapshot() throws Exception {
        final IndexCommit commit = lastCommit();

        repository.snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            commit,
            null,
            status(),
            1L,
            commit.getGeneration(),
            0L,
            null,
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e))
        );

        assertTrue("the catalog-based implementation must be reached", repository.called.get());
        final CatalogSnapshot forwarded = repository.received.get();
        assertThat(forwarded, notNullValue());
        assertEquals(commit.getGeneration(), forwarded.getGeneration());
    }

    /**
     * A closed index has no live commit point: the file listing is supplied separately and the commit is
     * {@code null}. The adapter must not invent a catalog snapshot in that case.
     */
    public void testSnapshotRemoteStoreShardPassesNullCommitThroughForClosedIndex() {
        repository.snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            (IndexCommit) null,
            null,
            status(),
            1L,
            5L,
            0L,
            Map.of("_0.si", 42L),
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e))
        );

        assertTrue("the catalog-based implementation must be reached", repository.called.get());
        assertThat("a null commit must stay null", repository.received.get(), nullValue());
    }

    public void testSnapshotRemoteStoreShardReportsUnreadableCommitAsSnapshotFailure() throws Exception {
        final IndexCommit commit = lastCommit();
        deleteCommitFile();

        final AtomicReference<Exception> failure = new AtomicReference<>();
        repository.snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            commit,
            null,
            status(),
            1L,
            commit.getGeneration(),
            0L,
            null,
            ActionListener.wrap(r -> fail("should not succeed, got " + r), failure::set)
        );

        assertFalse("the catalog-based implementation must not be reached", repository.called.get());
        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("Failed to read commit point"));

        Lucene.cleanLuceneIndex(store.directory());
    }
}
