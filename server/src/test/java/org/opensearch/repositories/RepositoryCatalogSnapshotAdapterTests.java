/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.SegmentInfosCatalogSnapshot;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests the {@link CatalogSnapshot}-based default methods on {@link Repository}, and their forwarding through
 * {@link FilterRepository}.
 * <p>
 * These defaults are the compatibility contract for repository plugins that have not been converted to the
 * catalog-based API: a plugin that only implements the {@link IndexCommit} overloads must keep working for
 * Lucene-backed shards, and must fail with an actionable message rather than silently misbehaving when handed a
 * catalog snapshot that has no {@link IndexCommit} representation.
 */
public class RepositoryCatalogSnapshotAdapterTests extends OpenSearchTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
    );

    private Store store;
    private SegmentInfos segmentInfos;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final ShardId shardId = new ShardId("index", "_na_", 1);
        store = new Store(shardId, INDEX_SETTINGS, newDirectory(), new DummyShardLock(shardId));
        try (IndexWriter writer = new IndexWriter(store.directory(), new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
        }
        segmentInfos = store.readLastCommittedSegmentsInfo();
    }

    @Override
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
        super.tearDown();
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

    /** A Repository that implements only the IndexCommit overloads, i.e. an unconverted plugin. */
    private Repository indexCommitOnlyRepository() {
        final Repository repository = mock(Repository.class);
        doCallRealMethod().when(repository)
            .snapshotShard(
                any(Store.class),
                any(MapperService.class),
                any(SnapshotId.class),
                any(IndexId.class),
                nullable(CatalogSnapshot.class),
                nullable(String.class),
                any(IndexShardSnapshotStatus.class),
                any(Version.class),
                any(),
                any(),
                nullable(IndexMetadata.class)
            );
        doCallRealMethod().when(repository)
            .snapshotRemoteStoreIndexShard(
                any(Store.class),
                any(SnapshotId.class),
                any(IndexId.class),
                nullable(CatalogSnapshot.class),
                nullable(String.class),
                any(IndexShardSnapshotStatus.class),
                anyLong(),
                anyLong(),
                anyLong(),
                nullable(Map.class),
                any()
            );
        return repository;
    }

    /** Stands in for a multi-format catalog, which has no IndexCommit representation. */
    private CatalogSnapshot multiFormatCatalog() {
        return mock(CatalogSnapshot.class);
    }

    // ═══════════════════════════════════════════════════════════════
    // snapshotShard(CatalogSnapshot) default adapter
    // ═══════════════════════════════════════════════════════════════

    public void testSnapshotShardDefaultAdaptsLuceneBackedCatalogToIndexCommit() {
        final Repository repository = indexCommitOnlyRepository();

        repository.snapshotShard(
            store,
            mock(MapperService.class),
            snapshotId(),
            indexId(),
            new SegmentInfosCatalogSnapshot(segmentInfos),
            null,
            status(),
            Version.CURRENT,
            Map.of(),
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e)),
            null
        );

        final ArgumentCaptor<IndexCommit> captor = ArgumentCaptor.forClass(IndexCommit.class);
        verify(repository).snapshotShard(
            any(Store.class),
            any(MapperService.class),
            any(SnapshotId.class),
            any(IndexId.class),
            captor.capture(),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            any(Version.class),
            any(),
            any(),
            nullable(IndexMetadata.class)
        );
        assertThat("default must adapt the catalog down to an IndexCommit", captor.getValue(), notNullValue());
        assertEquals(segmentInfos.getGeneration(), captor.getValue().getGeneration());
    }

    public void testSnapshotShardDefaultFailsForMultiFormatCatalog() {
        final Repository repository = indexCommitOnlyRepository();
        final AtomicReference<Exception> failure = new AtomicReference<>();

        repository.snapshotShard(
            store,
            mock(MapperService.class),
            snapshotId(),
            indexId(),
            multiFormatCatalog(),
            null,
            status(),
            Version.CURRENT,
            Map.of(),
            ActionListener.wrap(r -> fail("should not succeed, got " + r), failure::set),
            null
        );

        assertThat(failure.get(), instanceOf(IOException.class));
        assertThat(failure.get().getMessage(), containsString("does not support snapshotting multi-format shards"));
        assertThat(failure.get().getMessage(), containsString("must override the CatalogSnapshot-based"));

        verify(repository, never()).snapshotShard(
            any(Store.class),
            any(MapperService.class),
            any(SnapshotId.class),
            any(IndexId.class),
            any(IndexCommit.class),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            any(Version.class),
            any(),
            any(),
            nullable(IndexMetadata.class)
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // snapshotRemoteStoreIndexShard(CatalogSnapshot) default adapter
    // ═══════════════════════════════════════════════════════════════

    public void testSnapshotRemoteStoreDefaultAdaptsLuceneBackedCatalog() {
        final Repository repository = indexCommitOnlyRepository();

        repository.snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            new SegmentInfosCatalogSnapshot(segmentInfos),
            null,
            status(),
            1L,
            segmentInfos.getGeneration(),
            0L,
            null,
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e))
        );

        final ArgumentCaptor<IndexCommit> captor = ArgumentCaptor.forClass(IndexCommit.class);
        verify(repository).snapshotRemoteStoreIndexShard(
            any(Store.class),
            any(SnapshotId.class),
            any(IndexId.class),
            captor.capture(),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            anyLong(),
            anyLong(),
            anyLong(),
            nullable(Map.class),
            any()
        );
        assertThat(captor.getValue(), notNullValue());
        assertEquals(segmentInfos.getGeneration(), captor.getValue().getGeneration());
    }

    /** The closed-index path passes a null catalog snapshot and carries the file listing separately. */
    public void testSnapshotRemoteStoreDefaultPassesNullCommitThroughForClosedIndex() {
        final Repository repository = indexCommitOnlyRepository();

        repository.snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            (CatalogSnapshot) null,
            null,
            status(),
            1L,
            5L,
            0L,
            Map.of("_0.si", 42L),
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e))
        );

        final ArgumentCaptor<IndexCommit> captor = ArgumentCaptor.forClass(IndexCommit.class);
        verify(repository).snapshotRemoteStoreIndexShard(
            any(Store.class),
            any(SnapshotId.class),
            any(IndexId.class),
            captor.capture(),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            anyLong(),
            anyLong(),
            anyLong(),
            nullable(Map.class),
            any()
        );
        assertThat("null catalog must adapt to a null IndexCommit", captor.getValue(), nullValue());
    }

    public void testSnapshotRemoteStoreDefaultFailsForMultiFormatCatalog() {
        final Repository repository = indexCommitOnlyRepository();
        final AtomicReference<Exception> failure = new AtomicReference<>();

        repository.snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            multiFormatCatalog(),
            null,
            status(),
            1L,
            5L,
            0L,
            null,
            ActionListener.wrap(r -> fail("should not succeed, got " + r), failure::set)
        );

        assertThat(failure.get(), instanceOf(IOException.class));
        assertThat(failure.get().getMessage(), containsString("does not support snapshotting multi-format shards"));

        verify(repository, never()).snapshotRemoteStoreIndexShard(
            any(Store.class),
            any(SnapshotId.class),
            any(IndexId.class),
            any(IndexCommit.class),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            anyLong(),
            anyLong(),
            anyLong(),
            nullable(Map.class),
            any()
        );
    }

    // ═══════════════════════════════════════════════════════════════
    // FilterRepository forwarding
    // ═══════════════════════════════════════════════════════════════

    public void testFilterRepositoryForwardsCatalogSnapshotSnapshotShard() {
        final Repository delegate = mock(Repository.class);
        final CatalogSnapshot catalogSnapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        new FilterRepository(delegate).snapshotShard(
            store,
            mock(MapperService.class),
            snapshotId(),
            indexId(),
            catalogSnapshot,
            "shard-state-id",
            status(),
            Version.CURRENT,
            Map.of(),
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e)),
            null
        );

        final ArgumentCaptor<CatalogSnapshot> captor = ArgumentCaptor.forClass(CatalogSnapshot.class);
        verify(delegate).snapshotShard(
            any(Store.class),
            any(MapperService.class),
            any(SnapshotId.class),
            any(IndexId.class),
            captor.capture(),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            any(Version.class),
            any(),
            any(),
            nullable(IndexMetadata.class)
        );
        assertSame("FilterRepository must forward the catalog snapshot unchanged", catalogSnapshot, captor.getValue());
    }

    public void testFilterRepositoryForwardsCatalogSnapshotRemoteStoreShard() {
        final Repository delegate = mock(Repository.class);
        final CatalogSnapshot catalogSnapshot = new SegmentInfosCatalogSnapshot(segmentInfos);

        new FilterRepository(delegate).snapshotRemoteStoreIndexShard(
            store,
            snapshotId(),
            indexId(),
            catalogSnapshot,
            "shard-state-id",
            status(),
            1L,
            segmentInfos.getGeneration(),
            0L,
            Map.of(),
            ActionListener.wrap(r -> {}, e -> fail("should not fail: " + e))
        );

        final ArgumentCaptor<CatalogSnapshot> captor = ArgumentCaptor.forClass(CatalogSnapshot.class);
        verify(delegate).snapshotRemoteStoreIndexShard(
            any(Store.class),
            any(SnapshotId.class),
            any(IndexId.class),
            captor.capture(),
            nullable(String.class),
            any(IndexShardSnapshotStatus.class),
            anyLong(),
            anyLong(),
            anyLong(),
            nullable(Map.class),
            any()
        );
        assertSame("FilterRepository must forward the catalog snapshot unchanged", catalogSnapshot, captor.getValue());
    }
}
