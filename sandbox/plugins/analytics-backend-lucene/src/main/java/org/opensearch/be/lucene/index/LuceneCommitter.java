/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.LuceneReader;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.be.lucene.stats.LuceneStatsProvider;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.SafeBootstrapCommitter;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.engine.exec.coord.LuceneVersionConverter;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.plugin.stats.DataFormatStatsProviderRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Lucene-specific {@link Committer} that owns the shared {@link IndexWriter} lifecycle
 * for a single shard.
 * Extends {@link SafeBootstrapCommitter} to enforce safe commit trimming on startup.
 * <p>
 * The shared writer is opened during construction using configuration from
 * {@link CommitterConfig} (analyzer, codec, similarity, RAM buffer, index sort, etc.).
 * All per-generation {@link LuceneWriter} instances produce segments in isolated temp
 * directories; those segments are later incorporated into this shared writer via
 * {@code IndexWriter.addIndexes} during refresh in {@link LuceneIndexingExecutionEngine}.
 * <p>
 * Commit data (catalog snapshot, translog UUID, sequence numbers) is persisted atomically
 * via {@link #commit(org.opensearch.index.engine.exec.commit.Committer.CommitInput)}, which sets the live commit data on the writer and calls
 * {@link IndexWriter#commit()}.
 * <p>
 * The store reference is incremented on construction and decremented on {@link #close()}.
 * Closing the committer also closes the underlying IndexWriter.
 *
 * <h2>Refresh-lock coordination</h2>
 *
 * <p>The engine passes a {@code preMergeCommitHook} via {@link CommitterConfig}. We wire it
 * into Lucene as a {@code MergedSegmentWarmer} on the {@link IndexWriterConfig}. The warmer
 * runs between {@code mergeMiddle} and {@code commitMerge} while the {@link IndexWriter}
 * monitor is <em>not</em> held, so invoking the hook there establishes the ordering
 * {@code refreshLock → IW monitor} on the merge thread — matching the refresh path and
 * avoiding the lock inversion that would occur if coordination happened inside
 * {@code commitMerge}. Ownership of whatever the hook acquires (currently the engine's
 * refresh lock) is transferred to the engine's {@code applyMergeChanges} callback, which
 * releases it after the catalog is updated. This committer never touches the refresh lock
 * directly.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneCommitter extends SafeBootstrapCommitter {

    private static final Logger logger = LogManager.getLogger(LuceneCommitter.class);

    private final Store store;
    private final Sort userProvidedSort;
    private final MergeIndexWriter indexWriter;
    private final LuceneCommitDeletionPolicy deletionPolicy;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    /** Cached latest committed {@link SegmentInfos}; refreshed inside {@link #commit}, read by {@link #getCommitStats}. */
    private volatile SegmentInfos lastCommittedSegmentInfos;
    // Keyed by catalog snapshot generation — survives snapshot cloning at the upload boundary.
    private final Map<Long, LuceneReader> readers = new ConcurrentHashMap<>();

    /**
     * Creates a new LuceneCommitter. Trims unsafe commits (via {@link SafeBootstrapCommitter}),
     * then opens the IndexWriter.
     *
     * @param committerConfig the committer committerConfig (shard path, index committerConfig, engine config, store)
     * @throws IOException if opening the IndexWriter fails
     */
    public LuceneCommitter(CommitterConfig committerConfig) throws IOException {
        super(committerConfig);
        this.store = Objects.requireNonNull(committerConfig.engineConfig().getStore());
        this.userProvidedSort = committerConfig.engineConfig().getIndexSort();
        this.store.incRef();
        try {
            this.deletionPolicy = new LuceneCommitDeletionPolicy();
            IndexWriterConfig iwc = createIndexWriterConfig(committerConfig);
            this.indexWriter = new MergeIndexWriter(store.directory(), iwc);
            this.lastCommittedSegmentInfos = SegmentInfos.readLatestCommit(indexWriter.getDirectory());
        } catch (Exception e) {
            store.decRef();
            throw e;
        }
    }

    // --- Committer interface ---

    /**
     * Atomically persists the given commit data (catalog snapshot, translog UUID,
     * sequence numbers) and commits the IndexWriter. When a catalog snapshot is present,
     * all referenced data files are fsync'd before the commit point to ensure crash
     * consistency (write-ahead ordering).
     *
     * @param commitData the key-value pairs to store as live commit data
     * @throws IOException if the commit fails
     * @throws IllegalStateException if this committer is closed
     */
    @Override
    public synchronized CommitResult commit(CommitInput commitData) throws IOException {
        ensureOpen();
        long start = System.nanoTime();
        try {
            indexWriter.setLiveCommitData(commitData.userData());
            // Write-ahead fsync: data files durable before the commit point that references them.
            // getFiles(false) excludes segments_N — IndexWriter.commit() handles that via rename + syncMetaData.
            if (commitData.catalogSnapshot() != null) {
                store.directory().sync(commitData.catalogSnapshot().getFiles(false));
                store.directory().syncMetaData();
            }
            indexWriter.commit();
            SegmentInfos committed = SegmentInfos.readLatestCommit(indexWriter.getDirectory());
            this.lastCommittedSegmentInfos = committed;

            // Encode writer's Lucene version as a long — keeps CatalogSnapshot Lucene-type-agnostic.
            long version = LuceneVersionConverter.encode(committed.getCommitLuceneVersion());
            return new CommitResult(committed.getSegmentsFileName(), committed.getGeneration(), version);
        } finally {
            LuceneStatsProvider provider = (LuceneStatsProvider) DataFormatStatsProviderRegistry.INSTANCE.get(
                LuceneStatsProvider.FORMAT_NAME
            );
            if (provider != null) {
                LuceneShardStatsTracker tracker = provider.getTracker(store.shardId());
                if (tracker != null) {
                    tracker.incCommitTotal();
                    tracker.addCommitTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            }
        }
    }

    /**
     * Closes the IndexWriter and releases the store reference.
     * Subsequent calls are no-ops.
     *
     * @throws IOException if closing the IndexWriter fails
     */
    @Override
    public List<CatalogSnapshot> listCommittedSnapshots() throws IOException {
        ensureOpen();
        return loadCommittedSnapshots(store).values().stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            try {
                indexWriter.close();
            } finally {
                this.store.decRef();
            }
        }
    }

    /**
     * Returns the last committed data as an unmodifiable map.
     * If no commit data has been set, returns an empty map.
     *
     * @return the last committed key-value pairs
     * @throws IOException if reading commit data fails
     * @throws IllegalStateException if this committer is closed
     */
    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        ensureOpen();
        Iterable<Map.Entry<String, String>> liveCommitData = indexWriter.getLiveCommitData();
        if (liveCommitData == null) {
            return Map.of();
        }
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : liveCommitData) {
            result.put(entry.getKey(), entry.getValue());
        }
        return Map.copyOf(result);
    }

    /**
     * Returns commit stats from the cached {@link SegmentInfos} to avoid a per-call disk read
     * (which validates referenced files and races with concurrent merges).
     */
    @Override
    public CommitStats getCommitStats() {
        ensureOpen();
        return new CommitStats(lastCommittedSegmentInfos);
    }

    @Override
    public void deleteCommit(CatalogSnapshot snapshot) throws IOException {
        ensureOpen();
        deletionPolicy.purgeCommit(snapshot.getId());
        indexWriter.deleteUnusedFiles();
    }

    @Override
    public boolean isCommitManagedFile(String fileName) {
        return fileName.startsWith(IndexFileNames.SEGMENTS) || fileName.equals(IndexWriter.WRITE_LOCK_NAME);
    }

    @Override
    public void markStoreCorrupted(IOException cause) {
        if (store.tryIncRef() == false) {
            return;
        }
        try {
            store.markStoreCorrupted(cause);
        } catch (IOException e) {
            logger.warn("Couldn't mark store corrupted", e);
        } finally {
            store.decRef();
        }
    }

    /**
     * Builds upload-time Lucene {@code SegmentInfos} bytes from the {@link DirectoryReader}
     * registered for this {@code catalogSnapshot} (opened at that snapshot's refresh point).
     * The resulting {@code SegmentInfos} is strictly consistent with the catalog's Lucene
     * file set.
     *
     * <p>A missing reader indicates a plumbing bug — whenever Lucene is an active indexing
     * format, {@code LuceneReaderManager.afterRefresh} MUST have registered the reader for
     * this snapshot before upload is invoked. We fail fast rather than open a divergent
     * reader off the current {@code IndexWriter} state.
     */
    @Override
    public byte[] serializeToCommitFormat(CatalogSnapshot catalogSnapshot) throws IOException {
        ensureOpen();
        LuceneReader luceneReader = readers.get(catalogSnapshot.getId());
        DirectoryReader reader = luceneReader == null ? null : luceneReader.directoryReader();
        SegmentInfos sis;
        if (reader == null) {
            assert catalogSnapshot.getDataFormats().contains(LuceneDataFormat.LUCENE_FORMAT_NAME) == false
                : "Lucene is listed in catalog data formats but no reader was registered for version=" + catalogSnapshot.getId();
            logger.info("No Lucene reader for catalog snapshot version={} — producing empty SegmentInfos", catalogSnapshot.getId());
            sis = new SegmentInfos(Version.LATEST.major);
        } else {
            DirectoryReader unwrapped = reader;
            while (unwrapped instanceof FilterDirectoryReader fdr) {
                unwrapped = fdr.getDelegate();
            }
            if (unwrapped instanceof StandardDirectoryReader == false) {
                throw new IllegalStateException(
                    "Reader for catalog snapshot version=" + catalogSnapshot.getId() + " is not a StandardDirectoryReader: " + reader
                );
            }
            sis = ((StandardDirectoryReader) unwrapped).getSegmentInfos().clone();
        }
        Map<String, String> sisUserData = new HashMap<>(catalogSnapshot.getUserData());
        sisUserData.put(CatalogSnapshot.CATALOG_SNAPSHOT_ID, Long.toString(catalogSnapshot.getId()));
        sisUserData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, catalogSnapshot.serializeToString());
        sis.setUserData(sisUserData, false);
        sis.setNextWriteGeneration(catalogSnapshot.getLastCommitGeneration());
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        sis.write(new ByteBuffersIndexOutput(out, "Snapshot of SegmentInfos", "SegmentInfos"));
        return out.toArrayCopy();
    }

    /**
     * Returns the underlying IndexWriter.
     * Visible to other classes in this package (e.g., LuceneIndexingExecutionEngine).
     *
     * @return the index writer, or null if closed
     */
    MergeIndexWriter getIndexWriter() {
        ensureOpen();
        return indexWriter;
    }

    Sort getUserProvidedSort() {
        ensureOpen();
        return userProvidedSort;
    }

    /** Returns the store reference. Package-private for sibling classes (e.g., LuceneDeleteExecutionEngine). */
    Store getStore() {
        return store;
    }

    /** Returns the version-keyed reader map used by {@link #serializeToCommitFormat}. */
    Map<Long, LuceneReader> readers() {
        ensureOpen();
        return readers;
    }

    // --- Internal ---

    private IndexWriterConfig createIndexWriterConfig(CommitterConfig committerConfig) {
        EngineConfig engineConfig = committerConfig.engineConfig();
        if (engineConfig == null) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setIndexDeletionPolicy(deletionPolicy);
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setMergeScheduler(new SerialMergeScheduler());
            return iwc;
        }
        // TODO:: Merge Config needs to be wired in
        IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCodec(engineConfig.getCodec());
        if (engineConfig.getSimilarity() != null) {
            iwc.setSimilarity(engineConfig.getSimilarity());
        }
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        iwc.setUseCompoundFile(engineConfig.useCompoundFile());
        // Refresh-lock hand-off: the MergedSegmentWarmer fires on the merge thread between
        // mergeMiddle and commitMerge, while the IndexWriter monitor is NOT held. Invoking
        // the engine-provided preMergeCommitHook here gives the merge path the ordering
        // refreshLock → IW monitor, which matches the refresh path (DataFormatAwareEngine#refresh
        // takes refreshLock before calling IndexWriter#addIndexes). Ownership of whatever the
        // hook acquires is transferred to applyMergeChanges, which releases it after the
        // catalog is updated. See the class Javadoc.
        iwc.setMergedSegmentWarmer(_ -> committerConfig.preMergeCommitHook().run());

        // Determine if Lucene is a secondary format in a composite setup.
        // When secondary, use a SortedNumericSortField on the row ID so MultiSorter can reorder
        // documents by remapped row ID during merge. When primary (or standalone), use the
        // engine config's IndexSort (which may be user-configured).
        // TODO Check what is the right way to get this information as the below one is leaky
        // https://github.com/opensearch-project/OpenSearch/issues/21506
        List<String> secondaryFormats = engineConfig.getIndexSettings().getSettings().getAsList("index.composite.secondary_data_formats");
        boolean isSecondary = secondaryFormats.contains("lucene");

        if (isSecondary) {
            iwc.setIndexSort(new Sort(new SortedNumericSortField(DocumentInput.ROW_ID_FIELD, SortField.Type.LONG)));
        } else if (userProvidedSort != null) {
            iwc.setIndexSort(userProvidedSort);
        }
        iwc.setCommitOnClose(false);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        iwc.setIndexDeletionPolicy(deletionPolicy);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setMergeScheduler(new SerialMergeScheduler());
        return iwc;
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LuceneCommitter is closed");
        }
    }

    // --- SafeBootstrapCommitter abstract method ---

    @Override
    protected void discoverAndTrimUnsafeCommits(Store store, Path translogPath) throws IOException {
        Map<IndexCommit, CatalogSnapshot> committed = loadCommittedSnapshots(store);
        if (committed.isEmpty()) {
            throw new IllegalStateException("No Lucene commits found — index may be corrupt");
        }
        List<CatalogSnapshot> snapshots = committed.values().stream().filter(Objects::nonNull).toList();
        // No CatalogSnapshot commits found among Lucene commits — skipping safe commit trimming
        if (snapshots.isEmpty()) {
            return;
        }
        String translogUUID = snapshots.getLast().getUserData().get(Translog.TRANSLOG_UUID_KEY);
        long globalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
        CatalogSnapshot safeCommit = CombinedCatalogSnapshotDeletionPolicy.findSafeCommitPoint(snapshots, globalCheckpoint);
        IndexCommit targetCommit = null;
        for (Map.Entry<IndexCommit, CatalogSnapshot> entry : committed.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getGeneration() == safeCommit.getGeneration()) {
                targetCommit = entry.getKey();
                break;
            }
        }
        if (targetCommit == null) {
            throw new IllegalStateException("Safe commit [gen=" + safeCommit.getGeneration() + "] not found among Lucene IndexCommits");
        }
        // Open a temp IndexWriter at the target commit and re-commit. The default deletion policy
        // (KeepOnlyLastCommitDeletionPolicy) discards all other segments_N files, cleaning up
        // both unsafe commits and orphan non-CatalogSnapshot commits as well, if any.
        // Pin the merge policy to NoMergePolicy: this writer's only job is to re-anchor the
        // commit point. The default TieredMergePolicy would otherwise merge segments without
        // honoring the engine's index sort, producing an unsorted merged segment that the
        // subsequent (sorted) MergeIndexWriter cannot open.
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setCommitOnClose(false)
            .setIndexCommit(targetCommit)
            .setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter tempWriter = new IndexWriter(store.directory(), iwc)) {
            tempWriter.setLiveCommitData(targetCommit.getUserData().entrySet());
            tempWriter.commit();
        }
    }

    /**
     * Loads committed CatalogSnapshots from Lucene IndexCommits in the given store.
     * Returns a {@link Map} preserving commit order (oldest → newest).
     * Only commits containing a serialized CatalogSnapshot are included.
     */
    static Map<IndexCommit, CatalogSnapshot> loadCommittedSnapshots(Store store) throws IOException {
        Function<String, String> resolver = store.shardFormatDirectoryResolver();
        List<IndexCommit> commits = DirectoryReader.listCommits(store.directory());
        LinkedHashMap<IndexCommit, CatalogSnapshot> result = new LinkedHashMap<>();
        for (IndexCommit ic : commits) {
            String serialized = ic.getUserData().get(CatalogSnapshot.CATALOG_SNAPSHOT_KEY);
            DataformatAwareCatalogSnapshot dfa;
            if (serialized != null && serialized.isEmpty() == false) {
                dfa = DataformatAwareCatalogSnapshot.deserializeFromString(serialized, resolver);
            } else {
                // Initial empty commit from store.createEmpty() has no serialized catalog.
                // Create an empty snapshot and seed its commit generation from the on-disk
                // segments_N so that getLastCommitGeneration() returns a stable value before
                // the first real flush — preventing the catalog generation fallback from
                // leaking into ReplicationCheckpoint.segmentsGen.
                dfa = (DataformatAwareCatalogSnapshot) CatalogSnapshotManager.createInitialSnapshot(
                    0L,
                    0L,
                    0L,
                    List.of(),
                    -1L,
                    ic.getUserData()
                );
            }
            SegmentInfos committed = SegmentInfos.readCommit(store.directory(), ic.getSegmentsFileName());
            long version = LuceneVersionConverter.encode(committed.getCommitLuceneVersion());
            dfa.setLastCommitInfo(ic.getSegmentsFileName(), ic.getGeneration(), version);
            result.put(ic, dfa);
        }
        return result;
    }
}
