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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.SafeBootstrapCommitter;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.engine.exec.coord.LuceneVersionConverter;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
 * via {@link #commit(Map)}, which sets the live commit data on the writer and calls
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
    private final MergeIndexWriter indexWriter;
    private final LuceneCommitDeletionPolicy deletionPolicy;
    private final AtomicBoolean isClosed = new AtomicBoolean();
    // Keyed by catalog snapshot generation — survives snapshot cloning at the upload boundary.
    private final Map<Long, DirectoryReader> readers = new ConcurrentHashMap<>();

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
        this.store.incRef();
        try {
            this.deletionPolicy = new LuceneCommitDeletionPolicy();
            IndexWriterConfig iwc = createIndexWriterConfig(committerConfig);
            this.indexWriter = new MergeIndexWriter(store.directory(), iwc);
        } catch (Exception e) {
            store.decRef();
            throw e;
        }
    }

    // --- Committer interface ---

    /**
     * Atomically persists the given commit data (catalog snapshot, translog UUID,
     * sequence numbers) and commits the IndexWriter.
     *
     * @param commitData the key-value pairs to store as live commit data
     * @throws IOException if the commit fails
     * @throws IllegalStateException if this committer is closed
     */
    @Override
    public synchronized CommitResult commit(Map<String, String> commitData) throws IOException {
        ensureOpen();
        indexWriter.setLiveCommitData(commitData.entrySet());
        indexWriter.commit();
        SegmentInfos committed = SegmentInfos.readLatestCommit(indexWriter.getDirectory());
        // Encode writer's Lucene version as a long — keeps CatalogSnapshot Lucene-type-agnostic.
        long version = LuceneVersionConverter.encode(committed.getCommitLuceneVersion());
        return new CommitResult(committed.getSegmentsFileName(), committed.getGeneration(), version);
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
            indexWriter.close();
            this.store.decRef();
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
     * Returns commit statistics derived from the latest committed segment infos.
     *
     * @return the commit stats, or {@code null} if segment infos cannot be read
     */
    @Override
    public CommitStats getCommitStats() {
        ensureOpen();
        try {
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(indexWriter.getDirectory());
            return new CommitStats(segmentInfos);
        } catch (IOException e) {
            logger.warn("Failed to read segment infos for commit stats", e);
            return null;
        }
    }

    /**
     * Not yet implemented. Will return safe commit info once the index deleter is wired in.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always
     */
    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        throw new UnsupportedOperationException("TODO:: with index deleter");
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
        DirectoryReader reader = readers.get(catalogSnapshot.getVersion());
        SegmentInfos sis;
        if (reader == null) {
            sis = new SegmentInfos(Version.LATEST.major);
        } else {
            if (reader instanceof StandardDirectoryReader == false) {
                throw new IllegalStateException(
                    "Reader for catalog snapshot version=" + catalogSnapshot.getVersion() + " is not a StandardDirectoryReader: " + reader
                );
            }
            sis = ((StandardDirectoryReader) reader).getSegmentInfos().clone();
        }
        Map<String, String> userData = new HashMap<>(catalogSnapshot.getUserData());
        userData.put(CatalogSnapshot.CATALOG_SNAPSHOT_KEY, catalogSnapshot.serializeToString());
        sis.setUserData(userData, false);
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

    /** Returns the version-keyed reader map used by {@link #serializeToCommitFormat}. */
    Map<Long, DirectoryReader> readers() {
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
        } else if (engineConfig.getIndexSort() != null) {
            iwc.setIndexSort(engineConfig.getIndexSort());
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
        // both unsafe commits and orphan non-CatalogSnapshot commits as well, if any
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setCommitOnClose(false)
            .setIndexCommit(targetCommit);
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
            if (serialized != null && serialized.isEmpty() == false) {
                DataformatAwareCatalogSnapshot dfa = DataformatAwareCatalogSnapshot.deserializeFromString(serialized, resolver);
                // Read the commit's SegmentInfos to get the writer's Lucene version.
                SegmentInfos committed = SegmentInfos.readCommit(store.directory(), ic.getSegmentsFileName());
                long version = LuceneVersionConverter.encode(committed.getCommitLuceneVersion());
                dfa.setLastCommitInfo(ic.getSegmentsFileName(), ic.getGeneration(), version);
                result.put(ic, dfa);
            } else {
                // serialized can be null for the initial empty commit from store.createEmpty() during
                // empty store recovery, since that commit has no CatalogSnapshot data.
                result.put(ic, null);
            }
        }
        return result;
    }
}
