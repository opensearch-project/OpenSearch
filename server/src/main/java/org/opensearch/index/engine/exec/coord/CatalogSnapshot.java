/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Sort;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class representing a snapshot of the catalog state at a specific point in time.
 * Maintains versioned information about segments, files, and metadata for index operations.
 * Uses an internal reference counter for safe concurrent access.
 * Subclasses must implement {@link #closeInternal()} for resource cleanup and methods for
 * accessing file metadata, segments, and user data.
 *
 * <p><b>Important:</b> Do not call {@code incRef()}, {@code decRef()}, or {@code tryIncRef()} directly.
 * Use {@link org.opensearch.index.engine.exec.coord.CatalogSnapshotManager#acquireSnapshot()} to obtain
 * a reference-counted handle, and close the returned {@link org.opensearch.common.concurrent.GatedCloseable}
 * when done. The manager handles all reference counting internally.</p>
 */
@ExperimentalApi
public abstract class CatalogSnapshot implements Writeable, Cloneable {

    private static final Logger logger = LogManager.getLogger(CatalogSnapshot.class);

    /**
     * Key for storing catalog snapshot in user data.
     */
    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    /**
     * Key for storing last composite writer generation in user data.
     */
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    /**
     * Key for storing catalog snapshot ID in user data.
     */
    public static final String CATALOG_SNAPSHOT_ID = "_id";

    protected final long generation;
    protected long version;

    private final AbstractRefCounted refCounter;

    /**
     * Lazily computed cache of file names grouped by format. Computed once on first access
     * since segments are immutable after construction.
     */
    private volatile Map<String, Collection<String>> filesByFormatCache;

    /**
     * Whether this snapshot has been committed (persisted via flush).
     * Package-private — managed by {@link IndexFileDeleter}.
     */
    private volatile boolean committed;

    protected CatalogSnapshot(String name, long generation, long version) {
        this.generation = generation;
        this.version = version;
        this.refCounter = new AbstractRefCounted(name) {
            @Override
            protected void closeInternal() {
                CatalogSnapshot.this.closeInternal();
            }
        };
    }

    /**
     * Constructs a CatalogSnapshot from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    protected CatalogSnapshot(StreamInput in) throws IOException {
        this.generation = in.readLong();
        this.version = in.readLong();
        this.refCounter = new AbstractRefCounted("catalog_snapshot") {
            @Override
            protected void closeInternal() {
                CatalogSnapshot.this.closeInternal();
            }
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(generation);
        out.writeLong(version);
    }

    public long getGeneration() {
        return generation;
    }

    public long getVersion() {
        return version;
    }

    /**
     * Marks this snapshot as committed (persisted via flush).
     * Package-private — only called by {@link IndexFileDeleter} and {@link CatalogSnapshotManager}.
     */
    void markCommitted() {
        this.committed = true;
    }

    /**
     * Returns whether this snapshot was committed.
     * Package-private — only called by {@link IndexFileDeleter} and {@link CatalogSnapshotManager}.
     */
    boolean isCommitted() {
        return committed;
    }

    // Package-private ref counting — only accessible within exec.coord (i.e., CatalogSnapshotManager)

    /**
     * Decrements the reference count. Returns {@code true} if the count reached zero
     * and {@link #closeInternal()} was invoked.
     */
    boolean decRef() {
        return refCounter.decRef();
    }

    /**
     * Tries to increment the reference count. Returns {@code false} if the snapshot is already closed.
     */
    boolean tryIncRef() {
        return refCounter.tryIncRef();
    }

    /**
     * Returns the current reference count.
     */
    int refCount() {
        return refCounter.refCount();
    }

    /**
     * Called when the reference count reaches zero. Subclasses should release any resources here.
     */
    protected abstract void closeInternal();

    /**
     * Gets user-defined metadata associated with this catalog snapshot.
     *
     * @return map of user data key-value pairs
     */
    public abstract Map<String, String> getUserData();

    /**
     * Gets the unique identifier for this catalog snapshot.
     *
     * @return the catalog snapshot ID
     */
    public abstract long getId();

    /**
     * Gets all segments in this catalog snapshot.
     *
     * @return list of Segment objects
     */
    public abstract List<Segment> getSegments();

    /**
     * Retrieves searchable files for a specific data format.
     *
     * @param dataFormat the data format identifier
     * @return collection of WriterFileSet objects for the specified format
     */
    public abstract Collection<WriterFileSet> getSearchableFiles(String dataFormat);

    /**
     * Gets all data formats present in this catalog snapshot.
     *
     * @return set of data format identifiers
     */
    public abstract Set<String> getDataFormats();

    /**
     * Gets the last writer generation number.
     *
     * @return the last writer generation
     */
    public abstract long getLastWriterGeneration();

    /**
     * Serializes this catalog snapshot to a string representation.
     *
     * @return serialized string
     * @throws IOException if an I/O error occurs
     */
    public abstract String serializeToString() throws IOException;

    /**
     * Returns all file names grouped by data format name. The result is computed once
     * and cached for the lifetime of this snapshot (segments are immutable after construction).
     * <p>
     * Used by {@code IndexFileDeleter} for ref-count bookkeeping, avoiding repeated
     * iteration over segments on every add/remove call.
     *
     * @return unmodifiable map of format name to unmodifiable set of file names
     */
    public Map<String, Collection<String>> getFilesByFormat() {
        Map<String, Collection<String>> cached = this.filesByFormatCache;
        if (cached != null) {
            return cached;
        }
        Map<String, Set<String>> result = new HashMap<>();
        for (Segment segment : getSegments()) {
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                result.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue().files());
            }
        }
        Map<String, Collection<String>> unmodifiable = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : result.entrySet()) {
            unmodifiable.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
        }
        cached = Collections.unmodifiableMap(unmodifiable);
        this.filesByFormatCache = cached;
        return cached;
    }

    /**
     * Finds the {@link WriterFileSet} for the given data format whose {@code writerGeneration}
     * matches, or {@code null} if no segment carries a non-empty file set for that format/generation.
     *
     * @param format           the data format name (e.g. {@code "parquet"})
     * @param writerGeneration the writer generation to match
     * @return the matching file set, or {@code null}
     */
    public WriterFileSet findFileSet(String format, long writerGeneration) {
        for (Segment seg : getSegments()) {
            WriterFileSet candidate = seg.dfGroupedSearchableFiles().get(format);
            if (candidate == null || candidate.files().isEmpty()) continue;
            if (candidate.writerGeneration() != writerGeneration) continue;
            return candidate;
        }
        return null;
    }

    /**
     * Sets user-defined metadata for this catalog snapshot.
     *
     * @param userData map of user data key-value pairs
     * @param commitData whether this is commit-level user data
     */
    public abstract void setUserData(Map<String, String> userData, boolean commitData);

    /**
     * Creates a deep copy of this catalog snapshot. The cloned snapshot starts with a fresh reference count of 1.
     * Subclasses must ensure all mutable state is properly copied.
     *
     * @return a new {@link CatalogSnapshot} with the same logical state
     */
    public abstract CatalogSnapshot clone();

    /**
     * Returns the format-version string for the given file. Empty string means pre-versioning
     * (legacy / BWC). The string is plugin-defined and MUST NOT be compared across formats.
     *
     * @param file the file name
     * @return the format version string
     */
    /**
     * Returns the format-version for the given file as a long-encoded value.
     * Encoding is defined by {@link LuceneVersionConverter}:
     * {@code major * 1_000_000 + minor * 1_000 + bugfix}. Returns {@code 0} to indicate
     * "unknown / pre-versioning" (callers map this to {@link org.apache.lucene.util.Version#LATEST}).
     *
     * @param file the file name to inspect
     * @return the encoded long version, or {@code 0} if unknown
     */
    public abstract long getFormatVersionForFile(String file);

    /**
     * Returns the minimum segment format version as a long-encoded value (see
     * {@link LuceneVersionConverter}). Returns {@code 0} for multi-format catalogs
     * (cross-format comparison is not meaningful) or for empty catalogs.
     */
    public abstract long getMinSegmentFormatVersion();

    /**
     * Returns the commit-time format version as a long-encoded value
     * (see {@link LuceneVersionConverter}). Returns {@code 0} for multi-format
     * catalogs or when no commit has landed yet. Callers decode via
     * {@link LuceneVersionConverter#toLuceneOrLatest(long)} when a Lucene view is needed.
     */
    public abstract long getCommitDataFormatVersion();

    /** Total number of live documents in this snapshot. SI → Lucene live docs; DFA → 0 (TODO). */
    public abstract long getNumDocs();

    /**
     * Name of the top-level commit file, or {@code null} if not yet committed.
     * Format-neutral: Lucene-backed snapshots use {@code segments_N} but callers must not
     * assume any naming convention. {@code MetadataSnapshot.loadMetadata} skips the
     * hash-full-file step when this is {@code null}.
     */
    public abstract String getLastCommitFileName();

    /**
     * Returns the Lucene {@code segments_N} generation to use when serializing this snapshot
     * into a synthetic {@code SegmentInfos} or building a {@code ReplicationCheckpoint}.
     * <p>
     * For {@code SegmentInfosCatalogSnapshot}, this is the Lucene generation from {@code SegmentInfos}.
     * For {@code DataformatAwareCatalogSnapshot}, this is the generation set via
     * {@link DataformatAwareCatalogSnapshot#setLastCommitInfo}, falling back to {@link #getGeneration()}.
     */
    public long getLastCommitGeneration() {
        return getGeneration();
    }

    /**
     * Builds a list of {@link org.opensearch.index.engine.Segment} from this catalog snapshot's segments.
     * <p>
     * All segments are marked as searchable. Committed state is determined by comparing this snapshot's ID
     * against the last committed snapshot ID stored in commit data.
     *
     * @param lastCommitData commit data map from the committer
     * @param indexSort      the index sort, or null if unsorted
     * @return list of engine segments sorted by generation
     */
    public List<org.opensearch.index.engine.Segment> buildEngineSegments(Map<String, String> lastCommitData, Sort indexSort) {
        boolean isCommitted = resolveIsCommitted(lastCommitData);
        List<org.opensearch.index.engine.Segment> result = new java.util.ArrayList<>(getSegments().size());
        for (Segment dfSeg : getSegments()) {
            org.opensearch.index.engine.Segment seg = new org.opensearch.index.engine.Segment("_" + Long.toHexString(dfSeg.generation()));
            seg.search = true;
            seg.committed = isCommitted;
            seg.version = org.apache.lucene.util.Version.LATEST;
            if (dfSeg.dfGroupedSearchableFiles().isEmpty()) {
                logger.warn("Segment [{}] has no searchable files; reporting 0 doc count", dfSeg.generation());
            }
            long numRows = dfSeg.dfGroupedSearchableFiles().values().stream().findFirst().map(WriterFileSet::numRows).orElse(0L);
            if (numRows > Integer.MAX_VALUE) {
                logger.warn("Segment [{}] has {} rows exceeding Integer.MAX_VALUE; clamping docCount", dfSeg.generation(), numRows);
            }
            seg.docCount = (int) Math.min(numRows, Integer.MAX_VALUE);
            seg.delDocCount = 0;
            seg.sizeInBytes = dfSeg.dfGroupedSearchableFiles().values().stream().mapToLong(WriterFileSet::getTotalSize).sum();
            seg.segmentSort = indexSort;
            result.add(seg);
        }
        result.sort(java.util.Comparator.comparingLong(org.opensearch.index.engine.Segment::getGeneration));
        return result;
    }

    /**
     * Collects file sizes for all files in this catalog snapshot.
     * Returns individual file names as keys, matching the existing stats API format.
     *
     * Note: The existing stats API groups these files by extension for display purposes
     * (e.g., compound files show underlying format extensions in descriptions),
     * but the collection phase returns individual file names as keys.
     *
     * @param store  the store to resolve file sizes from
     * @return map of individual file names to their sizes
     */
    public Map<String, Long> collectFileSizes(Store store) {
        Map<String, Long> allFileSizes = new java.util.HashMap<>();
        for (Segment segment : getSegments()) {
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                String formatName = entry.getKey();
                for (String file : entry.getValue().files()) {
                    String dirFile = org.opensearch.index.store.FileMetadata.serialize(formatName, file);
                    try {
                        allFileSizes.put(file, store.directory().fileLength(dirFile));
                    } catch (java.io.IOException e) {
                        logger.warn(() -> "Failed to read size for file [" + dirFile + "]; reporting 0", e);
                        allFileSizes.put(file, 0L);
                    }
                }
            }
        }
        return allFileSizes;
    }

    /**
     * Collects file sizes grouped by file extension in a single pass, avoiding an intermediate
     * per-file map. Sizes for files sharing the same extension are summed.
     * <p>
     * Note: Compound files (e.g., Lucene .cfs) are reported under their physical extension
     * rather than broken down by internal sub-file format. This is a known limitation.
     *
     * @param store the store to resolve file sizes from
     * @return map of file extension to total size in bytes
     */
    public Map<String, Long> collectFileSizesGroupedByExtension(Store store) {
        Map<String, Long> grouped = new HashMap<>();
        for (Segment segment : getSegments()) {
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                String formatName = entry.getKey();
                for (String file : entry.getValue().files()) {
                    String dirFile = org.opensearch.index.store.FileMetadata.serialize(formatName, file);
                    long size;
                    try {
                        size = store.directory().fileLength(dirFile);
                    } catch (java.io.IOException e) {
                        logger.warn(() -> "Failed to read size for file [" + dirFile + "]; reporting 0", e);
                        size = 0L;
                    }
                    String extension = extractExtension(file);
                    grouped.merge(extension, size, Long::sum);
                }
            }
        }
        return grouped;
    }

    private static String extractExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        if (lastDot == -1 || lastDot == fileName.length() - 1) {
            return "unknown";
        }
        return fileName.substring(lastDot + 1);
    }

    /**
     * Resolves whether this snapshot has been committed by comparing its ID
     * against the last committed snapshot ID stored in commit data.
     *
     * @param lastCommitData commit data map from the committer
     * @return true if the snapshot ID matches the last committed snapshot ID
     */
    public boolean resolveIsCommitted(Map<String, String> lastCommitData) {
        String lastCommittedIdStr = lastCommitData.get(CATALOG_SNAPSHOT_ID);
        if (lastCommittedIdStr == null) {
            return false;
        }
        try {
            return getId() == Long.parseLong(lastCommittedIdStr);
        } catch (NumberFormatException e) {
            logger.warn("Invalid catalog snapshot ID in committed data: {}", lastCommittedIdStr);
            return false;
        }
    }

    /**
     * Returns the canonical file names for upload to remote store.
     * Each subclass formats names appropriately for its data format:
     * <ul>
     *   <li>{@link SegmentInfosCatalogSnapshot}: plain Lucene file names (e.g., {@code "_0.cfe"})</li>
     *   <li>{@link DataformatAwareCatalogSnapshot}: serialized format-aware names
     *       (e.g., {@code "parquet/data.parquet"}) for non-lucene files</li>
     * </ul>
     *
     * @param includeSegmentsFile whether to include the segments file in the returned collection
     * @return collection of file name strings ready for upload
     * @throws IOException in case of I/O error
     */
    public abstract Collection<String> getFiles(boolean includeSegmentsFile) throws IOException;

}
