/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

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
     * Creates a clone without acquiring a reference count.
     * Used for Lucene compatibility where clone is required.
     *
     * @return this catalog snapshot instance
     */
    public CatalogSnapshot cloneNoAcquire() {
        return this;
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
     * Returns the major version of the format that wrote the given file.
     * For Lucene files, this is the Lucene major version from SegmentInfo.
     * For non-Lucene files (e.g., parquet), this is the format-specific version.
     *
     * @param file the file name
     * @return the format major version
     */
    public abstract Version getFormatVersionForFile(String file);

    /**
     * Initial seed for {@code maxVersion} in {@link org.opensearch.index.store.Store.MetadataSnapshot
     * #loadMetadata(CatalogSnapshot, org.apache.lucene.store.Directory, org.apache.logging.log4j.Logger,
     * boolean)}. SI-backed snapshots return {@code segmentInfos.getMinSegmentLuceneVersion()};
     * DFA snapshots return {@code null}. May be {@code null} even on SI (in-memory infos).
     */
    public abstract Version getMinSegmentFormatVersion();

    /**
     * Returns the version snapshot was committed under.
     * Used to populate remote-store checkpoint metadata so downstream consumers can interpret
     * per-file encodings consistently.
     *
     * <p>Implementations:
     * <ul>
     *   <li>Lucene-backed snapshots return the version read from {@code SegmentInfos}.</li>
     *   <li>DFA snapshots return the version tracked at commit time for their underlying format.</li>
     * </ul>
     *
     * @return the commit-time Lucene version
     */
    public abstract Version getCommitDataFormatVersion();

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
     * Returns the Lucene {@link SegmentInfos} bytes written into the remote metadata file. Bytes
     * must describe this snapshot's commit — implementations must not re-read {@code segments_N}
     * from disk at call time (would race with concurrent flush).
     *
     * @throws IOException on serialization error
     * @throws IllegalStateException if bytes were expected to be pre-captured but weren't
     */
    public abstract byte[] serialize() throws IOException;

    /**
     * Returns the underlying SegmentInfos for Lucene-backed snapshots.
     * Throws UnsupportedOperationException for non-Lucene snapshots.
     */
    public abstract SegmentInfos getSegmentInfos();

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
