/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.Collection;
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
     */
    public abstract void setUserData(Map<String, String> userData);

    /**
     * Creates a deep copy of this catalog snapshot. The cloned snapshot starts with a fresh reference count of 1.
     * Subclasses must ensure all mutable state is properly copied.
     *
     * @return a new {@link CatalogSnapshot} with the same logical state
     */
    public abstract CatalogSnapshot clone();
}
