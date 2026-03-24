/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class representing a snapshot of the catalog state at a specific point in time.
 * Maintains versioned information about segments, files, and metadata for index operations.
 * Extends AbstractRefCounted to support reference counting for safe concurrent access.
 * Subclasses must implement methods for accessing file metadata, segments, and user data.
 */
@ExperimentalApi
public abstract class CatalogSnapshot extends AbstractRefCounted implements Writeable, Cloneable {

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

    public CatalogSnapshot(String name, long generation, long version) {
        super(name);
        this.generation = generation;
        this.version = version;
    }

    /**
     * Constructs a CatalogSnapshot from a {@link StreamInput}.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public CatalogSnapshot(StreamInput in) throws IOException {
        super("catalog_snapshot");
        this.generation = in.readLong();
        this.version = in.readLong();
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
        // Still using the clone call since Lucene call requires clone. This will allow a SegmentsInfos backed CatalogSnapshot to use the
        // same method in calls.
        return this;
    }

    /**
     * Sets user-defined metadata for this catalog snapshot.
     *
     * @param userData map of user data key-value pairs
     */
    public abstract void setUserData(Map<String, String> userData);

    /**
     * Creates a deep copy of this catalog snapshot. The cloned snapshot starts with a fresh reference count
     * of 1 (from {@link AbstractRefCounted}).
     * Subclasses must ensure all mutable state is properly copied.
     *
     * @return a new {@link CatalogSnapshot} with the same logical state
     */
    public abstract CatalogSnapshot clone();

    public abstract Object getReader(DataFormat dataFormat);
}
