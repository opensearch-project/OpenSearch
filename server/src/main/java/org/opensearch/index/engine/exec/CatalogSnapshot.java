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
public abstract class CatalogSnapshot extends AbstractRefCounted {

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
     * Sets the catalog snapshot map for tracking multiple snapshots.
     *
     * @param catalogSnapshotMap map of generation to catalog snapshots
     */
    public abstract void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> catalogSnapshotMap);

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
     * @param b additional boolean parameter for implementation-specific behavior
     */
    public abstract void setUserData(Map<String, String> userData, boolean b);

    public abstract Object getReader(DataFormat dataFormat);
}
