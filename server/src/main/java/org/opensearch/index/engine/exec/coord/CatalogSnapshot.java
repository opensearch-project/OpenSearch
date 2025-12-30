/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CatalogSnapshot extends AbstractRefCounted implements Writeable, Cloneable {

    // Static constants
    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    public static final String CATALOG_SNAPSHOT_ID = "_id";

    protected final long generation;
    protected final long version;

    public CatalogSnapshot(String name, long generation, long version) {
        super(name);
        this.generation = generation;
        this.version = version;
    }

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

    // Abstract methods that subclasses must implement
    public abstract Collection<FileMetadata> getFileMetadataList() throws IOException;
    public abstract Map<String, String> getUserData();
    public abstract long getId();
    public abstract List<Segment> getSegments();
    public abstract Collection<WriterFileSet> getSearchableFiles(String dataFormat);
    public abstract Set<String> getDataFormats();
    public abstract long getLastWriterGeneration();
    public abstract String serializeToString() throws IOException;
    public abstract void remapPaths(Path newShardDataPath);
    public abstract void setIndexFileDeleterSupplier(java.util.function.Supplier<IndexFileDeleter> supplier);
    public abstract void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> catalogSnapshotMap);

    public CatalogSnapshot cloneNoAcquire() {
        // Still using the clone call since Lucene call requires clone. This will allow a SegmentsInfos backed CatalogSnapshot to use the same method in calls.
        return this;
    }

    public abstract  void setUserData(Map<String, String> userData, boolean b);
}
