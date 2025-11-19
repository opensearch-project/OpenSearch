/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.InputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted implements Writeable {

    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    private final long id;
    private long version;
    private Map<String, String> userData;
    private long lastWriterGeneration;
    private final Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles;
    private Supplier<IndexFileDeleter> indexFileDeleterSupplier;
    private Map<Long, CatalogSnapshot> catalogSnapshotMap;

    // Todo: @Kamal, update version increment logic properly
    public CatalogSnapshot(RefreshResult refreshResult, long id, long version, Map<Long, CatalogSnapshot> catalogSnapshotMap, Supplier<IndexFileDeleter> indexFileDeleterSupplier) {
        super("catalog_snapshot_" + id);
        this.id = id;
        this.version = version;
        this.userData = new HashMap<>();
        this.dfGroupedSearchableFiles = new HashMap<>();
        if (refreshResult != null) {
            refreshResult.getRefreshedFiles().forEach((dataFormat, writerFiles) -> dfGroupedSearchableFiles.put(dataFormat.name(), writerFiles));
        }
        this.lastWriterGeneration = -1;
        refreshResult.getRefreshedFiles().forEach((dataFormat, writerFiles) -> {
            dfGroupedSearchableFiles.put(dataFormat.name(), writerFiles);
            writerFiles.stream().mapToLong(WriterFileSet::getWriterGeneration).max().ifPresent(value -> this.lastWriterGeneration = value);
        });
        this.catalogSnapshotMap = catalogSnapshotMap;
        this.indexFileDeleterSupplier = indexFileDeleterSupplier;
        // Whenever a new CatalogSnapshot is created add its files to the IndexFileDeleter
        indexFileDeleterSupplier.get().addFileReferences(this);
    }

    private CatalogSnapshot(long id, Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles) {
        super("catalog_snapshot");
        this.id = id;
        this.dfGroupedSearchableFiles = dfGroupedSearchableFiles;
    }

    public CatalogSnapshot(StreamInput in) throws IOException {
        super("catalog_snapshot");
        this.id = in.readLong();
        this.version = in.readLong();

        // Read userData map
        int userDataSize = in.readVInt();
        this.userData = new HashMap<>();
        for (int i = 0; i < userDataSize; i++) {
            String key = in.readString();
            String value = in.readString();
            userData.put(key, value);
        }

        this.lastWriterGeneration = in.readLong();
        this.dfGroupedSearchableFiles = new HashMap<>();

        int mapSize = in.readVInt();
        for (int i = 0; i < mapSize; i++) {
            String dataFormat = in.readString();
            int fileSetCount = in.readVInt();
            List<WriterFileSet> fileSets = new ArrayList<>(fileSetCount);
            for (int j = 0; j < fileSetCount; j++) {
                fileSets.add(new WriterFileSet(in));
            }
            dfGroupedSearchableFiles.put(dataFormat, fileSets);
        }
    }

    public CatalogSnapshot remapPaths(Path newShardDataPath) {
        Map<String, Collection<WriterFileSet>> remappedFiles = new HashMap<>();

        for (Map.Entry<String, Collection<WriterFileSet>> entry : dfGroupedSearchableFiles.entrySet()) {
            String dataFormat = entry.getKey();
            List<WriterFileSet> remappedFileSets = new ArrayList<>();
            Path shardDataPath = newShardDataPath.resolve(dataFormat);
            for (WriterFileSet fileSet : entry.getValue()) {
                // Create new WriterFileSet with updated directory and file paths
                WriterFileSet remappedFileSet = fileSet.withDirectory(shardDataPath.toString());
                remappedFileSets.add(remappedFileSet);
            }

            remappedFiles.put(dataFormat, remappedFileSets);
        }

        return new CatalogSnapshot(this.id, remappedFiles);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(version);

        // Write userData map
        if (userData == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(userData.size());
            for (Map.Entry<String, String> entry : userData.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
        }

        out.writeLong(lastWriterGeneration);
        out.writeVInt(dfGroupedSearchableFiles.size());
        for (Map.Entry<String, Collection<WriterFileSet>> entry : dfGroupedSearchableFiles.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (WriterFileSet fileSet : entry.getValue()) {
                fileSet.writeTo(out);
            }
        }
    }

    public String serializeToString() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            this.writeTo(out);
            return Base64.getEncoder().encodeToString(out.bytes().toBytesRef().bytes);
        }
    }

    public static CatalogSnapshot deserializeFromString(String serializedData) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(serializedData);
        try (BytesStreamInput in = new BytesStreamInput(bytes)) {
            return new CatalogSnapshot(in);
        }
    }

    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        if (dfGroupedSearchableFiles.containsKey(dataFormat)) {
            return dfGroupedSearchableFiles.get(dataFormat);
        }
        return Collections.emptyList();
    }

    public Collection<Segment> getSegments() {
        Map<Long, Segment> segmentMap = new HashMap<>();
        dfGroupedSearchableFiles.forEach((dataFormat, writerFileSets) -> writerFileSets.forEach(writerFileSet -> {
            Segment segment = segmentMap.computeIfAbsent(writerFileSet.getWriterGeneration(), Segment::new);
            segment.addSearchableFiles(dataFormat, writerFileSet);
        }));
        return Collections.unmodifiableCollection(segmentMap.values());
    }

    public Collection<FileMetadata> getFileMetadataList() {
        Collection<Segment> segments = getSegments();
        Collection<FileMetadata> allFileMetadata = new ArrayList<>();

        for (Segment segment : segments) {
            segment.dfGroupedSearchableFiles.forEach((dataFormatName, writerFileSet) -> {
                for (String filePath : writerFileSet.getFiles()) {
                    File file = new File(filePath);
                    String fileName = file.getName();
                    FileMetadata fileMetadata = new FileMetadata(
                        dataFormatName,
                        writerFileSet.getDirectory(),
                        fileName
                    );
                    allFileMetadata.add(fileMetadata);
                }
            });
        }

        return allFileMetadata;
    }

    public long getGeneration() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    /**
     * Returns user data associated with this catalog snapshot.
     *
     * @return map of user data key-value pairs
     */
    public Map<String, String> getUserData() {
        return new HashMap<>(userData);
    }

    /**
     * Sets user data for this catalog snapshot.
     *
     * @param userData the user data map to set
     * @param doIncrementVersion whether to increment version (for compatibility with SegmentInfos API)
     */
    public void setUserData(Map<String, String> userData, boolean doIncrementVersion) {
        if (userData == null) {
            this.userData = Collections.emptyMap();
        } else {
            this.userData = new HashMap<>(userData);
        }
        if (doIncrementVersion) {
            changed();
        }
    }

    public void changed() {
        version++;
    }

    @Override
    protected void closeInternal() {
        // Notify to FileDeleter to remove references of files referenced in this CatalogSnapshot
        indexFileDeleterSupplier.get().removeFileReferences(this);
        // Remove entry from catalogSnapshotMap
        catalogSnapshotMap.remove(this.id);
    }

    public long getId() {
        return id;
    }


    /**
     * Creates a clone of this CatalogSnapshot with the same content but independent lifecycle.
     * Similar to SegmentInfos.clone(), this allows creating independent copies that can be
     * modified without affecting the original snapshot.
     *
     * @return a new CatalogSnapshot instance with the same data
     */
    public CatalogSnapshot clone() {
        CatalogSnapshot cloned = new CatalogSnapshot(new RefreshResult(), this.id, this.version, this.catalogSnapshotMap, this.indexFileDeleterSupplier);
        cloned.userData = new HashMap<>(this.userData);

        for (Map.Entry<String, Collection<WriterFileSet>> entry : this.dfGroupedSearchableFiles.entrySet()) {
            String dataFormat = entry.getKey();
            Collection<WriterFileSet> writerFileSets = entry.getValue();

            Collection<WriterFileSet> clonedWriterFileSets = new ArrayList<>(writerFileSets);
            cloned.dfGroupedSearchableFiles.put(dataFormat, clonedWriterFileSets);
        }

        return cloned;
    }

    // @Override
    // public String toString() {
    //     return "CatalogSnapshot{" +
    //         "id=" + id +
    //         ", version=" + version +
    //         ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles +
    //         '}';
    // }

    public long getLastWriterGeneration() {
        return lastWriterGeneration;
    }

    public Set<String> getDataFormats() {
        return dfGroupedSearchableFiles.keySet();
    }

    // used only when catalog snapshot is created from last commited segment and hence the object is not initialized with the deleter and map
    public void setIndexFileDeleterSupplier(Supplier<IndexFileDeleter> supplier) {
        this.indexFileDeleterSupplier = supplier;
    }

    public void setCatalogSnapshotMap(Map<Long, CatalogSnapshot> catalogSnapshotMap) {
        this.catalogSnapshotMap = catalogSnapshotMap;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" + "id=" + id + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + '}';
    }

    public static class Segment implements Serializable {

        private final long generation;
        private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

        public Segment(long generation) {
            this.dfGroupedSearchableFiles = new HashMap<>();
            this.generation = generation;
        }

        public void addSearchableFiles(String dataFormat, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormat, writerFileSetGroup);
        }

        public Collection<FileMetadata> getSearchableFiles(String df) {
            List<FileMetadata> searchableFiles = new ArrayList<>();
            String directory = dfGroupedSearchableFiles.get(df).getDirectory();
            for(String file : dfGroupedSearchableFiles.get(df).getFiles()) {
                searchableFiles.add(new FileMetadata(df ,directory, file));
            }
            return searchableFiles;
        }

        public long getGeneration() {
            return generation;
        }

        @Override
        public String toString() {
            return "Segment{" + "generation=" + generation + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + '}';
        }
    }
}
