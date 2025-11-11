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

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted implements Writeable {

    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    private final long id;
    private long version;
    private Map<String, String> userData;
    private long lastWriterGeneration;
    private final Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles;

    // Todo: @Kamal, update version increment logic properly
    public CatalogSnapshot(RefreshResult refreshResult, long id, long version) {
        super("catalog_snapshot");
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
    }

    private CatalogSnapshot(long id, Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles) {
        super("catalog_snapshot");
        this.id = id;
        this.dfGroupedSearchableFiles = dfGroupedSearchableFiles;
    }

    public CatalogSnapshot(StreamInput in) throws IOException {
        super("catalog_snapshot");
        this.id = in.readLong();
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
        // notify to file deleter, search, etc
    }

    public long getId() {
        return id;
    }

    /**
     * Writes this CatalogSnapshot to an OutputStream using standard Java serialization.
     * This method serializes the catalog snapshot data without relying on Lucene-specific APIs.
     *
     * @param outputStream the OutputStream to write to
     * @throws IOException if an I/O error occurs during writing
     */
    public void writeTo(OutputStream outputStream) throws IOException {
        DataOutputStream dataOut = new DataOutputStream(outputStream);

        // Write basic metadata
        dataOut.writeLong(id);
        dataOut.writeLong(version);

        // Write userData map
        dataOut.writeInt(userData.size());
        for (Map.Entry<String, String> entry : userData.entrySet()) {
            dataOut.writeUTF(entry.getKey());
            dataOut.writeUTF(entry.getValue());
        }

        // Write dfGroupedSearchableFiles map
        dataOut.writeInt(dfGroupedSearchableFiles.size());
        for (Map.Entry<String, Collection<WriterFileSet>> entry : dfGroupedSearchableFiles.entrySet()) {
            dataOut.writeUTF(entry.getKey()); // data format name

            Collection<WriterFileSet> writerFileSets = entry.getValue();
            dataOut.writeInt(writerFileSets.size());

            // Serialize each WriterFileSet using ObjectOutputStream
            for (WriterFileSet writerFileSet : writerFileSets) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(writerFileSet);
                oos.flush();

                byte[] serializedWriterFileSet = baos.toByteArray();
                dataOut.writeInt(serializedWriterFileSet.length);
                dataOut.write(serializedWriterFileSet);
            }
        }

        dataOut.flush();
    }

    /**
     * Reads a CatalogSnapshot from an InputStream using standard Java deserialization.
     * This method deserializes catalog snapshot data without relying on Lucene-specific APIs.
     *
     * @param inputStream the InputStream to read from
     * @return a new CatalogSnapshot instance
     * @throws IOException if an I/O error occurs during reading
     * @throws ClassNotFoundException if a class cannot be found during deserialization
     */
    public static CatalogSnapshot readFrom(InputStream inputStream) throws IOException, ClassNotFoundException {
        DataInputStream dataIn = new DataInputStream(inputStream);

        // Read basic metadata
        long id = dataIn.readLong();
        long version = dataIn.readLong();

        // Create CatalogSnapshot instance
        CatalogSnapshot catalog = new CatalogSnapshot(null, id, version);

        // Read userData map
        int userDataSize = dataIn.readInt();
        Map<String, String> userData = new HashMap<>();
        for (int i = 0; i < userDataSize; i++) {
            String key = dataIn.readUTF();
            String value = dataIn.readUTF();
            userData.put(key, value);
        }
        catalog.userData = userData;

        // Read dfGroupedSearchableFiles map
        int dfGroupedSize = dataIn.readInt();
        for (int i = 0; i < dfGroupedSize; i++) {
            String dataFormat = dataIn.readUTF();

            int writerFileSetsSize = dataIn.readInt();
            Collection<WriterFileSet> writerFileSets = new ArrayList<>();

            for (int j = 0; j < writerFileSetsSize; j++) {
                int serializedSize = dataIn.readInt();
                byte[] serializedData = new byte[serializedSize];
                dataIn.readFully(serializedData);

                ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
                ObjectInputStream ois = new ObjectInputStream(bais);
                WriterFileSet writerFileSet = (WriterFileSet) ois.readObject();
                writerFileSets.add(writerFileSet);
            }

            catalog.dfGroupedSearchableFiles.put(dataFormat, writerFileSets);
        }

        return catalog;
    }

    /**
     * Creates a clone of this CatalogSnapshot with the same content but independent lifecycle.
     * Similar to SegmentInfos.clone(), this allows creating independent copies that can be
     * modified without affecting the original snapshot.
     *
     * @return a new CatalogSnapshot instance with the same data
     */
    public CatalogSnapshot clone() {
        CatalogSnapshot cloned = new CatalogSnapshot(null, this.id, this.version);
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
