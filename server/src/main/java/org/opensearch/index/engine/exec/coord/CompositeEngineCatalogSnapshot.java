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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.*;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.*;
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
public class CompositeEngineCatalogSnapshot extends CatalogSnapshot {
    
    private static final Logger logger = LogManager.getLogger(CompositeEngineCatalogSnapshot.class);

    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    private Map<String, String> userData;
    private long lastWriterGeneration;
    private final Map<String, List<WriterFileSet>> dfGroupedSearchableFiles;
    private List<Segment> segmentList;
    private Supplier<IndexFileDeleter> indexFileDeleterSupplier;
    private Map<Long, CompositeEngineCatalogSnapshot> catalogSnapshotMap;

    public CompositeEngineCatalogSnapshot(long id, long version, List<Segment> segmentList, Map<Long, CompositeEngineCatalogSnapshot> catalogSnapshotMap, Supplier<IndexFileDeleter> indexFileDeleterSupplier) {
        super("catalog_snapshot_" + id, id, version);
        this.segmentList = segmentList;
        this.userData = new HashMap<>();
        this.dfGroupedSearchableFiles = new HashMap<>();
        this.lastWriterGeneration = -1;

        segmentList.forEach(segment -> segment.getDFGroupedSearchableFiles().forEach((dataFormat, writerFiles) -> {
            dfGroupedSearchableFiles.computeIfAbsent(dataFormat, k -> new ArrayList<>()).add(writerFiles);
            this.lastWriterGeneration = Math.max(this.lastWriterGeneration, writerFiles.getWriterGeneration());
        }));
        this.catalogSnapshotMap = catalogSnapshotMap;
        this.indexFileDeleterSupplier = indexFileDeleterSupplier;
        // Whenever a new CatalogSnapshot is created add its files to the IndexFileDeleter
        indexFileDeleterSupplier.get().addFileReferences(this);
    }

    public CompositeEngineCatalogSnapshot(StreamInput in) throws IOException {
        super(in);
        logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Starting deserialization, generation={}, version={}", generation, version);

        // Read userData map
        int userDataSize = in.readVInt();
        this.userData = new HashMap<>();
        for (int i = 0; i < userDataSize; i++) {
            String key = in.readString();
            String value = in.readString();
            userData.put(key, value);
        }
        logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Read userData with {} entries: keys={}", userDataSize, userData.keySet());

        this.lastWriterGeneration = in.readLong();
        logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Read lastWriterGeneration={}", lastWriterGeneration);

        int segmentCount = in.readVInt();
        logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Reading {} segments", segmentCount);
        this.segmentList = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            Segment segment = new Segment(in);
            segmentList.add(segment);
            logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Segment[{}]: files={}", i, segment.getDFGroupedSearchableFiles());
        }

        // Rebuild dfGroupedSearchableFiles from segmentList
        this.dfGroupedSearchableFiles = new HashMap<>();
        segmentList.forEach(segment -> segment.getDFGroupedSearchableFiles().forEach((dataFormat, writerFiles) -> {
            dfGroupedSearchableFiles.computeIfAbsent(dataFormat, k -> new ArrayList<>()).add(writerFiles);
            logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Rebuilt dfGroupedSearchableFiles for format={}: writerGeneration={}", 
                       dataFormat, writerFiles.getWriterGeneration());
        }));
        
        logger.info("[CATALOG_SNAPSHOT_DESERIALIZE] Completed. Final lastWriterGeneration={}, segmentCount={}, dataFormats={}",
                   lastWriterGeneration, segmentList.size(), dfGroupedSearchableFiles.keySet());
    }

    public void remapPaths(Path newShardDataPath) {
        List<Segment> remappedSegments = new ArrayList<>();
        for (Segment segment : segmentList) {
            Segment remappedSegment = new Segment(segment.getGeneration());
            for (Map.Entry<String, WriterFileSet> entry : segment.getDFGroupedSearchableFiles().entrySet()) {
                String dataFormat = entry.getKey();
                // TODO this path resolution should be handled by core components
                Path newDataFormatSpecificShardPath = newShardDataPath.resolve(dataFormat);
                WriterFileSet originalFileSet = entry.getValue();
                WriterFileSet remappedFileSet = originalFileSet.withDirectory(newDataFormatSpecificShardPath.toString());
                remappedSegment.addSearchableFiles(dataFormat, remappedFileSet);
            }
            remappedSegments.add(remappedSegment);
        }
        dfGroupedSearchableFiles.clear();
        this.segmentList = remappedSegments;
        segmentList.forEach(segment -> segment.getDFGroupedSearchableFiles().forEach((dataFormat, writerFiles) -> {
            dfGroupedSearchableFiles.computeIfAbsent(dataFormat, k -> new ArrayList<>()).add(writerFiles);
        }));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

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

        out.writeVInt(segmentList != null ? segmentList.size() : 0);
        if (segmentList != null) {
            for (Segment segment : segmentList) {
                segment.writeTo(out);
            }
        }
    }

    public String serializeToString() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            this.writeTo(out);
            return Base64.getEncoder().encodeToString(out.bytes().toBytesRef().bytes);
        }
    }

    public static CompositeEngineCatalogSnapshot deserializeFromString(String serializedData) throws IOException {
        byte[] bytes = Base64.getDecoder().decode(serializedData);
        try (BytesStreamInput in = new BytesStreamInput(bytes)) {
            return new CompositeEngineCatalogSnapshot(in);
        }
    }

    public Collection<WriterFileSet> getSearchableFiles(String dataFormat) {
        if (dfGroupedSearchableFiles.containsKey(dataFormat)) {
            return dfGroupedSearchableFiles.get(dataFormat);
        }
        return Collections.emptyList();
    }

    public List<Segment> getSegments() {
        return segmentList;
    }

    public Collection<FileMetadata> getFileMetadataList() throws IOException {
        Collection<Segment> segments = getSegments();
        Collection<FileMetadata> allFileMetadata = new ArrayList<>();

        for (Segment segment : segments) {
            segment.getDFGroupedSearchableFiles().forEach((dataFormatName, writerFileSet) -> {
                for (String filePath : writerFileSet.getFiles()) {
                    File file = new File(filePath);
                    String fileName = file.getName();
                    FileMetadata fileMetadata = new FileMetadata(
                        dataFormatName,
                            fileName
                    );
                    allFileMetadata.add(fileMetadata);
                }
            });
        }

        return allFileMetadata;
    }

    /**
     * Returns user data associated with this catalog snapshot.
     *
     * @return map of user data key-value pairs
     */
    public Map<String, String> getUserData() {
        return userData;
    }

    @Override
    protected void closeInternal() {
        // Notify to FileDeleter to remove references of files referenced in this CatalogSnapshot
        indexFileDeleterSupplier.get().removeFileReferences(this);
        // Remove entry from catalogSnapshotMap
        catalogSnapshotMap.remove(generation);
    }

    public long getLastWriterGeneration() {
        return lastWriterGeneration;
    }

    public Set<String> getDataFormats() {
        return dfGroupedSearchableFiles.keySet();
    }

    // used only when catalog snapshot is created from last commited segment and hence the object is not initialized with the deleter and map
    public void setIndexFileDeleterSupplier(Supplier<IndexFileDeleter> supplier) {
        if (this.indexFileDeleterSupplier == null) {
            this.indexFileDeleterSupplier = supplier;
        }
    }

    @Override
    public void setCatalogSnapshotMap(Map<Long, ? extends CatalogSnapshot> catalogSnapshotMap) {
        this.catalogSnapshotMap = (Map<Long, CompositeEngineCatalogSnapshot>) catalogSnapshotMap;
    }

    @Override
    public  void setUserData(Map<String, String> userData, boolean b)
    {
        if (userData == null) {
            this.userData = Collections.emptyMap();
        } else {
            this.userData = new HashMap<>(userData);
        }
    }

    @Override
    public long getId() {
        return generation;
    }

    @Override
    public CompositeEngineCatalogSnapshot clone() {
        CompositeEngineCatalogSnapshot cloned = new CompositeEngineCatalogSnapshot(
            this.generation,
            this.version,
            new ArrayList<>(this.segmentList),
            this.catalogSnapshotMap,
            this.indexFileDeleterSupplier
        );
        cloned.userData = new HashMap<>(this.userData);
        cloned.lastWriterGeneration = this.lastWriterGeneration;
        return cloned;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" + "id=" + generation + ", version=" + version + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + ", List of Segment= " + segmentList + ", userData=" + userData +'}';
    }

}
