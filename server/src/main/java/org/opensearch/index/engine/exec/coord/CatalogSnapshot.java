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

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted implements Writeable {

    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    private final long id;
    private long lastWriterGeneration;
    private final Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles;
    private static IndexFileDeleter indexFileDeleter;
    private static Map<Long, CatalogSnapshot> catalogSnapshotMap;

    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        this.id = id;
        this.dfGroupedSearchableFiles = new HashMap<>();
        this.lastWriterGeneration = -1;
        refreshResult.getRefreshedFiles().forEach((dataFormat, writerFiles) -> {
            dfGroupedSearchableFiles.put(dataFormat.name(), writerFiles);
            writerFiles.stream().mapToLong(WriterFileSet::getWriterGeneration).max().ifPresent(value -> this.lastWriterGeneration = value);
        });
        // Whenever a new CatalogSnapshot is created add its files to the IndexFileDeleter
        indexFileDeleter.addFileReferences(this);
    }

    public static void setIndexFileDeleter(IndexFileDeleter deleter) {
        indexFileDeleter = deleter;
    }

    public static void setCatalogSnapshotMap(Map<Long, CatalogSnapshot> map) {
        catalogSnapshotMap = map;
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
            for (WriterFileSet fileSet : entry.getValue()) {
                // Create new WriterFileSet with updated directory and file paths
                WriterFileSet remappedFileSet = fileSet.withDirectory(newShardDataPath.toString());
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

    @Override
    protected void closeInternal() {
        // Notify to FileDeleter to remove references of files referenced in this CatalogSnapshot
        if (indexFileDeleter != null) {
            indexFileDeleter.removeFileReferences(this);
        }
        // Remove entry from catalogSnapshotMap
        catalogSnapshotMap.remove(this.id);
    }

    public long getId() {
        return id;
    }

    public long getLastWriterGeneration() {
        return lastWriterGeneration;
    }

    public Set<String> getDataFormats() {
        return dfGroupedSearchableFiles.keySet();
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
            for (String file : dfGroupedSearchableFiles.get(df).getFiles()) {
                searchableFiles.add(new FileMetadata(directory, file));
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
