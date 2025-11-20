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
import java.util.function.Supplier;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted implements Writeable {

    public static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";
    public static final String LAST_COMPOSITE_WRITER_GEN_KEY = "_last_composite_writer_gen_";
    private final long id;
    private long lastWriterGeneration;
    private final Map<String, List<WriterFileSet>> dfGroupedSearchableFiles;
    private List<Segment> segmentList;
    private Supplier<IndexFileDeleter> indexFileDeleterSupplier;
    private Map<Long, CatalogSnapshot> catalogSnapshotMap;

    public CatalogSnapshot(long id, List<Segment> segmentList, Map<Long, CatalogSnapshot> catalogSnapshotMap, Supplier<IndexFileDeleter> indexFileDeleterSupplier) {
        super("catalog_snapshot_" + id);
        this.id = id;
        this.segmentList = segmentList;
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

    public CatalogSnapshot(StreamInput in) throws IOException {
        super("catalog_snapshot");
        this.id = in.readLong();
        this.lastWriterGeneration = in.readLong();

        int segmentCount = in.readVInt();
        this.segmentList = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            segmentList.add(new Segment(in));
        }

        // Rebuild dfGroupedSearchableFiles from segmentList
        this.dfGroupedSearchableFiles = new HashMap<>();
        segmentList.forEach(segment -> segment.getDFGroupedSearchableFiles().forEach((dataFormat, writerFiles) -> {
            dfGroupedSearchableFiles.computeIfAbsent(dataFormat, k -> new ArrayList<>()).add(writerFiles);
        }));
    }

    public void remapPaths(Path newShardDataPath) {
        List<Segment> remappedSegments = new ArrayList<>();
        for (Segment segment : segmentList) {
            Segment remappedSegment = new Segment(segment.getGeneration());
            for (Map.Entry<String, WriterFileSet> entry : segment.getDFGroupedSearchableFiles().entrySet()) {
                String dataFormat = entry.getKey();
                WriterFileSet originalFileSet = entry.getValue();
                WriterFileSet remappedFileSet = originalFileSet.withDirectory(newShardDataPath.toString());
                remappedSegment.addSearchableFiles(dataFormat, remappedFileSet);
            }
            remappedSegments.add(remappedSegment);
        }
        this.segmentList = remappedSegments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
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

    public List<Segment> getSegments() {
        return segmentList;
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

    public void setCatalogSnapshotMap(Map<Long, CatalogSnapshot> catalogSnapshotMap) {
        this.catalogSnapshotMap = catalogSnapshotMap;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" + "id=" + id + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + ", List of Segment= " + segmentList + '}';
    }

    public static class Segment implements Serializable, Writeable {

        private final long generation;
        private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

        public Segment(long generation) {
            this.dfGroupedSearchableFiles = new HashMap<>();
            this.generation = generation;
        }

        public Segment(StreamInput in) throws IOException {
            this.generation = in.readLong();
            this.dfGroupedSearchableFiles = new HashMap<>();
            int mapSize = in.readVInt();
            for (int i = 0; i < mapSize; i++) {
                String dataFormat = in.readString();
                WriterFileSet writerFileSet = new WriterFileSet(in);
                dfGroupedSearchableFiles.put(dataFormat, writerFileSet);
            }
        }

        public void addSearchableFiles(String dataFormat, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormat, writerFileSetGroup);
        }

        public Map<String, WriterFileSet> getDFGroupedSearchableFiles() {
            return dfGroupedSearchableFiles;
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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(generation);
            out.writeVInt(dfGroupedSearchableFiles.size());
            for (Map.Entry<String, WriterFileSet> entry : dfGroupedSearchableFiles.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }

        @Override
        public String toString() {
            return "Segment{" + "generation=" + generation + ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + '}';
        }
    }
}
