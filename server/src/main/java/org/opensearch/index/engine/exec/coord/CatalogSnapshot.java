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
import java.nio.file.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted implements Writeable {

    private final long id;
    private final Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles;
    private static final String CATALOG_SNAPSHOT_KEY = "_catalog_snapshot_";

    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        this.id = id;
        this.dfGroupedSearchableFiles = new HashMap<>();
        refreshResult.getRefreshedFiles().forEach((dataFormat, writerFiles) -> dfGroupedSearchableFiles.put(dataFormat.name(), writerFiles));
    }

    private CatalogSnapshot(long id, Map<String, Collection<WriterFileSet>> dfGroupedSearchableFiles) {
        super("catalog_snapshot");
        this.id = id;
        this.dfGroupedSearchableFiles = dfGroupedSearchableFiles;
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

    public CatalogSnapshot(StreamInput in) throws IOException {
        super("catalog_snapshot");
        this.id = in.readLong();
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
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

    public Map<String, String> toCommitUserData() throws IOException {
        Map<String, String> userData = new HashMap<>();
        userData.put(CATALOG_SNAPSHOT_KEY, serializeToString());
        return userData;
    }

    public static CatalogSnapshot fromCommitUserData(String userData) throws IOException {
        return deserializeFromString(userData);
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
        // notify to file deleter, search, etc
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" +
            "id=" + id +
            ", dfGroupedSearchableFiles=" + dfGroupedSearchableFiles +
            '}';
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
                WriterFileSet fileSet = new WriterFileSet(in);
                dfGroupedSearchableFiles.put(dataFormat, fileSet);
            }
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

        public void addSearchableFiles(String dataFormat, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormat, writerFileSetGroup);
        }

        public Collection<FileMetadata> getSearchableFiles(String df) {
            List<FileMetadata> searchableFiles = new ArrayList<>();
            String directory = dfGroupedSearchableFiles.get(df).getDirectory();
            for(String file : dfGroupedSearchableFiles.get(df).getFiles()) {
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
