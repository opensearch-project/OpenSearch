/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.manage;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.exec.engine.FileMetadata;
import org.opensearch.index.engine.exec.engine.RefreshResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CatalogSnapshot represents a searchable view of the data-files that has been created for the data present in the index.
 * This is recreated upon every refresh/commit performed on the index.
 * It is used for sharing the file metadata for searchable data across multiple search engines along with semantics around
 * acquire/release so that operations like indexing/merge/searches can work on a view of data guaranteed to exist if being referenced,
 * allowing to easily manage lifecycle of various files which are part of the index.
 *
 * The id associated to a CatalogSnapshot should always be higher than the previous instance as more data is indexed.
 *
 */
@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted {

    private Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles = new HashMap<>();
    private final long id;

    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        refreshResult.getRefreshedFiles()
            .forEach((df, files) -> { dfGroupedSearchableFiles.put(df.name(), Collections.unmodifiableList(files)); });
        this.id = id;
    }

    public Iterable<String> dataFormats() {
        return dfGroupedSearchableFiles.keySet();
    }

    public Collection<FileMetadata> getSearchableFiles(String df) {
        return dfGroupedSearchableFiles.get(df);
    }

    /**
     * Creates a generation by generation view for files across data formats.
     */
    public List<Segment> getSegments() {
        Map<Long, Segment.Builder> segments = new HashMap<>();
        for (Map.Entry<String, Collection<FileMetadata>> entry : dfGroupedSearchableFiles.entrySet()) {
            for (FileMetadata fileMetadata : entry.getValue()) {
                segments.compute(fileMetadata.generation(), (k, v) -> {
                    return Objects.requireNonNullElseGet(v, () -> new Segment.Builder(k)).addFileMetadata(fileMetadata);
                });
            }
        }
        return segments.values().stream().map(Segment.Builder::build).sorted().toList();
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
        return "CatalogSnapshot{" + "dfGroupedSearchableFiles=" + dfGroupedSearchableFiles + ", id=" + id + '}';
    }

    @ExperimentalApi
    public static class Segment implements Comparable<Segment> {
        private final long generation;
        private final Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles;

        public Segment(Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles, long generation) {
            this.dfGroupedSearchableFiles = dfGroupedSearchableFiles;
            this.generation = generation;
        }

        public Collection<FileMetadata> getSearchableFiles(String df) {
            return dfGroupedSearchableFiles.get(df);
        }

        public long getGeneration() {
            return generation;
        }

        @Override
        public int compareTo(Segment o) {
            if (this.generation < o.generation) {
                return -1;
            } else if (this.generation > o.generation) {
                return 1;
            }
            return 0;
        }

        private static class Builder {
            private Map<String, List<FileMetadata>> dfGroupedSearchableFiles = new HashMap<>();
            private final long generation;

            Builder(long generation) {
                this.generation = generation;
            }

            Builder addFileMetadata(FileMetadata fileMetadata) {
                dfGroupedSearchableFiles.compute(fileMetadata.df().name(), (df, fm) -> {
                    if (fm == null) {
                        fm = new ArrayList<>();
                    }
                    fm.add(fileMetadata);
                    return fm;
                });
                return this;
            }

            Segment build() {
                Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles = new HashMap<>(this.dfGroupedSearchableFiles);
                return new Segment(dfGroupedSearchableFiles, generation);
            }
        }
    }
}
