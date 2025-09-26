/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted {

    private final long id;
    private final List<Segment> segments;

    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        this.id = id;
        this.segments = new ArrayList<>();
        refreshResult.getWriterGenerations().forEach(writerGeneration -> {
            Segment segment = new Segment(writerGeneration);
            refreshResult.getDataFormats().forEach(dataFormat -> {
                WriterFileSet writerFileSet = refreshResult.getRefreshedFiles(dataFormat).get(writerGeneration);
                segment.addSearchableFiles(dataFormat, writerFileSet);
            });
            segments.add(segment);
        });
    }

    public Collection<WriterFileSet> getSearchableFiles(DataFormat dataFormat) {
        return segments.stream().map(segment -> segment.getSearchableFiles(dataFormat)).filter(Optional::isPresent).map(Optional::get).toList();
    }

    public List<Segment> getSegments() {
        return Collections.unmodifiableList(segments);
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
        return "CatalogSnapshot{" + "id=" + id + ", segments=" + segments + '}';
    }

    public static class Segment implements Serializable {

        private final long generation;
        private final Map<String, WriterFileSet> dfGroupedSearchableFiles;

        public Segment(long generation) {
            this.dfGroupedSearchableFiles = new HashMap<>();
            this.generation = generation;
        }

        public void addSearchableFiles(DataFormat dataFormat, WriterFileSet writerFileSetGroup) {
            dfGroupedSearchableFiles.put(dataFormat.name(), writerFileSetGroup);
        }

        public Optional<WriterFileSet> getSearchableFiles(DataFormat dataFormat) {
            if (dfGroupedSearchableFiles.containsKey(dataFormat.name())) {
                return Optional.of(dfGroupedSearchableFiles.get(dataFormat.name()));
            }
            return Optional.empty();
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
