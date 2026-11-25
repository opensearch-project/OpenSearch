/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implemented by a reader manager whose data format knows which rows are still reachable.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface LiveDocsSource {

    /**
     * Live and hidden document counts for the given snapshot, keyed by writer generation.
     *
     * @param catalogSnapshot the snapshot to report on
     * @return per-generation counts, empty if this source cannot answer for this snapshot
     * @throws IOException if reading the underlying reader fails
     */
    Map<Long, DocCounts> docCountsByGeneration(CatalogSnapshot catalogSnapshot) throws IOException;

    /**
     * Picks the live docs sources out of a shard's reader managers once, and returns a function that
     * asks them for per-generation counts.
     *
     * @param readerManagers the shard's reader managers, in any order
     * @return a resolver that returns an empty map when no format holds liveness information
     */
    static CheckedFunction<CatalogSnapshot, Map<Long, DocCounts>, IOException> docCountsResolver(
        Collection<? extends EngineReaderManager<?>> readerManagers
    ) {
        List<LiveDocsSource> sources = new ArrayList<>();
        for (EngineReaderManager<?> readerManager : readerManagers) {
            if (readerManager instanceof LiveDocsSource) {
                sources.add((LiveDocsSource) readerManager);
            }
        }
        assert sources.size() <= 1 : "at most one data format may hold liveness information, found " + sources.size();
        if (sources.isEmpty()) {
            return catalogSnapshot -> Map.of();
        }
        return catalogSnapshot -> {
            Map<Long, DocCounts> counts = new HashMap<>();
            for (LiveDocsSource source : sources) {
                source.docCountsByGeneration(catalogSnapshot)
                    .forEach((generation, generationCounts) -> counts.merge(generation, generationCounts, DocCounts::plus));
            }
            return counts;
        };
    }
}
