/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Objects;

/**
 * Bundles a Lucene {@link DirectoryReader} with the {@code writer_generation → segment name}
 * map built at refresh time by matching the catalog's file sets against each leaf's files.
 *
 * <p>Built by {@link LuceneReaderManager#afterRefresh} and consumed by
 * {@link LuceneFilterDelegationHandle} — no attribute reads or file-set matching at query time.
 *
 * @param directoryReader the Lucene reader at the snapshot's point in time
 * @param generationToSegmentName {@code writer_generation → SegmentInfo.name in directoryReader}
 */
@ExperimentalApi
public record LuceneReader(DirectoryReader directoryReader, Map<Long, String> generationToSegmentName) {
    public LuceneReader {
        Objects.requireNonNull(directoryReader, "directoryReader must not be null");
        generationToSegmentName = Map.copyOf(generationToSegmentName);
    }
}
