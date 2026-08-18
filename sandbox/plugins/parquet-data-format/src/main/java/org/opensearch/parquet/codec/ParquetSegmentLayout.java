/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.SegmentReadState;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves the Parquet file that backs a Lucene segment's Parquet-resident doc values.
 *
 * <p>The composite engine binds each Lucene segment to its Parquet file at search time, stamping the
 * absolute path onto the segment as {@link #PARQUET_FILE_ATTRIBUTE}. This class is the read-side half
 * of that binding: it reads the stamped path back. There is no directory scan, because a shard's
 * Parquet directory holds one file per writer generation and only the stamped path identifies the one
 * whose rows are this segment's documents; guessing would risk reading the wrong file.
 *
 * <p>Returns {@code null} when the segment carries no stamped path (or the stamped file is gone), in
 * which case the codec serves no Parquet doc values for that segment.
 */
public final class ParquetSegmentLayout {

    /** {@code SegmentInfo} attribute key holding the absolute Parquet file path for the segment. */
    public static final String PARQUET_FILE_ATTRIBUTE = "parquet.docvalues.file";

    private ParquetSegmentLayout() {}

    /**
     * Returns the Parquet file path stamped on {@code state}'s segment, or {@code null} if none is
     * stamped or the stamped file no longer exists.
     */
    public static Path resolve(SegmentReadState state) {
        String attr = state.segmentInfo.getAttribute(PARQUET_FILE_ATTRIBUTE);
        if (attr == null || attr.isEmpty()) {
            return null;
        }
        Path path = Path.of(attr);
        return Files.exists(path) ? path : null;
    }
}
