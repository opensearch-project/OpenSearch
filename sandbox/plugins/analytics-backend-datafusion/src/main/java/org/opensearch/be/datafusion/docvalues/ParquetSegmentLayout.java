/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.SegmentReadState;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves the Parquet file that backs a Lucene segment's Parquet-resident doc values, and the store
 * its bytes must be read through.
 *
 * <p>Segments reach this codec with their Parquet path stamped onto the segment as
 * {@link #PARQUET_FILE_ATTRIBUTE}; this class is the read-side half, reading that stamped path back.
 * The stamping side ships with the composite-engine integration. Only the stamped path identifies
 * the segment's file.
 *
 * <p>A shard tiered to warm keeps no local copy of its Parquet files, so the engine also stamps the
 * native object store to read them through as {@link #PARQUET_STORE_ATTRIBUTE}. The stamped path is
 * still the identity of the file either way: the shard's native file registry is keyed by that same
 * absolute path.
 */
public final class ParquetSegmentLayout {

    /** {@code SegmentInfo} attribute key holding the absolute Parquet file path for the segment. */
    public static final String PARQUET_FILE_ATTRIBUTE = "parquet.docvalues.file";

    /**
     * {@code SegmentInfo} attribute key holding the native object-store pointer the Parquet file is read
     * through, as a decimal string. Absent for a shard whose Parquet files are on local disk.
     */
    public static final String PARQUET_STORE_ATTRIBUTE = "parquet.docvalues.store_ptr";

    /**
     * {@code SegmentInfo} attribute key holding the writer generation stamped onto the segment, as a
     * decimal string. The literal matches {@code LuceneWriter.WRITER_GENERATION_ATTRIBUTE} in the
     * analytics-backend-lucene plugin, which is plugin-private; the writer stamps the same generation
     * into the Parquet footer, so the two agree for the file written alongside a segment.
     */
    public static final String WRITER_GENERATION_ATTRIBUTE = "writer_generation";

    private ParquetSegmentLayout() {}

    /**
     * Where a segment's Parquet doc values are read from: the file's identity, plus the native store to
     * read it through ({@link ParquetColumnReader#LOCAL_STORE} for a file on local disk).
     */
    public record ParquetSource(Path file, long storePointer) {

        /** Whether the bytes come from a native object store rather than the local filesystem. */
        public boolean isRemote() {
            return storePointer != ParquetColumnReader.LOCAL_STORE;
        }
    }

    /**
     * Returns where {@code state}'s segment reads its Parquet doc values from, or {@code null} if the
     * segment carries no stamped path, or carries a local path that no longer exists.
     *
     * <p>The existence check applies only to a local file. A remote file is not probed: it is not
     * expected on this node's disk at all, and the store reports a genuinely missing object on the first
     * read.
     */
    public static ParquetSource resolve(SegmentReadState state) {
        String attr = state.segmentInfo.getAttribute(PARQUET_FILE_ATTRIBUTE);
        if (attr == null || attr.isEmpty()) {
            return null;
        }
        long storePointer = storePointer(state);
        Path path = Path.of(attr);
        if (storePointer == ParquetColumnReader.LOCAL_STORE && Files.exists(path) == false) {
            return null;
        }
        return new ParquetSource(path, storePointer);
    }

    /**
     * Returns the stamped native store pointer, or {@link ParquetColumnReader#LOCAL_STORE} when none is
     * stamped. An unparseable or non-positive value is treated as absent (local read); if the file is not
     * local {@link #resolve} then serves no Parquet doc values for that segment.
     */
    private static long storePointer(SegmentReadState state) {
        String attr = state.segmentInfo.getAttribute(PARQUET_STORE_ATTRIBUTE);
        if (attr == null || attr.isEmpty()) {
            return ParquetColumnReader.LOCAL_STORE;
        }
        try {
            long pointer = Long.parseLong(attr);
            return pointer > 0 ? pointer : ParquetColumnReader.LOCAL_STORE;
        } catch (NumberFormatException e) {
            return ParquetColumnReader.LOCAL_STORE;
        }
    }
}
