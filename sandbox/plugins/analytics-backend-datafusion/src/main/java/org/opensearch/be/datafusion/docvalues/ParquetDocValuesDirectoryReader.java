/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A {@link FilterDirectoryReader} that wraps each leaf in a {@link ParquetDocValuesLeafReader}, so
 * Parquet-resident doc values become visible to the standard OpenSearch search and aggregation path
 * at read time.
 *
 * <p>Installed as the index reader wrapper via {@code IndexModule.setReaderWrapper(...)}. Segment-core
 * resources are resolved through the per-index {@link ParquetSegmentResourceCache}: a leaf is wrapped only
 * when its core resolves a Parquet file and the mapping declares at least one codec-supported field
 * missing doc values in the Lucene segment; every other leaf passes through unchanged.
 *
 * <p>One {@link CursorRegistry} is created per wrap and shared by all leaves of that wrap. It records
 * the native cursors the request opens and is closed by {@link #doClose()} when the request ends.
 */
public final class ParquetDocValuesDirectoryReader extends FilterDirectoryReader {

    private final ParquetSegmentResourceCache cache;
    private final CursorRegistry requestCursors;

    private ParquetDocValuesDirectoryReader(DirectoryReader in, ParquetSegmentResourceCache cache, CursorRegistry requestCursors)
        throws IOException {
        super(in, new ParquetSubReaderWrapper(cache, requestCursors));
        this.cache = cache;
        this.requestCursors = requestCursors;
    }

    /** Wraps {@code in} so Parquet-resident doc values are visible to query and aggregation code. */
    public static DirectoryReader wrap(DirectoryReader in, ParquetSegmentResourceCache cache) throws IOException {
        return new ParquetDocValuesDirectoryReader(in, cache, new CursorRegistry());
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ParquetDocValuesDirectoryReader(in, cache, new CursorRegistry());
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // This reader does not change the set of live docs, so it stays cache-coherent with the
        // wrapped reader by delegating to its cache helper.
        return in.getReaderCacheHelper();
    }

    @Override
    protected void doClose() throws IOException {
        // Closes exactly the cursors this request opened; the shared per-core resources outlive it.
        requestCursors.close();
        super.doClose();
    }

    /** Per-leaf wrapper that swaps in {@link ParquetDocValuesLeafReader} when the core has Parquet resources. */
    private static final class ParquetSubReaderWrapper extends SubReaderWrapper {
        private final ParquetSegmentResourceCache cache;
        private final CursorRegistry requestCursors;

        private ParquetSubReaderWrapper(ParquetSegmentResourceCache cache, CursorRegistry requestCursors) {
            this.cache = cache;
            this.requestCursors = requestCursors;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                ParquetSegmentResources resources = cache.resourcesFor(reader);
                if (resources.isAbsent()) {
                    return reader;
                }
                return new ParquetDocValuesLeafReader(reader, resources, requestCursors);
            } catch (IOException e) {
                // SubReaderWrapper.wrap cannot throw checked exceptions; surface as unchecked so the
                // search fails loudly rather than silently dropping Parquet doc values.
                throw new UncheckedIOException("failed to wrap leaf reader for Parquet doc values", e);
            }
        }
    }
}
