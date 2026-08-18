/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A {@link FilterDirectoryReader} that wraps each leaf in a {@link ParquetDocValuesLeafReader}, so
 * Parquet-resident doc values become visible to the standard OpenSearch search and aggregation path
 * at read time.
 *
 * <p>Installed as the index reader wrapper via {@code IndexModule.setReaderWrapper(...)}. Per-leaf
 * wrapping self-gates: a leaf is wrapped only when a Parquet file resolves for its segment and the
 * mapping declares at least one codec-supported field missing doc values in the Lucene segment
 * (see {@link ParquetDocValuesLeafReader#wrapIfApplicable}).
 */
public final class ParquetDocValuesDirectoryReader extends FilterDirectoryReader {

    private final MapperService mapperService;

    private ParquetDocValuesDirectoryReader(DirectoryReader in, MapperService mapperService) throws IOException {
        super(in, new ParquetSubReaderWrapper(mapperService));
        this.mapperService = mapperService;
    }

    /** Wraps {@code in} so Parquet-resident doc values are visible to query and aggregation code. */
    public static DirectoryReader wrap(DirectoryReader in, MapperService mapperService) throws IOException {
        return new ParquetDocValuesDirectoryReader(in, mapperService);
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new ParquetDocValuesDirectoryReader(in, mapperService);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // This reader does not change the set of live docs, so it stays cache-coherent with the
        // wrapped reader by delegating to its cache helper.
        return in.getReaderCacheHelper();
    }

    @Override
    protected void doClose() throws IOException {
        IOException first = null;
        try {
            for (LeafReader leaf : getSequentialSubReaders()) {
                if (leaf instanceof ParquetDocValuesLeafReader parquetLeaf) {
                    try {
                        parquetLeaf.closeParquetResources();
                    } catch (IOException e) {
                        if (first == null) {
                            first = e;
                        }
                    }
                }
            }
        } finally {
            try {
                super.doClose();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /** Per-leaf wrapper that swaps in {@link ParquetDocValuesLeafReader} when applicable. */
    private static final class ParquetSubReaderWrapper extends SubReaderWrapper {
        private final MapperService mapperService;

        private ParquetSubReaderWrapper(MapperService mapperService) {
            this.mapperService = mapperService;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            try {
                return ParquetDocValuesLeafReader.wrapIfApplicable(reader, mapperService);
            } catch (IOException e) {
                // SubReaderWrapper.wrap cannot throw checked exceptions; surface as unchecked so the
                // search fails loudly rather than silently dropping Parquet doc values.
                throw new UncheckedIOException("failed to wrap leaf reader for Parquet doc values", e);
            }
        }
    }
}
