/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;

/**
 * {@link FilterDirectoryReader} that supports deriving source from lucene fields instead of directly reading from _source
 * field.
 *
 * @opensearch.internal
 */
public class DerivedSourceDirectoryReader extends FilterDirectoryReader {
    private final CheckedBiFunction<LeafReader, Integer, BytesReference, IOException> sourceProvider;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    private DerivedSourceDirectoryReader(
        DirectoryReader in,
        FilterDirectoryReader.SubReaderWrapper wrapper,
        CheckedBiFunction<LeafReader, Integer, BytesReference, IOException> sourceProvider
    ) throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.sourceProvider = sourceProvider;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader directoryReader) throws IOException {
        return new DerivedSourceDirectoryReader(in, wrapper, sourceProvider);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    public static DerivedSourceDirectoryReader wrap(
        DirectoryReader in,
        CheckedBiFunction<LeafReader, Integer, BytesReference, IOException> sourceProvider
    ) throws IOException {
        return new DerivedSourceDirectoryReader(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new DerivedSourceLeafReader(reader, docID -> sourceProvider.apply(reader, docID));
            }
        }, sourceProvider);
    }
}
