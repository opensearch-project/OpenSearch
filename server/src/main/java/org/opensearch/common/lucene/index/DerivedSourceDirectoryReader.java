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

import java.io.IOException;

/**
 * A {@link FilterDirectoryReader} that support derived source.
 *
 * @opensearch.internal
 */
public class DerivedSourceDirectoryReader extends FilterDirectoryReader {
    private final CheckedBiFunction<LeafReader, Integer, byte[], IOException> sourceProvider;
    private final SubReaderWrapper wrapper;

    private DerivedSourceDirectoryReader(
        DirectoryReader in,
        SubReaderWrapper wrapper,
        CheckedBiFunction<LeafReader, Integer, byte[], IOException> sourceProvider
    ) throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.sourceProvider = sourceProvider;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return in.getReaderCacheHelper();
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new DerivedSourceDirectoryReader(in, wrapper, sourceProvider);
    }

    public static DerivedSourceDirectoryReader wrap(
        DirectoryReader in,
        CheckedBiFunction<LeafReader, Integer, byte[], IOException> sourceProvider
    ) throws IOException {
        return new DerivedSourceDirectoryReader(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new DerivedSourceLeafReader(reader, docID -> sourceProvider.apply(reader, docID));
            }
        }, sourceProvider);
    }
}
