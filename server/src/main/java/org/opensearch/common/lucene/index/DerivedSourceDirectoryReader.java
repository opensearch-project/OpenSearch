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
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.util.List;

/**
 * {@link FilterDirectoryReader} that supports deriving source from lucene fields instead of directly reading from _source
 * field.
 *
 * @opensearch.internal
 */
public class DerivedSourceDirectoryReader extends FilterDirectoryReader {
    private final LeafSourceProviderFactory sourceProviderFactory;

    /**
     * Factory for creating a per-leaf source provider for a specific leaf context.
     */
    @FunctionalInterface
    public interface LeafSourceProviderFactory {
        /**
         * Returns a function that can build source bytes for docs in this leaf.
         *
         * @param context the lucene leaf context
         * @return function that accepts a document id and returns source bytes
         */
        CheckedFunction<Integer, BytesReference, IOException> apply(LeafReaderContext context);
    }

    private DerivedSourceDirectoryReader(
        DirectoryReader in,
        FilterDirectoryReader.SubReaderWrapper wrapper,
        LeafSourceProviderFactory sourceProviderFactory
    ) throws IOException {
        super(in, wrapper);
        this.sourceProviderFactory = sourceProviderFactory;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader directoryReader) throws IOException {
        return wrap(directoryReader, sourceProviderFactory);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    public static DerivedSourceDirectoryReader wrap(DirectoryReader in, LeafSourceProviderFactory sourceProviderFactory)
        throws IOException {
        final List<LeafReaderContext> leafReaderContexts = in.leaves();
        return new DerivedSourceDirectoryReader(in, new SubReaderWrapper() {
            @Override
            protected LeafReader[] wrap(List<? extends LeafReader> readers) {
                if (readers.size() != leafReaderContexts.size()) {
                    throw new IllegalStateException("Leaf reader context count does not match wrapped reader count");
                }
                LeafReader[] wrappedReaders = new LeafReader[readers.size()];
                for (int i = 0; i < readers.size(); i++) {
                    LeafReader reader = readers.get(i);
                    LeafReaderContext context = leafReaderContexts.get(i);
                    if (context.reader() != reader) {
                        throw new IllegalStateException("Leaf reader context does not match wrapped reader");
                    }
                    wrappedReaders[i] = new DerivedSourceLeafReader(reader, sourceProviderFactory.apply(context));
                }
                return wrappedReaders;
            }

            @Override
            public LeafReader wrap(LeafReader reader) {
                throw new UnsupportedOperationException("Leaf readers must be wrapped in order");
            }
        }, sourceProviderFactory);
    }
}
