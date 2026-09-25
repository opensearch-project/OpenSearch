/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.engine.exec.SearchableDirectoryReaderProvider;

import java.util.function.Function;

/**
 * Builds the Lucene {@link Engine.SearcherSupplier} that lets {@code _search} reach a data-format-aware
 * shard.
 *
 * <p>Shared by every data-format-aware engine that can serve searches, {@link DataFormatAwareEngine} for a
 * hot shard and {@link DataFormatAwareReadOnlyEngine} for one tiered to warm, so both expose the same
 * point-in-time contract. The searchable reader is found by what it can do, not by format name: it is the
 * one format reader implementing {@link SearchableDirectoryReaderProvider}. Anything a format needs to
 * attach to that reader is the format plugin's responsibility.
 */
final class DataFormatAwareSearcherSupport {

    private DataFormatAwareSearcherSupport() {}

    /**
     * Builds a point-in-time searcher supplier over the {@link DirectoryReader} exposed by
     * {@code readerRef}'s searchable format reader, wrapped in an {@link OpenSearchDirectoryReader} so the
     * standard searcher-wrapping path (and any registered reader wrappers) apply.
     *
     * <p>Takes ownership of {@code readerRef}: it is released by the returned supplier's {@code close()},
     * or immediately if this method throws.
     */
    static Engine.SearcherSupplier acquireSearcherSupplier(
        ShardId shardId,
        EngineConfig engineConfig,
        GatedCloseable<Reader> readerRef,
        Function<Engine.Searcher, Engine.Searcher> wrapper
    ) {
        try {
            final DirectoryReader rawDirectoryReader = searchableDirectoryReader(readerRef.get(), engineConfig, shardId);
            // IndexShard.wrapSearcher later asserts the reader is an OpenSearchDirectoryReader.
            final DirectoryReader directoryReader = OpenSearchDirectoryReader.wrap(rawDirectoryReader, shardId);
            return new Engine.SearcherSupplier(wrapper) {
                @Override
                protected Engine.Searcher acquireSearcherInternal(String source) {
                    return new Engine.Searcher(
                        source,
                        directoryReader,
                        engineConfig.getSimilarity(),
                        engineConfig.getQueryCache(),
                        engineConfig.getQueryCachingPolicy(),
                        () -> {}
                    );
                }

                @Override
                protected void doClose() {
                    IOUtils.closeWhileHandlingException(readerRef);
                }
            };
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw new EngineException(shardId, "failed to build searcher supplier from data format reader", e);
        }
    }

    /**
     * Returns the {@link DirectoryReader} of the single format reader implementing
     * {@link SearchableDirectoryReaderProvider}. Fails if none does, because the shard could not serve
     * {@code _search}, or if more than one does, because picking one would be arbitrary.
     */
    private static DirectoryReader searchableDirectoryReader(Reader reader, EngineConfig engineConfig, ShardId shardId) {
        SearchableDirectoryReaderProvider searchable = null;
        DataFormat searchableFormat = null;
        for (DataFormat format : engineConfig.getDataFormatRegistry().getRegisteredFormats()) {
            if (reader.reader(format) instanceof SearchableDirectoryReaderProvider provider) {
                if (searchable != null) {
                    throw new IllegalStateException(
                        "Data formats ["
                            + searchableFormat.name()
                            + "] and ["
                            + format.name()
                            + "] both expose a searchable DirectoryReader for "
                            + shardId
                    );
                }
                searchable = provider;
                searchableFormat = format;
            }
        }
        if (searchable == null) {
            throw new IllegalStateException("No data format reader exposes a searchable DirectoryReader for " + shardId);
        }
        return searchable.directoryReader();
    }
}
