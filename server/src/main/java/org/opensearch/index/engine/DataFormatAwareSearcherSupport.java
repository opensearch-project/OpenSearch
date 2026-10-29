/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.engine.exec.SearchableDirectoryReaderProvider;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;
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
@SuppressForbidden(reason = "reference counting is required here")
final class DataFormatAwareSearcherSupport {

    private DataFormatAwareSearcherSupport() {}

    /**
     * Builds a point-in-time searcher supplier over the {@link OpenSearchDirectoryReader} exposed by
     * {@code readerRef}'s searchable format reader.
     *
     * <p>Like {@link Engine#acquireSearcherSupplier}, the supplier pins the store and the reader until it is
     * closed, so an open scroll or PIT survives engine close. Takes ownership of {@code readerRef}: it is
     * released by the returned supplier's {@code close()}, or immediately if this method throws.
     */
    static Engine.SearcherSupplier acquireSearcherSupplier(
        ShardId shardId,
        EngineConfig engineConfig,
        GatedCloseable<Reader> readerRef,
        Function<Engine.Searcher, Engine.Searcher> wrapper
    ) {
        final Store store = engineConfig.getStore();
        if (store.tryIncRef() == false) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw new AlreadyClosedException(shardId + " store is closed");
        }
        boolean success = false;
        try {
            final OpenSearchDirectoryReader directoryReader = searchableDirectoryReader(readerRef.get(), engineConfig, shardId);
            if (directoryReader.tryIncRef() == false) {
                throw new AlreadyClosedException(shardId + " reader is closed");
            }
            final Engine.SearcherSupplier supplier = new Engine.SearcherSupplier(wrapper) {
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
                    try {
                        release(directoryReader);
                    } finally {
                        IOUtils.closeWhileHandlingException(readerRef);
                        store.decRef();
                    }
                }
            };
            success = true;
            return supplier;
        } catch (AlreadyClosedException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineException(shardId, "failed to build searcher supplier from data format reader", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(readerRef);
                store.decRef();
            }
        }
    }

    private static void release(OpenSearchDirectoryReader reader) {
        try {
            reader.decRef();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close", e);
        }
    }

    /**
     * Returns the {@link DirectoryReader} of the single format reader implementing
     * {@link SearchableDirectoryReaderProvider}. Fails if none does, because the shard could not serve
     * {@code _search}, or if more than one does, because picking one would be arbitrary.
     */
    private static OpenSearchDirectoryReader searchableDirectoryReader(Reader reader, EngineConfig engineConfig, ShardId shardId) {
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
        // Reuse the format's wrapper: a fresh wrap gets a new cache key and defeats the request cache.
        if (searchable.directoryReader() instanceof OpenSearchDirectoryReader openSearchReader) {
            return openSearchReader;
        }
        throw new IllegalStateException(
            "Data format [" + searchableFormat.name() + "] must expose an OpenSearchDirectoryReader for " + shardId
        );
    }
}
