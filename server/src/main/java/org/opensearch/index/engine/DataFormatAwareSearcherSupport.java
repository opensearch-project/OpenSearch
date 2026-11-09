/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.OpenSearchStatusException;
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
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Builds the Lucene {@link Engine.SearcherSupplier} that lets {@code _search} reach a data-format-aware
 * shard.
 *
 * <p>One instance per engine, shared by {@link DataFormatAwareEngine} for a hot shard and
 * {@link DataFormatAwareReadOnlyEngine} for one tiered to warm, so both expose the same point-in-time
 * contract. The searchable format is the one whose reader implements {@link SearchableDirectoryReaderProvider};
 * it is resolved once among the shard's own formats and then reused.
 */
@SuppressForbidden(reason = "reference counting is required here")
final class DataFormatAwareSearcherSupport {

    private final ShardId shardId;
    private final EngineConfig engineConfig;
    private final List<DataFormat> shardFormats;
    private volatile DataFormat searchableFormat;

    DataFormatAwareSearcherSupport(ShardId shardId, EngineConfig engineConfig, Set<DataFormat> shardFormats) {
        this.shardId = shardId;
        this.engineConfig = engineConfig;
        this.shardFormats = List.copyOf(shardFormats);
    }

    /**
     * Builds a point-in-time searcher supplier over the {@link OpenSearchDirectoryReader} exposed by
     * {@code readerRef}'s searchable format reader.
     *
     * <p>Like {@link Engine#acquireSearcherSupplier}, the supplier pins the store and the reader until it is
     * closed, so an open scroll or PIT survives engine close. Takes ownership of {@code readerRef}: it is
     * released by the returned supplier's {@code close()}, or immediately if this method throws.
     *
     * @throws OpenSearchStatusException with {@code 400} if none of the shard's data formats can serve {@code _search}
     */
    Engine.SearcherSupplier acquireSearcherSupplier(GatedCloseable<Reader> readerRef, Function<Engine.Searcher, Engine.Searcher> wrapper) {
        final Store store = engineConfig.getStore();
        if (store.tryIncRef() == false) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw new AlreadyClosedException(shardId + " store is closed");
        }
        boolean success = false;
        try {
            final OpenSearchDirectoryReader directoryReader = searchableDirectoryReader(readerRef.get());
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
        } catch (AlreadyClosedException | OpenSearchStatusException e) {
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

    private OpenSearchDirectoryReader searchableDirectoryReader(Reader reader) {
        DataFormat format = searchableFormat;
        if (format == null) {
            format = searchableFormat = resolveSearchableFormat(reader);
        }
        if (reader.reader(format) instanceof SearchableDirectoryReaderProvider provider) {
            // Reuse the format's wrapper: a fresh wrap gets a new cache key and defeats the request cache.
            if (provider.directoryReader() instanceof OpenSearchDirectoryReader openSearchReader) {
                return openSearchReader;
            }
            throw new IllegalStateException("Data format [" + format.name() + "] must expose an OpenSearchDirectoryReader for " + shardId);
        }
        throw new IllegalStateException("Data format [" + format.name() + "] has no searchable reader for this snapshot of " + shardId);
    }

    /**
     * Finds the single shard format whose reader implements {@link SearchableDirectoryReaderProvider}. More than
     * one is a bug, because picking one would be arbitrary.
     */
    private DataFormat resolveSearchableFormat(Reader reader) {
        DataFormat found = null;
        for (DataFormat format : shardFormats) {
            if (reader.reader(format) instanceof SearchableDirectoryReaderProvider) {
                if (found != null) {
                    throw new IllegalStateException(
                        "Data formats [" + found.name() + "] and [" + format.name() + "] both expose a searchable reader for " + shardId
                    );
                }
                found = format;
            }
        }
        if (found == null) {
            throw SearcherSupplierProvider.searchNotSupported(shardId, "none of its data formats " + formatNames() + " can serve it");
        }
        return found;
    }

    private List<String> formatNames() {
        return shardFormats.stream().map(DataFormat::name).sorted().toList();
    }
}
