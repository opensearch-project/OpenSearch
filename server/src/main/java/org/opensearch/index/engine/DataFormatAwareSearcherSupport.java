/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.engine.exec.SearchableDirectoryReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.NativeStoreHandle;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Builds the Lucene {@link Engine.SearcherSupplier} that lets {@code _search} reach a composite,
 * data-format-aware shard, and binds each Lucene leaf to the Parquet file its doc values live in.
 *
 * <p>Shared by every data-format-aware engine that can serve searches, because the binding must be
 * identical whichever engine produced the reader: {@link DataFormatAwareEngine} for a hot shard and
 * {@link DataFormatAwareReadOnlyEngine} for one tiered to warm. The two differ only in where the
 * bytes come from, which is what {@link #PARQUET_DOCVALUES_STORE_ATTRIBUTE} records.
 */
final class DataFormatAwareSearcherSupport {

    /**
     * SegmentInfo attribute key carrying the absolute Parquet file path backing a leaf's doc values.
     * Mirrors {@code ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE} in the parquet-data-format plugin;
     * duplicated as a literal here because the server module must not depend on a plugin.
     */
    static final String PARQUET_DOCVALUES_FILE_ATTRIBUTE = "parquet.docvalues.file";

    /**
     * SegmentInfo attribute key carrying the native object-store pointer the Parquet file must be read
     * through, as a decimal string. Absent for a hot shard, whose Parquet files are on local disk; a
     * warm shard's files live in the remote object store and are reachable only through its
     * {@code TieredObjectStore}. Mirrors {@code ParquetSegmentLayout.PARQUET_STORE_ATTRIBUTE}.
     *
     * <p>Carried as a segment attribute rather than plumbed through the reader-wrapper chain because
     * the store is per shard while the wrapper is registered per index, and the engine stamping these
     * attributes is the one component that holds both the shard's {@link Store} and its segments.
     */
    static final String PARQUET_DOCVALUES_STORE_ATTRIBUTE = "parquet.docvalues.store_ptr";

    /** Data format whose files back Parquet doc values. */
    private static final String PARQUET_FORMAT = "parquet";

    /** Data format supplying the Lucene {@link DirectoryReader} searches run against. */
    private static final String LUCENE_FORMAT = "lucene";

    private DataFormatAwareSearcherSupport() {}

    /**
     * Builds a point-in-time searcher supplier over {@code readerRef}'s Lucene reader. Before wrapping,
     * each segment is stamped with the path of its backing Parquet file and, on a warm shard, the store
     * to read it through, so the Parquet DocValues codec can resolve both; the reader is then wrapped in
     * an {@link OpenSearchDirectoryReader} so the standard searcher-wrapping path (and any registered
     * reader wrappers) apply.
     *
     * <p>Takes ownership of {@code readerRef}: it is released by the returned supplier's {@code close()},
     * or immediately if this method throws.
     *
     * @param store the shard's store, source of the per-format native store handles; may be
     *              {@code null} for an engine that has none
     */
    static Engine.SearcherSupplier acquireSearcherSupplier(
        ShardId shardId,
        EngineConfig engineConfig,
        Store store,
        GatedCloseable<Reader> readerRef,
        Function<Engine.Searcher, Engine.Searcher> wrapper,
        Logger logger
    ) {
        try {
            DataFormat luceneFormat = engineConfig.getDataFormatRegistry().format(LUCENE_FORMAT);
            Object luceneReaderObj = readerRef.get().reader(luceneFormat);
            if (luceneReaderObj == null) {
                throw new IllegalStateException("No Lucene reader available for composite index " + shardId);
            }
            final DirectoryReader rawDirectoryReader = extractDirectoryReader(luceneReaderObj);
            stampParquetDocValuesFiles(rawDirectoryReader, readerRef.get().catalogSnapshot(), store, shardId, logger);
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
        } catch (IllegalStateException e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw e;
        } catch (Exception e) {
            IOUtils.closeWhileHandlingException(readerRef);
            throw new EngineException(shardId, "failed to build searcher supplier from composite reader", e);
        }
    }

    /**
     * Resolves the Lucene {@link DirectoryReader} from a format-specific reader object. The lucene
     * format's reader implements {@link SearchableDirectoryReaderProvider}; the server module unwraps it
     * through that interface rather than depending on the plugin's concrete type.
     */
    private static DirectoryReader extractDirectoryReader(Object luceneReaderObj) {
        if (luceneReaderObj instanceof SearchableDirectoryReaderProvider searchable) {
            return searchable.directoryReader();
        }
        throw new IllegalStateException(
            "Lucene format reader "
                + luceneReaderObj.getClass().getName()
                + " does not implement SearchableDirectoryReaderProvider; cannot build searcher"
        );
    }

    /**
     * Binds each Lucene leaf of {@code directoryReader} to its backing Parquet file and stamps the
     * resolved absolute path onto the leaf's {@code SegmentInfo} via {@link #PARQUET_DOCVALUES_FILE_ATTRIBUTE},
     * so the Parquet DocValues codec reads the right file. Resolution matches the leaf's Lucene file set
     * against each catalog segment's Lucene {@link WriterFileSet}, then takes that segment's {@code parquet}
     * file set as the backing file - correct even for merged segments, whose per-leaf writer generation is
     * reset. Best-effort: a leaf that cannot be resolved is left unstamped and simply serves no Parquet
     * doc values rather than reading the wrong file.
     *
     * <p>On a warm shard the stamped path does not exist on local disk, so the store to read it through
     * is stamped alongside it via {@link #PARQUET_DOCVALUES_STORE_ATTRIBUTE}. Both come from the same
     * absolute path: {@code StoreStrategyRegistry} keys the shard's file registry by
     * {@code shardPath.getDataPath().resolve(file)}, which is what {@link WriterFileSet#directory()}
     * resolves to here.
     */
    private static void stampParquetDocValuesFiles(
        DirectoryReader directoryReader,
        CatalogSnapshot catalogSnapshot,
        Store store,
        ShardId shardId,
        Logger logger
    ) {
        if (catalogSnapshot == null) {
            return;
        }
        Map<Set<String>, String> luceneFilesToParquetPath = new HashMap<>();
        for (Segment segment : catalogSnapshot.getSegments()) {
            WriterFileSet luceneWfs = segment.dfGroupedSearchableFiles().get(LUCENE_FORMAT);
            WriterFileSet parquetWfs = segment.dfGroupedSearchableFiles().get(PARQUET_FORMAT);
            if (luceneWfs == null || parquetWfs == null || parquetWfs.files().isEmpty()) {
                continue;
            }
            String parquetFileName = parquetWfs.files().iterator().next();
            String parquetPath = Path.of(parquetWfs.directory(), parquetFileName).toString();
            luceneFilesToParquetPath.put(luceneWfs.files(), parquetPath);
        }

        if (luceneFilesToParquetPath.isEmpty()) {
            return;
        }

        // Resolved once for the whole reader: the store is a property of the shard, not of a leaf.
        String parquetStorePointer = parquetStorePointer(store);

        // Match each live Lucene leaf to its Parquet file by file-set and stamp the resolved path. Both
        // sides are SegmentCommitInfo.files() captured from the same snapshot, so a Parquet-backed leaf
        // matches exactly. A leaf that matches nothing is left unstamped and serves no Parquet doc values:
        // never the wrong file, but the symptom is missing values rather than an error, so the equality is
        // load-bearing. It holds today only because this engine rejects deletes; once deletes land, a
        // per-segment identifier should replace file-set equality, because a new .liv file would grow the
        // leaf's set and break the match.
        for (LeafReaderContext lrc : directoryReader.leaves()) {
            try {
                SegmentReader sr = Lucene.segmentReader(lrc.reader());
                Set<String> leafFiles = new HashSet<>(sr.getSegmentInfo().files());
                String parquetPath = luceneFilesToParquetPath.get(leafFiles);
                if (parquetPath != null) {
                    // putAttribute mutates SegmentInfo's plain HashMap, and this reader is shared by
                    // every concurrent search over a long-lived (warm) engine, so guard the
                    // first-insert window and skip the write once the stamp is present. The values
                    // never change for a given segment, so a stamped leaf needs no update.
                    SegmentInfo si = sr.getSegmentInfo().info;
                    synchronized (si) {
                        if (parquetPath.equals(si.getAttribute(PARQUET_DOCVALUES_FILE_ATTRIBUTE)) == false) {
                            si.putAttribute(PARQUET_DOCVALUES_FILE_ATTRIBUTE, parquetPath);
                            if (parquetStorePointer != null) {
                                si.putAttribute(PARQUET_DOCVALUES_STORE_ATTRIBUTE, parquetStorePointer);
                            }
                        }
                    }
                }
            } catch (IOException | RuntimeException e) {
                // Best-effort binding: a leaf that cannot be resolved is left unstamped and serves no
                // Parquet doc values. Logged because the visible symptom would otherwise be missing
                // values rather than a failure.
                logger.warn(new ParameterizedMessage("could not bind leaf [{}] to its Parquet file", lrc.ord), e);
            }
        }
    }

    /**
     * Returns the shard's native Parquet object-store pointer as a decimal string, or {@code null} when
     * the shard has none and its Parquet files are therefore local. A warm shard is the only one with a
     * handle: {@code IndexService} builds the per-format handles only for a warm, pluggable-data-format
     * index.
     */
    private static String parquetStorePointer(Store store) {
        if (store == null) {
            return null;
        }
        // Matched by name rather than resolved through DataFormatRegistry.format("parquet"), which throws
        // for an index that does not use the format at all.
        for (Map.Entry<DataFormat, NativeStoreHandle> entry : store.getDataformatAwareStoreHandles().entrySet()) {
            if (PARQUET_FORMAT.equals(entry.getKey().name()) == false) {
                continue;
            }
            NativeStoreHandle handle = entry.getValue();
            if (handle == null || handle.isLive() == false) {
                return null;
            }
            return Long.toString(handle.getPointer());
        }
        return null;
    }
}
