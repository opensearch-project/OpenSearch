/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.docvalues.ParquetSegmentBindings;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.NativeStoreHandle;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manages {@link DatafusionReader} instances per shard.
 * <p>
 * On refresh, a new reader is created from the updated catalog snapshot.
 * File lifecycle events (add/delete) are delegated to the node-level
 * {@link DataFusionService} for cache management.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private static final Logger logger = LogManager.getLogger(DatafusionReaderManager.class);

    private final Map<Long, DatafusionReader> readers = new HashMap<>();
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;
    private final NativeStoreHandle dataformatAwareStoreHandle;
    /** This shard's id, kept so refresh/delete/close can key the plugin-owned {@link ParquetSegmentBindings}. */
    private final ShardId shardId;
    /**
     * Node-level registry the Parquet doc-values codec resolves backing files from. The server no longer
     * stamps parquet paths onto {@code SegmentInfo}; this manager populates the registry from each
     * refreshed {@link CatalogSnapshot} instead. Shared across all shards, keyed by {@link #shardId}.
     */
    private final ParquetSegmentBindings parquetSegmentBindings;
    /**
     * Sourced from {@code index.sort.field} once at construction; passed to every
     * {@link DatafusionReader} created on refresh so the native side can declare
     * file sort order to DataFusion's query optimizer.
     */
    private final List<String> sortFields;
    /** Parallel to {@link #sortFields}; values are {@code "asc"} or {@code "desc"}. */
    private final List<String> sortOrders;

    /**
     * Creates a reader manager.
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     * @param dataFusionService node-level service for cache management
     * @param dataformatAwareStoreHandle per-format native store handle for reads (null if not available).
     *                                   Pointer is extracted at reader creation time via {@code getPointer()}.
     *                                   0 means use default local file system.
     * @param sortFields {@code index.sort.field} values, or empty if no index sort. Threaded to the native
     *                   reader so the indexed scan path can decide whether to iterate segments in reverse
     *                   catalog-snapshot order to feed a {@code TopK} above us.
     * @param sortOrders {@code index.sort.order} values ("asc"/"desc"), parallel to {@code sortFields}.
     * @param parquetSegmentBindings node-level registry this manager populates from each refreshed
     *                               catalog snapshot so the Parquet doc-values codec can resolve backing
     *                               files without server-side {@code SegmentInfo} stamping.
     */
    public DatafusionReaderManager(
        DataFormat dataFormat,
        ShardPath shardPath,
        DataFusionService dataFusionService,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders,
        ParquetSegmentBindings parquetSegmentBindings
    ) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
        this.dataformatAwareStoreHandle = dataformatAwareStoreHandle;
        this.sortFields = sortFields == null ? List.of() : List.copyOf(sortFields);
        this.sortOrders = sortOrders == null ? List.of() : List.copyOf(sortOrders);
        this.shardId = shardPath.getShardId();
        this.parquetSegmentBindings = parquetSegmentBindings;
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (catalogSnapshot == null) {
            throw new IllegalArgumentException("catalogSnapshot must not be null");
        }
        DatafusionReader reader = readers.get(catalogSnapshot.getId());
        if (reader == null) {
            throw new IOException("No DataFusion reader available for catalog snapshot [version=" + catalogSnapshot.getId() + "]");
        }
        return reader;
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DatafusionReader removed = readers.remove(catalogSnapshot.getId());
        if (removed != null) {
            removed.close();
        }
        // Release this snapshot's Parquet bindings. A generation still referenced by another live
        // snapshot of this shard stays resolvable, because resolve searches live snapshots newest-first.
        parquetSegmentBindings.release(shardId, catalogSnapshot.getId());
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesDeleted(toAbsolutePaths(files));
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        Collection<String> absolutePaths = toAbsolutePaths(files);
        long storePtr = storePointerOrDefault(dataformatAwareStoreHandle);
        if (storePtr > 0) {
            dataFusionService.onFilesAddedWithStore(absolutePaths, storePtr);
        } else {
            dataFusionService.onFilesAdded(absolutePaths);
        }
    }

    /**
     * Resolves the native store pointer for cache warming. Returns {@link ParquetColumnReader#LOCAL_STORE}
     * when there is no live handle (no per-shard remote store, e.g. hot tier) so the caller falls back to
     * the legacy local-FS warming path.
     */
    private static long storePointerOrDefault(NativeStoreHandle handle) {
        // Fall back to LOCAL_STORE unless there is a live per-shard remote store, exactly as the deleted
        // server code did (it only stamped a store pointer when handle.isLive()); this keeps the single
        // "pointer > 0 means remote, otherwise LOCAL_STORE" predicate true everywhere downstream.
        if (handle == null || handle.isLive() == false) {
            return ParquetColumnReader.LOCAL_STORE;
        }
        try {
            long pointer = handle.getPointer();
            return pointer > 0 ? pointer : ParquetColumnReader.LOCAL_STORE;
        } catch (IllegalStateException closed) {
            // Handle closed between the liveness check and extraction — fall back to local.
            return ParquetColumnReader.LOCAL_STORE;
        }
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false) return;
        // Idempotent per snapshot id: mirror the reader guard so each snapshot is seen exactly once,
        // including the initial afterRefresh at open (warm engines fire this exactly once).
        if (readers.containsKey(catalogSnapshot.getId())) return;
        // Populate the plugin-owned binding registry from this snapshot before any searcher over it is
        // acquired. The server no longer stamps parquet paths onto SegmentInfo; the codec resolves them
        // from these bindings at wrap time (on search threads), so they must be visible first.
        registerParquetBindings(catalogSnapshot);
        DatafusionReader reader = new DatafusionReader(
            directoryPath,
            catalogSnapshot.getSearchableFiles(dataFormat.name()),
            dataformatAwareStoreHandle,
            sortFields,
            sortOrders
        );
        readers.put(catalogSnapshot.getId(), reader);
    }

    /**
     * Builds this snapshot's {@code generation -> Binding} map from its parquet {@link WriterFileSet}s and
     * records it under the snapshot id, so the codec can resolve a segment's Parquet file by the segment's
     * {@code writer_generation} attribute (which equals the catalog {@link Segment}'s generation).
     */
    private void registerParquetBindings(CatalogSnapshot catalogSnapshot) {
        // Store pointer is a per-shard property, resolved once for the whole snapshot. For a hot/local
        // shard this yields 0 (== ParquetColumnReader.LOCAL_STORE), exactly as the old server code
        // stamped no store attribute; a warm shard yields its live native store pointer.
        long storePointer = storePointerOrDefault(dataformatAwareStoreHandle);
        Map<Long, ParquetSegmentBindings.Binding> generationBindings = new HashMap<>();
        for (Segment segment : catalogSnapshot.getSegments()) {
            WriterFileSet parquetWfs = segment.dfGroupedSearchableFiles().get(dataFormat.name());
            if (parquetWfs == null || parquetWfs.files().isEmpty()) {
                continue;
            }
            String parquetFileName = firstParquetFile(parquetWfs, segment.generation());
            // Mirror the exact path composition the deleted server code used: Path.of(directory, file).
            Path parquetFile = Path.of(parquetWfs.directory(), parquetFileName);
            generationBindings.put(segment.generation(), new ParquetSegmentBindings.Binding(parquetFile, storePointer));
        }
        parquetSegmentBindings.register(shardId, catalogSnapshot.getId(), generationBindings);
    }

    /**
     * The single Parquet file backing a segment. Parquet is a mono-file-per-generation format, so a set
     * of more than one file is not expected; if it ever happens, pick deterministically (lexicographically
     * smallest) rather than relying on iteration order, and log at debug — the server likewise took a
     * single best-effort file per segment.
     */
    private static String firstParquetFile(WriterFileSet parquetWfs, long generation) {
        Set<String> files = parquetWfs.files();
        if (files.size() == 1) {
            return files.iterator().next();
        }
        String chosen = files.stream().sorted().findFirst().orElseThrow();
        logger.debug("parquet WriterFileSet for generation {} has {} files {}; picking {}", generation, files.size(), files, chosen);
        return chosen;
    }

    private Collection<String> toAbsolutePaths(Collection<String> fileNames) {
        return fileNames.stream().map(f -> directoryPath + "/" + f).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        for (DatafusionReader reader : readers.values()) {
            reader.close();
        }
        readers.clear();
        // The manager and its shard are going away; drop every binding for this shard.
        parquetSegmentBindings.removeShard(shardId);
    }
}
