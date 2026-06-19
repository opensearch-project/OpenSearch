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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.NativeStoreHandle;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     */
    public DatafusionReaderManager(
        DataFormat dataFormat,
        ShardPath shardPath,
        DataFusionService dataFusionService,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders
    ) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
        this.dataformatAwareStoreHandle = dataformatAwareStoreHandle;
        this.sortFields = sortFields == null ? List.of() : List.copyOf(sortFields);
        this.sortOrders = sortOrders == null ? List.of() : List.copyOf(sortOrders);
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
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesDeleted(toAbsolutePaths(files));
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        if (files == null || files.isEmpty()) return;
        dataFusionService.onFilesAdded(toAbsolutePaths(files));
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false) return;
        if (readers.containsKey(catalogSnapshot.getId())) return;
        DatafusionReader reader = new DatafusionReader(
            directoryPath,
            catalogSnapshot.getSearchableFiles(dataFormat.name()),
            dataformatAwareStoreHandle,
            sortFields,
            sortOrders
        );
        readers.put(catalogSnapshot.getId(), reader);
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
    }
}
