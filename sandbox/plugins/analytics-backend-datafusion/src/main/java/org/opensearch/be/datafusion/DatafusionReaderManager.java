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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
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

    private final Map<CatalogSnapshot, DatafusionReader> readers = new HashMap<>();
    // TODO [BUG]: CatalogSnapshotManager creates a new snapshot instance with an incremented generation
    // on every refresh cycle, but a reader is only created when data actually changes (didRefresh=true).
    // During search, acquireSnapshot() returns the latest snapshot which may not have a reader.
    // This field tracks the most recent reader so getReader() can fall back to it.
    private volatile DatafusionReader latestReader;
    private final DataFormat dataFormat;
    private final String directoryPath;
    private final DataFusionService dataFusionService;

    /**
     * Creates a reader manager.
     * @param dataFormat the data format for this reader
     * @param shardPath the shard path to read data from
     * @param dataFusionService node-level service for cache management
     */
    public DatafusionReaderManager(DataFormat dataFormat, ShardPath shardPath, DataFusionService dataFusionService) {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name()).toString();
        this.dataFusionService = dataFusionService;
    }

    @Override
    public DatafusionReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        logger.info(
            "[getReader] looking up snapshot@{} (class={}, gen={}), readers map size={}, keys={}",
            System.identityHashCode(catalogSnapshot),
            catalogSnapshot.getClass().getSimpleName(),
            catalogSnapshot.getGeneration(),
            readers.size(),
            readers.keySet()
                .stream()
                .map(k -> k.getClass().getSimpleName() + "@" + System.identityHashCode(k) + "(gen=" + k.getGeneration() + ")")
                .collect(Collectors.joining(", "))
        );
        DatafusionReader reader = readers.get(catalogSnapshot);
        if (reader != null) return reader;
        // TODO [BUG]: Search acquires the latest catalog snapshot whose generation may not have a reader,
        // since readers are only created when data changes. Fall back to the latest valid reader.
        if (latestReader != null) return latestReader;
        throw new IOException("No DataFusion reader available");
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        DatafusionReader removed = readers.remove(catalogSnapshot);
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
        logger.info(
            "[afterRefresh] didRefresh={}, snapshot@{} (class={}, gen={}), readers map size={}",
            didRefresh,
            System.identityHashCode(catalogSnapshot),
            catalogSnapshot.getClass().getSimpleName(),
            catalogSnapshot.getGeneration(),
            readers.size()
        );
        if (didRefresh == false) return;
        if (readers.containsKey(catalogSnapshot)) return;
        var files = catalogSnapshot.getSearchableFiles(dataFormat.name());
        logger.info("[afterRefresh] searchable files for format [{}]: {}", dataFormat.name(), files != null ? files.size() : "null");
        DatafusionReader reader = new DatafusionReader(directoryPath, files);
        readers.put(catalogSnapshot, reader);
        latestReader = reader;
        logger.info("[afterRefresh] added reader, map size now={}", readers.size());
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
