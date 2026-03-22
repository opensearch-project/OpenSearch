/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.engine;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.writer.ParquetWriter;

import java.io.Closeable;
import java.util.Collections;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Per-shard Parquet indexing execution engine.
 *
 * <p>Implements {@link IndexingExecutionEngine} to integrate with OpenSearch's data format
 * framework. Each shard gets its own engine instance, which manages:
 * <ul>
 *   <li>A shared {@link ArrowBufferPool} for Arrow memory allocation across all writers.</li>
 *   <li>Writer creation per writer generation, each producing a separate Parquet file.</li>
 *   <li>Native memory usage reporting (Arrow allocations + Rust-side allocations).</li>
 * </ul>
 *
 * <p>Node-level {@link Settings} are passed through to each {@link ParquetWriter} at creation
 * time, where writer-specific settings (e.g., {@code index.parquet.max_rows_per_vsr}) are
 * extracted and applied.
 */
public class ParquetIndexingEngine implements IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput>, Closeable {

    private static final Logger logger = LogManager.getLogger(ParquetIndexingEngine.class);

    public static final String FILE_NAME_PREFIX = "_parquet_file_generation";
    public static final String FILE_NAME_EXT = ".parquet";

    private final ParquetDataFormat dataFormat;
    private final ShardPath shardPath;
    private final Supplier<Schema> schemaSupplier;
    private final ArrowBufferPool bufferPool;
    private final IndexSettings indexSettings;
    private final Settings settings;

    public ParquetIndexingEngine(
        Settings settings,
        ParquetDataFormat dataFormat,
        ShardPath shardPath,
        Supplier<Schema> schemaSupplier,
        IndexSettings indexSettings
    ) {
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.schemaSupplier = schemaSupplier;
        this.bufferPool = new ArrowBufferPool(settings);
        this.indexSettings = indexSettings;
        this.settings = settings;
    }

    @Override
    public Writer<ParquetDocumentInput> createWriter(long writerGeneration) {
        Path filePath = Path.of(
            shardPath.getDataPath().toString(),
            dataFormat.name(),
            FILE_NAME_PREFIX + "_" + writerGeneration + FILE_NAME_EXT
        );
        return new ParquetWriter(filePath.toString(), writerGeneration, dataFormat, schemaSupplier.get(), bufferPool, settings);
    }

    @Override
    public long getNativeBytesUsed() {
        return bufferPool.getTotalAllocatedBytes() + RustBridge.getFilteredNativeBytesUsed(shardPath.getDataPath().toString());
    }

    @Override
    public Merger getMerger() {
        return null;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return new RefreshResult(Collections.emptyList());
    }

    @Override
    public ParquetDataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        Collection<String> parquetFiles = filesToDelete.get(dataFormat.name());
        if (parquetFiles == null) {
            return;
        }
        for (String fileName : parquetFiles) {
            Path filePath = Path.of(fileName);
            logger.info("Deleting parquet file: {}", filePath);
            Files.deleteIfExists(filePath);
        }
    }

    @Override
    public ParquetDocumentInput newDocumentInput() {
        return new ParquetDocumentInput();
    }

    @Override
    public void close() throws IOException {
        bufferPool.close();
    }
}
