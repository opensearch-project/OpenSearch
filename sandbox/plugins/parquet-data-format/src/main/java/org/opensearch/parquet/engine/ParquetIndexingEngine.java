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
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.NativeSettings;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.merge.ParquetMergeExecutor;
import org.opensearch.parquet.merge.StreamingParquetMergeStrategy;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.writer.ParquetWriter;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
 * time, where writer-specific settings (e.g., {@code parquet.max_rows_per_vsr}) are
 * extracted and applied.
 */
public class ParquetIndexingEngine implements IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput>, Closeable {

    private static final Logger logger = LogManager.getLogger(ParquetIndexingEngine.class);

    /** Prefix for generated Parquet file names. */
    public static final String FILE_NAME_PREFIX = "_parquet_file_generation";
    /** File extension for Parquet files. */
    public static final String FILE_NAME_EXT = ".parquet";

    private final ParquetDataFormat dataFormat;
    private final ShardPath shardPath;
    private final Supplier<Schema> schemaSupplier;
    private final ArrowBufferPool bufferPool;
    private final IndexSettings indexSettings;
    private final ThreadPool threadPool;
    private final Merger parquetMerger;

    /**
     * Creates a new ParquetIndexingEngine.
     *
     * @param settings       the node-level settings
     * @param dataFormat     the Parquet data format descriptor
     * @param shardPath      the shard path for file storage
     * @param schemaSupplier supplier for the Arrow schema
     * @param indexSettings  the index-level settings
     * @param threadPool     the thread pool for background native writes
     */
    public ParquetIndexingEngine(
        Settings settings,
        ParquetDataFormat dataFormat,
        ShardPath shardPath,
        Supplier<Schema> schemaSupplier,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.schemaSupplier = schemaSupplier;
        this.bufferPool = new ArrowBufferPool(settings);
        this.indexSettings = indexSettings;
        this.threadPool = threadPool;
        this.parquetMerger = new ParquetMergeExecutor(new StreamingParquetMergeStrategy());
        pushSettingsToRust();
    }

    private void pushSettingsToRust() {
        NativeSettings config = NativeSettings.builder()
            .indexName(indexSettings.getIndex().getName())
            .compressionType(indexSettings.getValue(ParquetSettings.COMPRESSION_TYPE))
            .compressionLevel(indexSettings.getValue(ParquetSettings.COMPRESSION_LEVEL))
            .pageSizeBytes(indexSettings.getValue(ParquetSettings.PAGE_SIZE_BYTES).getBytes())
            .pageRowLimit(indexSettings.getValue(ParquetSettings.PAGE_ROW_LIMIT))
            .dictSizeBytes(indexSettings.getValue(ParquetSettings.DICT_SIZE_BYTES).getBytes())
            .rowGroupSizeBytes(indexSettings.getValue(ParquetSettings.ROW_GROUP_SIZE_BYTES).getBytes())
            .build();
        try {
            RustBridge.onSettingsUpdate(config);
        } catch (Exception e) {
            logger.error("Failed to push Parquet settings to Rust store", e);
        }
    }

    @Override
    public Writer<ParquetDocumentInput> createWriter(long writerGeneration) {
        Path filePath = Path.of(
            shardPath.getDataPath().toString(),
            dataFormat.name(),
            FILE_NAME_PREFIX + "_" + writerGeneration + FILE_NAME_EXT
        );
        return new ParquetWriter(
            filePath.toString(), writerGeneration, dataFormat, schemaSupplier.get(),
            bufferPool, indexSettings, threadPool
        );
    }

    @Override
    public long getNativeBytesUsed() {
        return bufferPool.getTotalAllocatedBytes() + RustBridge.getFilteredNativeBytesUsed(shardPath.getDataPath().toString());
    }

    @Override
    public Merger getMerger() {
        return parquetMerger;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        if (refreshInput == null) {
            return new RefreshResult(List.of());
        }
        List<Segment> segments = new ArrayList<>(refreshInput.existingSegments());
        long gen = segments.stream().mapToLong(Segment::generation).max().orElse(-1) + 1;
        for (WriterFileSet wfs : refreshInput.writerFiles()) {
            segments.add(Segment.builder(gen++).addSearchableFiles(dataFormat, wfs).build());
        }
        return new RefreshResult(segments);
    }

    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException("getNextWriterGeneration not supported");
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
            logger.debug("Deleting parquet file: {}", filePath);
            boolean deleted = Files.deleteIfExists(filePath);
            if (deleted == false) {
                logger.warn("Failed to delete parquet file: {}", filePath);
            }
        }
    }

    @Override
    public ParquetDocumentInput newDocumentInput() {
        return new ParquetDocumentInput();
    }

    @Override
    public void close() throws IOException {
        try {
            RustBridge.removeSettings(indexSettings.getIndex().getName());
        } catch (Exception e) {
            logger.warn("Failed to remove Parquet settings from Rust store for index [{}]", indexSettings.getIndex().getName(), e);
        }
        bufferPool.close();
    }
}
