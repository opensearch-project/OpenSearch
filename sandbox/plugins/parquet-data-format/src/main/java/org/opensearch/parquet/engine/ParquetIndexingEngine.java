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
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.writer.ParquetDocumentInput;
import org.opensearch.parquet.writer.ParquetWriter;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
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
public class ParquetIndexingEngine implements IndexingExecutionEngine<ParquetDataFormat, ParquetDocumentInput> {

    private static final Logger logger = LogManager.getLogger(ParquetIndexingEngine.class);

    /** Prefix for generated Parquet file names. */
    public static final String FILE_NAME_PREFIX = "_parquet_file_generation";
    /** File extension for Parquet files. */
    public static final String FILE_NAME_EXT = ".parquet";

    private final ParquetDataFormat dataFormat;
    private final ShardPath shardPath;
    private final Supplier<Schema> schemaSupplier;
    private final ArrowBufferPool bufferPool;
    private final Settings settings;
    private final ThreadPool threadPool;
    private final FormatChecksumStrategy checksumStrategy;

    /**
     * Creates a new ParquetIndexingEngine.
     *
     * @param settings          the node-level settings
     * @param dataFormat        the Parquet data format descriptor
     * @param shardPath         the shard path for file storage
     * @param schemaSupplier    supplier for the Arrow schema
     * @param indexSettings     the index-level settings
     * @param threadPool        the thread pool for background native writes
     */
    public ParquetIndexingEngine(
        Settings settings,
        ParquetDataFormat dataFormat,
        ShardPath shardPath,
        Supplier<Schema> schemaSupplier,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this(settings, dataFormat, shardPath, schemaSupplier, indexSettings, threadPool, new PrecomputedChecksumStrategy());
    }

    /**
     * Creates a new ParquetIndexingEngine with an externally provided checksum strategy.
     *
     * <p>Use this constructor when the checksum strategy is shared with the
     * {@link org.opensearch.index.store.DataFormatAwareStoreDirectory} so that
     * pre-computed CRC32 values registered during write are visible to the upload path.
     *
     * @param settings          the node-level settings
     * @param dataFormat        the Parquet data format descriptor
     * @param shardPath         the shard path for file storage
     * @param schemaSupplier    supplier for the Arrow schema
     * @param indexSettings     the index-level settings
     * @param threadPool        the thread pool for background native writes
     * @param checksumStrategy  the checksum strategy to use (shared with the directory)
     */
    public ParquetIndexingEngine(
        Settings settings,
        ParquetDataFormat dataFormat,
        ShardPath shardPath,
        Supplier<Schema> schemaSupplier,
        IndexSettings indexSettings,
        ThreadPool threadPool,
        FormatChecksumStrategy checksumStrategy
    ) {
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.schemaSupplier = schemaSupplier;
        this.bufferPool = new ArrowBufferPool(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.checksumStrategy = checksumStrategy;
        try {
            Files.createDirectory(shardPath.resolve("parquet"));
        } catch (FileAlreadyExistsException ex) {
            logger.warn("Directory already exists: {}", shardPath.resolve("parquet"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the checksum strategy for this engine's Parquet files.
     * Used by the upload path to retrieve pre-computed checksums.
     */
    @Override
    public FormatChecksumStrategy getChecksumStrategy() {
        return checksumStrategy;
    }

    @Override
    public Writer<ParquetDocumentInput> createWriter(long writerGeneration) {
        Path filePath = Path.of(
            shardPath.getDataPath().toString(),
            dataFormat.name(),
            FILE_NAME_PREFIX + "_" + writerGeneration + FILE_NAME_EXT
        );
        return new ParquetWriter(
            filePath.toString(),
            writerGeneration,
            dataFormat,
            schemaSupplier.get(),
            bufferPool,
            settings,
            threadPool,
            checksumStrategy
        );
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
        if (refreshInput == null) {
            return new RefreshResult(List.of());
        }
        List<Segment> segments = new ArrayList<>();
        segments.addAll(refreshInput.existingSegments());
        segments.addAll(refreshInput.writerFiles());
        return new RefreshResult(List.copyOf(segments));
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
        return new ParquetDocumentInput(this.dataFormat);
    }

    @Override
    public IndexStoreProvider getProvider() {
        return null;
    }

    @Override
    public void close() throws IOException {
        bufferPool.close();
    }
}
