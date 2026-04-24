/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Parquet file writer integrating OpenSearch's {@link Writer} interface with the VSR batching layer.
 *
 * <p>Each instance corresponds to a single Parquet file for a given writer generation.
 * Documents are accepted via {@link #addDoc(ParquetDocumentInput)}, batched in Arrow vectors
 * by the {@link VSRManager}, and flushed to a Parquet file via the native Rust writer.
 *
 * <p>Writer-level settings (e.g., {@code parquet.max_rows_per_vsr}) are extracted from
 * the {@link Settings} passed at construction time and propagated to the VSR layer.
 *
 * <p>The returned {@link FileInfos} from {@link #flush()} contains the file path, writer
 * generation, and row count for downstream commit tracking.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {

    private final String objectPath;
    private final long storePointer;
    private final long writerGeneration;
    private final ParquetDataFormat dataFormat;
    private final VSRManager vsrManager;
    private final FormatChecksumStrategy checksumStrategy;

    /**
     * Creates a new ParquetWriter.
     *
     * @param storePointer native ObjectStore pointer (shard-scoped)
     * @param objectPath relative path within the store
     * @param writerGeneration generation number for this writer
     * @param dataFormat the Parquet data format instance
     * @param schema Arrow schema for vector creation
     * @param bufferPool shared Arrow buffer pool
     * @param settings node settings for writer configuration
     * @param threadPool the thread pool for background native writes
     * @param checksumStrategy strategy to register pre-computed checksums on
     */
    public ParquetWriter(
        long storePointer,
        String objectPath,
        long writerGeneration,
        ParquetDataFormat dataFormat,
        Schema schema,
        ArrowBufferPool bufferPool,
        Settings settings,
        ThreadPool threadPool,
        FormatChecksumStrategy checksumStrategy
    ) {
        this.storePointer = storePointer;
        this.objectPath = objectPath;
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.vsrManager = new VSRManager(
            storePointer,
            objectPath,
            schema,
            bufferPool,
            ParquetSettings.MAX_ROWS_PER_VSR.get(settings),
            threadPool
        );
        this.checksumStrategy = checksumStrategy;
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        vsrManager.addDocument(d);
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public FileInfos flush() throws IOException {
        ParquetFileMetadata metadata = vsrManager.flush();
        if (objectPath == null || metadata == null || metadata.numRows() == 0) {
            return FileInfos.empty();
        }
        // objectPath is a flat filename like "_parquet_file_generation_1.parquet"
        String fileName = objectPath;

        // Register the pre-computed CRC32 so the upload path can read it in O(1)
        if (checksumStrategy != null && metadata.crc32() != 0) {
            checksumStrategy.registerChecksum(fileName, metadata.crc32(), writerGeneration);
        }

        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(java.nio.file.Path.of(dataFormat.name()))
            .writerGeneration(writerGeneration)
            .addFile(fileName)
            .addNumRows(metadata.numRows())
            .build();
        return FileInfos.builder().putWriterFileSet(dataFormat, writerFileSet).build();
    }

    @Override
    public void sync() throws IOException {
        vsrManager.sync();
    }

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public void lock() {}

    @Override
    public boolean tryLock() {
        return false;
    }

    @Override
    public void unlock() {}

    @Override
    public void close() throws IOException {
        vsrManager.close();
    }

}
