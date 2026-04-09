/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Parquet file writer integrating OpenSearch's {@link Writer} interface with the VSR batching layer.
 *
 * <p>Each instance corresponds to a single Parquet file for a given writer generation.
 * Documents are accepted via {@link #addDoc(ParquetDocumentInput)}, batched in Arrow vectors
 * by the {@link VSRManager}, and flushed to a Parquet file via the native Rust writer.
 *
 * <p>Writer-level settings (e.g., {@code parquet.max_rows_per_vsr}) are extracted from
 * the {@link IndexSettings} passed at construction time and propagated to the VSR layer.
 *
 * <p>The returned {@link FileInfos} from {@link #flush()} contains the file path, writer
 * generation, and row count for downstream commit tracking.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {

    private final String file;
    private final long writerGeneration;
    private final ParquetDataFormat dataFormat;
    private final VSRManager vsrManager;

    /**
     * Creates a new ParquetWriter.
     *
     * @param file output Parquet file path
     * @param writerGeneration generation number for this writer
     * @param dataFormat the Parquet data format instance
     * @param schema Arrow schema for vector creation
     * @param bufferPool shared Arrow buffer pool
     * @param indexSettings index settings for writer configuration
     * @param threadPool the thread pool for background native writes
     */
    public ParquetWriter(
        String file,
        long writerGeneration,
        ParquetDataFormat dataFormat,
        Schema schema,
        ArrowBufferPool bufferPool,
        IndexSettings indexSettings,
        ThreadPool threadPool
    ) {
        this.file = file;
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.vsrManager = new VSRManager(
            file, indexSettings, schema, bufferPool,
            ParquetSettings.MAX_ROWS_PER_VSR.get(indexSettings.getSettings()), threadPool
        );
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        vsrManager.addDocument(d);
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public FileInfos flush() throws IOException {
        ParquetFileMetadata metadata = vsrManager.flush();
        if (file == null || metadata == null || metadata.numRows() == 0) {
            return FileInfos.empty();
        }
        Path filePath = Path.of(file);
        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(filePath.getParent().getFileName())
            .writerGeneration(writerGeneration)
            .addFile(filePath.getFileName().toString())
            .addNumRows(metadata.numRows())
            .build();
        return FileInfos.builder().putWriterFileSet(dataFormat, writerFileSet).build();
    }

    @Override
    public void sync() throws IOException {
        vsrManager.sync();
    }

    @Override
    public void close() throws IOException {
        vsrManager.close();
    }

}
