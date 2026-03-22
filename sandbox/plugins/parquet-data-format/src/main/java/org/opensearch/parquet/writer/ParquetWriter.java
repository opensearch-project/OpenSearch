/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.arrow.vector.types.pojo.Schema;

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

    private final String file;
    private final long writerGeneration;
    private final ParquetDataFormat dataFormat;
    private final VSRManager vsrManager;

    public ParquetWriter(String file, long writerGeneration, ParquetDataFormat dataFormat, Schema schema, ArrowBufferPool bufferPool, Settings settings) {
        this.file = file;
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.vsrManager = new VSRManager(file, schema, bufferPool, ParquetSettings.MAX_ROWS_PER_VSR.get(settings));
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        vsrManager.addDocument(d);
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public FileInfos flush() throws IOException {
        ParquetFileMetadata metadata = vsrManager.flush();
        if (file == null || metadata == null) {
            return FileInfos.empty();
        }
        Path filePath = Path.of(file);
        WriterFileSet writerFileSet = WriterFileSet.builder()
            .directory(filePath.getParent())
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
