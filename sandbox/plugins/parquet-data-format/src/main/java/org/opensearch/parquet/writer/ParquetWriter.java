/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Parquet file writer that integrates with OpenSearch's Writer interface.
 *
 * <p>Uses VSRManager for Arrow-based batching and native Rust bridge for Parquet file generation.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {

    private static final Logger logger = LogManager.getLogger(ParquetWriter.class);

    private final String file;
    private final long writerGeneration;
    private final ParquetDataFormat dataFormat;
    private final VSRManager vsrManager;

    public ParquetWriter(String file, long writerGeneration, ParquetDataFormat dataFormat, Schema schema, ArrowBufferPool bufferPool) {
        this.file = file;
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.vsrManager = new VSRManager(file, schema, bufferPool);
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
