/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.ParquetSettings;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.parquet.vsr.VSRManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

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
 * <p>The returned {@link FileInfos} from {@link #flush(FlushInput)} contains the file path, writer
 * generation, and row count for downstream commit tracking.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {

    private static final Logger logger = LogManager.getLogger(ParquetWriter.class);

    private final String file;
    private final long writerGeneration;
    private final ParquetDataFormat dataFormat;
    private final VSRManager vsrManager;
    private final FormatChecksumStrategy checksumStrategy;
    private final Supplier<Schema> schemaSupplier;
    private volatile long mappingVersion;
    private volatile WriterState state = WriterState.ACTIVE;
    private long acceptedRows = 0L;

    /**
     * Creates a new ParquetWriter.
     *
     * @param file output Parquet file path
     * @param writerGeneration generation number for this writer
     * @param mappingVersion the initial mapping version
     * @param dataFormat the Parquet data format instance
     * @param schema Arrow schema for vector creation at construction time
     * @param schemaSupplier supplies the up-to-date schema when the mapping version
     *                       advances, used by {@link #updateMappingVersion} to add new
     *                       field vectors before {@code addDoc} encounters them
     * @param bufferPool shared Arrow buffer pool
     * @param indexSettings index settings for writer configuration
     * @param threadPool the thread pool for background native writes
     * @param checksumStrategy strategy to register pre-computed checksums on
     */
    public ParquetWriter(
        String file,
        long writerGeneration,
        long mappingVersion,
        ParquetDataFormat dataFormat,
        Schema schema,
        Supplier<Schema> schemaSupplier,
        ArrowBufferPool bufferPool,
        IndexSettings indexSettings,
        ThreadPool threadPool,
        FormatChecksumStrategy checksumStrategy
    ) {
        this.file = file;
        this.writerGeneration = writerGeneration;
        this.mappingVersion = mappingVersion;
        this.dataFormat = dataFormat;
        this.checksumStrategy = checksumStrategy;
        this.schemaSupplier = schemaSupplier;
        this.vsrManager = new VSRManager(
            file,
            indexSettings,
            schema,
            bufferPool,
            ParquetSettings.MAX_ROWS_PER_VSR.get(indexSettings.getSettings()),
            threadPool,
            writerGeneration
        );
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        if (state != WriterState.ACTIVE) {
            throw new IllegalStateException("Writer is not active, state=" + state);
        }
        // Schema mismatch is recoverable: the VSR rejected the doc pre-admission, so the
        // caller-driven rollback no-ops in the VSR and restores ACTIVE.
        try {
            vsrManager.addDocument(d);
        } catch (MismatchedInputException e) {
            state = WriterState.PENDING_ROLLBACK;
            return new WriteResult.Failure(e, -1, -1, -1);
        }
        acceptedRows++;
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public void rollbackTo(long rowCount) throws IOException {
        if (state != WriterState.ACTIVE && state != WriterState.PENDING_ROLLBACK) {
            throw new IllegalStateException("rollbackTo requires ACTIVE or PENDING_ROLLBACK state but was " + state);
        }
        if (rowCount > acceptedRows) {
            throw new IllegalStateException("Cannot rollback to " + rowCount + ": only " + acceptedRows + " rows admitted");
        }
        if (acceptedRows - rowCount > 1) {
            throw new IllegalStateException(
                "rollbackTo supports rolling back exactly 1 doc, but asked to roll back " + (acceptedRows - rowCount)
            );
        }
        vsrManager.rollbackTo(rowCount);
        acceptedRows = rowCount;
        state = WriterState.ACTIVE;
    }

    @Override
    public WriterState state() {
        return state;
    }

    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        ParquetFileMetadata metadata = vsrManager.flush();
        if (file == null || metadata == null || metadata.numRows() == 0) {
            return FileInfos.empty();
        }
        assert metadata.numRows() > 0 : "flushed metadata must have positive row count";

        Path filePath = Path.of(file);
        String fileName = filePath.getFileName().toString();

        // Register the pre-computed CRC32 so the upload path can read it in O(1).
        // Use the FileMetadata overload so the strategy owns key derivation.
        if (checksumStrategy != null && metadata.crc32() != 0) {
            checksumStrategy.registerChecksum(new FileMetadata(dataFormat.name(), fileName), metadata.crc32(), writerGeneration);
        }

        MonoFileWriterSet monoFileSet = MonoFileWriterSet.of(
            filePath.getParent().toAbsolutePath(),
            writerGeneration,
            fileName,
            metadata.numRows(),
            ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION
        );
        return FileInfos.builder().putWriterFileSet(dataFormat, monoFileSet).rowIdMapping(vsrManager.getRowIdMapping()).build();
    }

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public boolean isSchemaMutable() {
        return vsrManager.isSchemaMutable();
    }

    @Override
    public long mappingVersion() {
        return mappingVersion;
    }

    @Override
    public void updateMappingVersion(long newVersion) {
        if (newVersion > this.mappingVersion) {
            logger.debug(
                "[Gen: {}] updateMappingVersion: advancing from {} to {}, will reconcile schema",
                writerGeneration,
                this.mappingVersion,
                newVersion
            );
            this.mappingVersion = newVersion;
            Schema schema = schemaSupplier.get();
            logger.trace(
                "[Gen: {}] updateMappingVersion: schema from supplier has {} fields: {}",
                writerGeneration,
                schema.getFields().size(),
                schema.getFields().stream().map(f -> f.getName()).collect(java.util.stream.Collectors.joining(", "))
            );
            boolean updated = vsrManager.reconcileSchema(schema);
            logger.debug("updateMappingVersion: reconcileSchema returned updated={}", updated);
        } else {
            logger.trace(
                "[Gen: {}] updateMappingVersion: no-op, newVersion={} <= current mappingVersion={}",
                writerGeneration,
                newVersion,
                this.mappingVersion
            );
        }
    }

    @Override
    public void close() throws IOException {
        try {
            vsrManager.close();
        } finally {
            state = WriterState.CLOSED;
        }
    }

}
