package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.vsr.VSRManager;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Path;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Parquet file writer implementation that integrates with OpenSearch's Writer interface.
 *
 * <p>This writer provides a high-level interface for writing Parquet documents to disk
 * using the underlying VSRManager for Arrow-based data management and native Rust
 * backend for efficient Parquet file generation.
 *
 * <p>Key features:
 * <ul>
 *   <li>Arrow schema-based document structure</li>
 *   <li>Batch-oriented writing with memory management</li>
 *   <li>Integration with OpenSearch indexing pipeline</li>
 *   <li>Native Rust backend for high-performance Parquet operations</li>
 * </ul>
 *
 * <p>The writer manages the complete lifecycle from document addition through
 * flushing and cleanup, delegating the actual Arrow and Parquet operations
 * to the {@link VSRManager}.
 */
public class ParquetWriter implements Writer<ParquetDocumentInput> {
    private final String file;
    private final Schema schema;
    private final VSRManager vsrManager;
    private final long writerGeneration;

    public ParquetWriter(String file, Schema schema, long writerGeneration) {
        this.file = file;
        this.schema = schema;
        this.vsrManager = new VSRManager(file, schema);
        this.writerGeneration = writerGeneration;
    }

    @Override
    public WriteResult addDoc(ParquetDocumentInput d) throws IOException {
        return vsrManager.addToManagedVSR(d);
    }

    @Override
    public FileInfos flush(FlushIn flushIn) throws IOException {
        String fileName = vsrManager.flush(flushIn);
        FileInfos fileInfos = new FileInfos();
        WriterFileSet writerFileSet = new WriterFileSet(Path.of(fileName).getParent(), writerGeneration);
        writerFileSet.add(fileName);
        fileInfos.putWriterFileSet(PARQUET_DATA_FORMAT, writerFileSet);
        return fileInfos;
    }

    @Override
    public void sync() throws IOException {

    }

    @Override
    public void close() {
        vsrManager.close();
    }

    @Override
    public ParquetDocumentInput newDocumentInput() {
        // Get a new ManagedVSR from VSRManager for this document input
        return new ParquetDocumentInput(vsrManager.getActiveManagedVSR());
    }
}
