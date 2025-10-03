package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.fields.ArrowFieldRegistry;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.mapper.MappedFieldType;
import com.parquet.parquetdataformat.vsr.ManagedVSR;

import java.io.IOException;

/**
 * Document input wrapper for Parquet-based document processing.
 *
 * <p>This class serves as an adapter between OpenSearch's DocumentInput interface
 * and the Arrow-based vector representation. It works directly with a {@link ManagedVSR}
 * to populate field vectors and manage document lifecycle.
 *
 * <p>The implementation follows the builder pattern, allowing incremental construction
 * of documents through field addition before finalizing the document for writing.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Direct field vector population using OpenSearch's {@link MappedFieldType}</li>
 *   <li>Document lifecycle management via ManagedVSR</li>
 *   <li>Integration with the Arrow-based Parquet writer pipeline</li>
 * </ul>
 *
 * <p>This implementation works directly with Arrow field vectors, eliminating the
 * intermediate ParquetDocument representation for improved performance and memory efficiency.
 */
public class ParquetDocumentInput implements DocumentInput<ManagedVSR> {
    private final ManagedVSR managedVSR;

    public ParquetDocumentInput(ManagedVSR managedVSR) {
        this.managedVSR = managedVSR;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        ArrowFieldRegistry.getParquetField(fieldType.typeName()).createField(fieldType, managedVSR, value);
    }

    @Override
    public ManagedVSR getFinalInput() {
        return managedVSR;
    }

    @Override
    public WriteResult addToWriter() throws IOException {
        // Complete the current document by incrementing row count
        // This will internally call setValueCount on all field vectors
        int currentRowCount = managedVSR.getRowCount();
        managedVSR.setRowCount(currentRowCount + 1);

        // TODO: Return appropriate WriteResult based on operation success
        return new WriteResult(true, null, 1, 1, 1);
    }

    @Override
    public void close() throws Exception {
        // NOTE: ParquetDocumentInput does NOT own the ManagedVSR lifecycle
        // The ManagedVSR is owned and managed by VSRManager/VSRPool
        // VSRManager.close() -> vsrPool.completeVSR(managedVSR) handles cleanup
        // ParquetDocumentInput only holds a reference for field population

        // No cleanup needed here - VSRManager handles the ManagedVSR lifecycle
    }
}
