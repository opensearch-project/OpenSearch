package com.parquet.parquetdataformat.writer;

import com.parquet.parquetdataformat.fields.ArrowFieldRegistry;
import com.parquet.parquetdataformat.fields.ParquetField;
import org.apache.arrow.vector.BigIntVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.parquet.parquetdataformat.engine.ParquetDataFormat;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.MappedFieldType;
import com.parquet.parquetdataformat.vsr.ManagedVSR;

import java.io.IOException;
import java.util.Objects;

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
 *   <li>Direct field vector population using {@link MappedFieldType}</li>
 *   <li>Document lifecycle management via ManagedVSR</li>
 *   <li>Integration with the Arrow-based Parquet writer pipeline</li>
 * </ul>
 *
 * <p>This implementation works directly with Arrow field vectors, eliminating the
 * intermediate ParquetDocument representation for improved performance and memory efficiency.
 */
public class ParquetDocumentInput implements DocumentInput<ManagedVSR> {
    private static final Logger logger = LogManager.getLogger(ParquetDocumentInput.class);
    private final ManagedVSR managedVSR;
    private final EngineRole engineRole;

    public ParquetDocumentInput(ManagedVSR managedVSR, EngineRole engineRole) {
        this.managedVSR = Objects.requireNonNull(managedVSR, "managedVSR must not be null");
        this.engineRole = Objects.requireNonNull(engineRole, "engineRole must not be null");
    }

    @Override
    public void addRowIdField(String fieldName, long rowId) {
        BigIntVector bigIntVector = (BigIntVector) managedVSR.getVector(CompositeDataFormatWriter.ROW_ID);
        int rowCount = managedVSR.getRowCount();
        bigIntVector.setSafe(rowCount, rowId);
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        final ParquetField parquetField = ArrowFieldRegistry.getParquetField(fieldType.typeName());

        if (parquetField == null) {
             logger.debug("[COMPOSITE_DEBUG] Parquet SKIP field=[{}] type=[{}] — no ParquetField registered in ArrowFieldRegistry", fieldType.name(), fieldType.typeName());
            return;
        }

        logger.debug("[COMPOSITE_DEBUG] Parquet ACCEPT field=[{}] type=[{}] value=[{}]", fieldType.name(), fieldType.typeName(), value);
        parquetField.createField(fieldType, managedVSR, value);
    }

    @Override
    public void setPrimaryTerm(String fieldName, long primaryTerm) {
        BigIntVector bigIntVector = (BigIntVector) managedVSR.getVector(fieldName);
        int rowCount = managedVSR.getRowCount();
        bigIntVector.setSafe(rowCount, primaryTerm);
    }

    @Override
    public ManagedVSR getFinalInput() {
        return managedVSR;
    }

    @Override
    public EngineRole getEngineRole() {
        return engineRole;
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
    public DataFormat getDataFormat() {
        return ParquetDataFormat.PARQUET_DATA_FORMAT;
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
