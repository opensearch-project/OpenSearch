package com.parquet.parquetdataformat.engine;

import com.parquet.parquetdataformat.bridge.RustBridge;
import com.parquet.parquetdataformat.memory.ArrowBufferPool;
import com.parquet.parquetdataformat.merge.CompactionStrategy;
import com.parquet.parquetdataformat.merge.ParquetMergeExecutor;
import com.parquet.parquetdataformat.merge.ParquetMerger;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import com.parquet.parquetdataformat.writer.ParquetWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.parquet.parquetdataformat.engine.ParquetDataFormat.PARQUET_DATA_FORMAT;

/**
 * Main execution engine for Parquet-based indexing operations in OpenSearch.
 *
 * <p>This engine implements OpenSearch's IndexingExecutionEngine interface to provide
 * Parquet file generation capabilities within the indexing pipeline. It manages the
 * lifecycle of Parquet writers and coordinates the overall document processing workflow.
 *
 * <p>Key responsibilities:
 * <ul>
 *   <li>Writer creation with unique file naming and Arrow schema integration</li>
 *   <li>Schema-based field type support and validation</li>
 *   <li>Refresh operations for completing indexing cycles</li>
 *   <li>Integration with the broader Parquet data format ecosystem</li>
 * </ul>
 *
 * <p>The engine uses an atomic counter to ensure unique Parquet file names across
 * concurrent operations, following the naming pattern "parquet_file_generation_N.parquet"
 * where N is an incrementing sequence number.
 *
 * <p>Each writer instance created by this engine is configured with:
 * <ul>
 *   <li>A unique file name for output isolation</li>
 *   <li>The Arrow schema provided during engine construction</li>
 *   <li>Full access to the Parquet processing pipeline via {@link ParquetWriter}</li>
 * </ul>
 *
 * <p>The engine is designed to work with {@link ParquetDocumentInput} for document
 * processing and integrates seamlessly with OpenSearch's execution framework.
 */
public class ParquetExecutionEngine implements IndexingExecutionEngine<ParquetDataFormat> {

    private static final Logger logger = LogManager.getLogger(ParquetExecutionEngine.class);

    public static final String FILE_NAME_PREFIX = "_parquet_file_generation";
    public static final String FILE_NAME_EXT = ".parquet";

    private final Supplier<Schema> schema;
    private final ShardPath shardPath;
    private final ParquetMerger parquetMerger = new ParquetMergeExecutor(CompactionStrategy.RECORD_BATCH);
    private final ArrowBufferPool arrowBufferPool;

    public ParquetExecutionEngine(Settings settings, Supplier<Schema> schema, ShardPath shardPath) {
        this.schema = schema;
        this.shardPath = shardPath;
        this.arrowBufferPool = new ArrowBufferPool(settings);
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) {
        // Noop, as refresh is handled in layers above
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) {
        if (filesToDelete.get(PARQUET_DATA_FORMAT.name()) != null) {
            Collection<String> parquetFilesToDelete = filesToDelete.get(PARQUET_DATA_FORMAT.name());
            for (String fileName : parquetFilesToDelete) {
                Path filePath = Paths.get(fileName);
                logger.info("Deleting file [ParquetExecutionEngine]: {}", filePath);
                try {
                    Files.delete(filePath);
                } catch (Exception e) {
                    logger.error("Failed to delete file [ParquetExecutionEngine]: {}", filePath, e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public Writer<ParquetDocumentInput> createWriter(long writerGeneration) {
        String fileName = Path.of(shardPath.getDataPath().toString(), FILE_NAME_PREFIX + "_" + writerGeneration + FILE_NAME_EXT).toString();
        return new ParquetWriter(fileName, schema.get(), writerGeneration, arrowBufferPool);
    }

    @Override
    public Merger getMerger() {
        return parquetMerger;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) {
        // NO-OP, as refresh is being handled at CompositeIndexingExecutionEngine
        return new RefreshResult();
    }

    @Override
    public DataFormat getDataFormat() {
        return new ParquetDataFormat();
    }

    @Override
    public long getNativeBytesUsed() {
        long vsrMemory = arrowBufferPool.getTotalAllocatedBytes();
        String shardDataPath = shardPath.getDataPath().toString();
        long filteredArrowWriterMemory = RustBridge.getFilteredNativeBytesUsed(shardDataPath);
        logger.debug("Native memory used by VSR Buffer Pool: {}", vsrMemory);
        logger.debug("Native memory used by ArrowWriters in shard path {}: {}", shardDataPath, filteredArrowWriterMemory);
        return vsrMemory + filteredArrowWriterMemory;
    }

    @Override
    public void close() throws IOException {
        arrowBufferPool.close();
    }
}
