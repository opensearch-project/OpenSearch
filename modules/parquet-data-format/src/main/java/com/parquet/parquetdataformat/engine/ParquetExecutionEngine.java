package com.parquet.parquetdataformat.engine;

import com.parquet.parquetdataformat.merge.CompactionStrategy;
import com.parquet.parquetdataformat.merge.ParquetMergeExecutor;
import com.parquet.parquetdataformat.merge.ParquetMerger;
import com.parquet.parquetdataformat.writer.ParquetDocumentInput;
import com.parquet.parquetdataformat.writer.ParquetWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

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

    private static final Pattern FILE_PATTERN = Pattern.compile(".*_(\\d+)\\.parquet$", Pattern.CASE_INSENSITIVE);
    private static final String FILE_NAME_PREFIX = "_parquet_file_generation";
    private static final String FILE_NAME_EXT = ".parquet";

    private final Supplier<Schema> schema;
    private final List<WriterFileSet> filesWrittenAlready = new ArrayList<>();
    private final ShardPath shardPath;
    private final ParquetMerger parquetMerger = new ParquetMergeExecutor(CompactionStrategy.RECORD_BATCH);

    public ParquetExecutionEngine(Supplier<Schema> schema, ShardPath shardPath) {
        this.schema = schema;
        this.shardPath = shardPath;
    }

    @Override
    public void loadWriterFiles(ShardPath shardPath) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardPath.getDataPath(), "*" + FILE_NAME_EXT)) {
            StreamSupport.stream(stream.spliterator(), false)
                .map(Path::getFileName)
                .map(Path::toString)
                .map(FILE_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(m -> WriterFileSet.builder()
                    .directory(shardPath.getDataPath())
                    .writerGeneration(Long.parseLong(m.group(1)))
                    .addFile(m.group(0))
                    .build())
                .forEach(filesWrittenAlready::add);
        }
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public Writer<ParquetDocumentInput> createWriter(long writerGeneration) throws IOException {
        String fileName = Path.of(shardPath.getDataPath().toString(), getDataFormat().name(), FILE_NAME_PREFIX + "_" + writerGeneration + FILE_NAME_EXT).toString();
        return new ParquetWriter(fileName, schema.get(), writerGeneration);
    }

    @Override
    public Merger getMerger() {
        return parquetMerger;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        RefreshResult refreshResult = new RefreshResult();
        filesWrittenAlready.addAll(refreshInput.getWriterFiles());
        if (!refreshInput.getFilesToRemove().isEmpty()) {
            filesWrittenAlready.removeAll(refreshInput.getFilesToRemove());
        }
        refreshResult.add(PARQUET_DATA_FORMAT, filesWrittenAlready);
        return refreshResult;
    }

    @Override
    public DataFormat getDataFormat() {
        return new ParquetDataFormat();
    }
}
