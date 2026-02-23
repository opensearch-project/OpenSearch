/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriter;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriterCodec;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter.ROW_ID;

public class LuceneExecutionEngine implements IndexingExecutionEngine<DataFormat.LuceneDataFormat> {

    private final Logger logger;
    private final MapperService mapperService;
    private final ShardPath shardPath;
    private final DataFormat dataFormat;
    private final EngineConfig engineConfig;

    public LuceneExecutionEngine(EngineConfig engineConfig, MapperService mapperService, ShardPath shardPath, DataFormat dataFormat) {
        this.logger = Loggers.getLogger(LuceneExecutionEngine.class, shardPath.getShardId());
        this.engineConfig = engineConfig;
        this.dataFormat = dataFormat;
        this.mapperService = mapperService;
        this.shardPath = shardPath;
    }

    @Override
    public List<String> supportedFieldTypes() {
        return List.of();
    }

    @Override
    public LuceneWriter createWriter(long writerGeneration) throws IOException {
        Path directoryPath = Files.createTempDirectory(Long.toString(System.nanoTime()));
        return new LuceneWriter(directoryPath, createWriter(directoryPath, writerGeneration), writerGeneration);
    }

    private IndexWriter createWriter(Path directoryPath, long writerGeneration) throws IOException {
        try {
            IndexWriterConfig iwc = getIndexWriterConfig(writerGeneration);
            Directory directory = NIOFSDirectory.open(directoryPath);
            return new IndexWriter(directory, iwc);
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    private IndexWriterConfig getIndexWriterConfig(long writerGeneration) {
        final IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setIndexSort(new Sort(new SortField(ROW_ID, SortField.Type.LONG)));
        iwc.setCodec(new LuceneWriterCodec(engineConfig.getCodec().getName(), engineConfig.getCodec(), writerGeneration));
        return iwc;
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) {
        // Noop, as refresh is handled in layers above
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) {

    }

    @Override
    public Merger getMerger() {
        return new Merger() {
            @Override
            public MergeResult merge(List<WriterFileSet> fileMetadataList, long writerGeneration) {
                return null;
            }

            @Override
            public MergeResult merge(List<WriterFileSet> fileMetadataList, RowIdMapping rowIdMapping, long writerGeneration) {
                return null;
            }
        };
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        // NO-OP, as refresh is being handled at CompositeIndexingExecutionEngine
        return new RefreshResult();
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    @Override
    public void close() throws IOException {

    }
}
