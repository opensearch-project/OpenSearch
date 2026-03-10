/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FieldAssignments;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.lucene.LuceneDataFormat;
import org.opensearch.index.engine.exec.lucene.fields.LuceneFieldRegistry;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriter;
import org.opensearch.index.engine.exec.lucene.writer.LuceneWriterCodec;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter.ROW_ID;

public class LuceneExecutionEngine implements IndexingExecutionEngine<LuceneDataFormat> {

    private final MapperService mapperService;
    private final ShardPath shardPath;
    private final DataFormat dataFormat;
    private final EngineConfig engineConfig;
    private static final Logger logger = LogManager.getLogger(LuceneExecutionEngine.class);
    private final boolean isPrimaryEngine;

    public LuceneExecutionEngine(EngineConfig engineConfig, MapperService mapperService, boolean isPrimaryEngine, ShardPath shardPath, IndexSettings indexSettings, FieldAssignments fieldAssignments) {
        this.engineConfig = engineConfig;
        this.mapperService = mapperService;
        this.dataFormat = DataFormat.LUCENE;
        this.isPrimaryEngine = isPrimaryEngine;
        this.shardPath = shardPath;
        // TODO: Add check for Lucene being the primary engine and MapperService has an unknown field, currently
        // in POC it's only a secondary engine so we don't need to have all fields in this.
    }

    @Override
    public List<String> supportedFieldTypes(boolean isPrimaryEngine) {
        // Delegate to the static LuceneFieldRegistry — each registered field type is supported
        return new ArrayList<>(LuceneFieldRegistry.getRegisteredFieldNames());
    }

    @Override
    public Writer<? extends DocumentInput<?>> createWriter(long writerGeneration) throws IOException {

        Path tmpDirectoryPath = shardPath.getDataPath().resolve("tmp");
        Files.createDirectories(tmpDirectoryPath);
        Path directoryPath = Files.createTempDirectory(tmpDirectoryPath, Long.toString(writerGeneration)); // TODO:: Is this the right name?
        //Path directoryPath = Files.createTempDirectory(Long.toString(System.nanoTime())); // TODO:: Is this the right name?
        EngineRole role = isPrimaryEngine ? EngineRole.PRIMARY : EngineRole.SECONDARY;
        return new LuceneWriter(directoryPath, createWriter(directoryPath, writerGeneration), writerGeneration, role);

    }

    private IndexWriter createWriter(Path directoryPath, long writerGeneration) {
        try {
            IndexWriterConfig indexWriterConfig = getIndexWriterConfig(writerGeneration, this.engineConfig);
            Directory directory = NIOFSDirectory.open(directoryPath);
            return new IndexWriter(directory, indexWriterConfig);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create lucene writer: {}", e);
        }
    }



    public class ForceMergeOnlyPolicy extends FilterMergePolicy {

        public ForceMergeOnlyPolicy(MergePolicy wrappedPolicy) {
            super(wrappedPolicy);
        }

        // Block regular/automatic merges — return null
        @Override
        public MergeSpecification findMerges(
            MergeTrigger mergeTrigger,
            SegmentInfos segmentInfos,
            MergeContext mergeContext) throws IOException {
            // No automatic merges
            return null;
        }

        // Allow forceMerge — delegates to wrapped policy
        @Override
        public MergeSpecification findForcedMerges(
            SegmentInfos segmentInfos,
            int maxSegmentCount,
            Map<SegmentCommitInfo, Boolean> segmentsToMerge,
            MergeContext mergeContext) throws IOException {
            return in.findForcedMerges(
                segmentInfos, maxSegmentCount, segmentsToMerge, mergeContext);
        }

        // Allow forceMergeDeletes — delegates to wrapped policy
        @Override
        public MergeSpecification findForcedDeletesMerges(
            SegmentInfos segmentInfos,
            MergeContext mergeContext) throws IOException {
            return in.findForcedDeletesMerges(segmentInfos, mergeContext);
        }
    }

    private IndexWriterConfig getIndexWriterConfig(long writerGeneration, EngineConfig engineConfig) {
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setIndexSort(new Sort(new SortField(ROW_ID, SortField.Type.LONG)));
        indexWriterConfig.setCodec(new LuceneWriterCodec(engineConfig.getCodec().getName(), engineConfig.getCodec(), writerGeneration));
        MergePolicy mergePolicy = indexWriterConfig.getMergePolicy();
        indexWriterConfig.setMergePolicy(new ForceMergeOnlyPolicy(mergePolicy));
        return indexWriterConfig;
    }

    @Override
    public Merger getMerger() {
        return null;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return new LuceneDataFormat();
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) throws IOException {

    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
