/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared test utilities for composite engine tests.
 */
final class CompositeTestHelper {

    private CompositeTestHelper() {}

    /**
     * Creates a CompositeIndexingExecutionEngine with stub engines for testing.
     */
    static CompositeIndexingExecutionEngine createStubEngine(String primaryName, String... secondaryNames) {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put(primaryName, stubPlugin(primaryName, 1));
        for (String name : secondaryNames) {
            plugins.put(name, stubPlugin(name, 2));
        }

        Settings.Builder settingsBuilder = Settings.builder()
            .put("index.composite.primary_data_format", primaryName)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);

        if (secondaryNames.length > 0) {
            settingsBuilder.putList("index.composite.secondary_data_formats", secondaryNames);
        }

        Settings settings = settingsBuilder.build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        return new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, new StubCommitter());
    }

    static DataFormatPlugin stubPlugin(String formatName, long priority) {
        DataFormat format = stubFormat(formatName, priority, Set.of());
        return new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return format;
            }

            @Override
            public IndexingExecutionEngine<?, ?> indexingEngine(
                Committer committer,
                MapperService mapperService,
                ShardPath shardPath,
                IndexSettings indexSettings
            ) {
                return new StubIndexingExecutionEngine(format);
            }
        };
    }

    static DataFormatPlugin stubPlugin(String formatName, long priority, Set<FieldTypeCapabilities> fields) {
        DataFormat format = stubFormat(formatName, priority, fields);
        return new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return format;
            }

            @Override
            public IndexingExecutionEngine<?, ?> indexingEngine(
                Committer committer,
                MapperService mapperService,
                ShardPath shardPath,
                IndexSettings indexSettings
            ) {
                return new StubIndexingExecutionEngine(format);
            }
        };
    }

    static DataFormat stubFormat(String name, long priority, Set<FieldTypeCapabilities> fields) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return fields;
            }

            @Override
            public String toString() {
                return "StubDataFormat{" + name + "}";
            }
        };
    }

    /**
     * Minimal stub IndexingExecutionEngine that returns no-op writers and empty results.
     */
    static class StubIndexingExecutionEngine implements IndexingExecutionEngine<DataFormat, DocumentInput<?>> {

        private final DataFormat dataFormat;
        private final AtomicLong writerGeneration = new AtomicLong(0);

        StubIndexingExecutionEngine(DataFormat dataFormat) {
            this.dataFormat = dataFormat;
        }

        @Override
        public Writer<DocumentInput<?>> createWriter(long writerGeneration) {
            return new StubWriter(dataFormat);
        }

        @Override
        public Merger getMerger() {
            return null;
        }

        @Override
        public RefreshResult refresh(RefreshInput refreshInput) {
            return new RefreshResult(Collections.emptyList());
        }

        @Override
        public DataFormat getDataFormat() {
            return dataFormat;
        }

        @Override
        public void deleteFiles(Map<String, Collection<String>> filesToDelete) {}

        @Override
        public long getNextWriterGeneration() {
            return writerGeneration.getAndIncrement();
        }

        @Override
        public DocumentInput<?> newDocumentInput() {
            return new StubDocumentInput();
        }
    }

    /**
     * Minimal stub Writer that always succeeds and returns empty FileInfos.
     */
    static class StubWriter implements Writer<DocumentInput<?>> {

        private final DataFormat format;
        private WriteResult resultToReturn = new WriteResult.Success(1, 1, 1);

        StubWriter(DataFormat format) {
            this.format = format;
        }

        void setResultToReturn(WriteResult result) {
            this.resultToReturn = result;
        }

        @Override
        public WriteResult addDoc(DocumentInput<?> d) {
            return resultToReturn;
        }

        @Override
        public FileInfos flush() {
            return FileInfos.empty();
        }

        @Override
        public void sync() {}

        @Override
        public void close() {}
    }

    /**
     * Minimal stub DocumentInput.
     */
    static class StubDocumentInput implements DocumentInput<Object> {
        @Override
        public Object getFinalInput() {
            return null;
        }

        @Override
        public void addField(org.opensearch.index.mapper.MappedFieldType fieldType, Object value) {}

        @Override
        public void setRowId(String rowIdFieldName, long rowId) {}

        @Override
        public void close() {}
    }

    /**
     * Minimal stub Committer that records init/close calls and does nothing on commit.
     */
    static class StubCommitter implements Committer {
        boolean initCalled = false;
        boolean closeCalled = false;

        @Override
        public void init(CommitterSettings settings) {
            initCalled = true;
        }

        @Override
        public void commit(Map<String, String> commitData) {}

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public Map<String, String> getLastCommittedData() {
            return Map.of();
        }

        @Override
        public CommitStats getCommitStats() {
            return null;
        }

        @Override
        public SafeCommitInfo getSafeCommitInfo() {
            return SafeCommitInfo.EMPTY;
        }
    }
}
