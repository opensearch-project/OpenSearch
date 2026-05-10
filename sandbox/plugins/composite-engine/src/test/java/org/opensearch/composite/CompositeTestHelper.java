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
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared test utilities for composite engine tests.
 */
final class CompositeTestHelper {

    private CompositeTestHelper() {}

    /**
     * Creates a CompositeIndexingExecutionEngine with stub engines for testing.
     */
    static CompositeIndexingExecutionEngine createStubEngine(String primaryName, String... secondaryNames) {
        Map<String, DataFormat> formats = new HashMap<>();
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        formats.put(primaryName, stubFormat(primaryName, 1, Set.of()));
        plugins.put(primaryName, stubPlugin(primaryName, 1));
        for (String name : secondaryNames) {
            formats.put(name, stubFormat(name, 2, Set.of()));
            plugins.put(name, stubPlugin(name, 2));
        }

        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        for (Map.Entry<String, DataFormat> entry : formats.entrySet()) {
            when(registry.format(entry.getKey())).thenReturn(entry.getValue());
        }
        when(registry.getIndexingEngine(any(), any())).thenAnswer(invocation -> {
            DataFormat format = invocation.getArgument(1);
            DataFormatPlugin plugin = plugins.get(format.name());
            return plugin.indexingEngine(null);
        });

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

        return new CompositeIndexingExecutionEngine(indexSettings, null, new StubCommitter(), registry, null, null);
    }

    static DataFormatPlugin stubPlugin(String formatName, long priority) {
        DataFormat format = stubFormat(formatName, priority, Set.of());
        return new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return format;
            }

            @Override
            public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
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
            public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
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
            return mergeInput -> new MergeResult(Map.of());
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
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) {
            return Map.of();
        }

        @Override
        public long getNextWriterGeneration() {
            return writerGeneration.getAndIncrement();
        }

        @Override
        public DocumentInput<?> newDocumentInput() {
            return new StubDocumentInput();
        }

        @Override
        public IndexStoreProvider getProvider() {
            return df -> null;
        }

        @Override
        public void close() {}
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

        @Override
        public long generation() {
            return 0;
        }
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
     * Minimal stub Committer that records close calls and does nothing on commit.
     */
    static class StubCommitter implements Committer {
        boolean closeCalled = false;

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

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            return List.of();
        }

        @Override
        public void deleteCommit(CatalogSnapshot snapshot) {}

        @Override
        public boolean isCommitManagedFile(String fileName) {
            return false;
        }
    }

    // --- Failable stubs for failure injection tests ---

    /** Builds a CompositeIndexingExecutionEngine wired with failable stubs. */
    static CompositeIndexingExecutionEngine buildFailableEngine(
        FailableEngine primary,
        FailableCommitter committer,
        FailableEngine... secondaries
    ) {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format(primary.getDataFormat().name())).thenReturn(primary.getDataFormat());
        when(registry.getIndexingEngine(any(), any())).thenAnswer(inv -> {
            DataFormat fmt = inv.getArgument(1);
            if (fmt.name().equals(primary.getDataFormat().name())) return primary;
            for (FailableEngine sec : secondaries) {
                if (fmt.name().equals(sec.getDataFormat().name())) return sec;
            }
            return null;
        });
        for (FailableEngine sec : secondaries) {
            when(registry.format(sec.getDataFormat().name())).thenReturn(sec.getDataFormat());
        }
        Settings.Builder sb = Settings.builder()
            .put("index.composite.primary_data_format", primary.getDataFormat().name())
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        if (secondaries.length > 0) {
            String[] names = new String[secondaries.length];
            for (int i = 0; i < secondaries.length; i++)
                names[i] = secondaries[i].getDataFormat().name();
            sb.putList("index.composite.secondary_data_formats", names);
        }
        IndexMetadata meta = IndexMetadata.builder("test-index").settings(sb.build()).build();
        return new CompositeIndexingExecutionEngine(new IndexSettings(meta, Settings.EMPTY), null, committer, registry, null, null);
    }

    /** Writer stub accepting {@code DocumentInput<?>} with configurable failures. */
    static class FailableWriter implements Writer<DocumentInput<?>> {
        volatile WriteResult resultToReturn = new WriteResult.Success(1, 1, 1);
        volatile IOException flushFailure;
        volatile IOException rollbackFailure;
        volatile boolean rollbackCalled;
        final AtomicInteger addDocCallCount = new AtomicInteger();

        FailableWriter(DataFormat format) {}

        void setResultToReturn(WriteResult result) {
            this.resultToReturn = result;
        }

        @Override
        public WriteResult addDoc(DocumentInput<?> d) {
            addDocCallCount.incrementAndGet();
            return resultToReturn;
        }

        @Override
        public FileInfos flush() throws IOException {
            if (flushFailure != null) throw flushFailure;
            return FileInfos.empty();
        }

        @Override
        public void rollbackLastDoc() throws IOException {
            rollbackCalled = true;
            if (rollbackFailure != null) throw rollbackFailure;
        }

        @Override
        public void sync() {}

        @Override
        public long generation() {
            return 0;
        }

        @Override
        public void lock() {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public void close() {}
    }

    /** Engine stub producing {@link FailableWriter} instances with optional default failure. */
    static class FailableEngine implements IndexingExecutionEngine<DataFormat, DocumentInput<?>> {
        private final DataFormat dataFormat;
        private final AtomicLong writerGen = new AtomicLong();
        private final List<FailableWriter> createdWriters = new ArrayList<>();
        private volatile Supplier<WriteResult> defaultWriteResultSupplier;

        FailableEngine(String name) {
            this.dataFormat = new DataFormat() {
                @Override
                public String name() {
                    return name;
                }

                @Override
                public long priority() {
                    return 1;
                }

                @Override
                public Set<FieldTypeCapabilities> supportedFields() {
                    return Set.of();
                }
            };
        }

        FailableWriter getLastCreatedWriter() {
            return createdWriters.get(createdWriters.size() - 1);
        }

        void setDefaultWriteResultSupplier(Supplier<WriteResult> s) {
            this.defaultWriteResultSupplier = s;
        }

        @Override
        public Writer<DocumentInput<?>> createWriter(long gen) {
            FailableWriter w = new FailableWriter(dataFormat);
            if (defaultWriteResultSupplier != null) w.setResultToReturn(defaultWriteResultSupplier.get());
            createdWriters.add(w);
            return w;
        }

        @Override
        public Merger getMerger() {
            return i -> new MergeResult(Map.of());
        }

        @Override
        public RefreshResult refresh(RefreshInput input) {
            return new RefreshResult(Collections.emptyList());
        }

        @Override
        public DataFormat getDataFormat() {
            return dataFormat;
        }

        @Override
        public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> f) {
            return Map.of();
        }

        @Override
        public long getNextWriterGeneration() {
            return writerGen.getAndIncrement();
        }

        @Override
        public DocumentInput<?> newDocumentInput() {
            return new MockDocumentInput();
        }

        @Override
        public IndexStoreProvider getProvider() {
            return df -> null;
        }

        @Override
        public void close() {}
    }

    /** Committer stub with configurable tragic exception. */
    static class FailableCommitter implements Committer {
        private Exception tragicException;

        void setTragicException(Exception e) {
            this.tragicException = e;
        }

        @Override
        public Exception getTragicException() {
            return tragicException;
        }

        @Override
        public void commit(Map<String, String> data) {}

        @Override
        public void close() {}

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

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            return List.of();
        }

        @Override
        public void deleteCommit(CatalogSnapshot s) {}

        @Override
        public boolean isCommitManagedFile(String f) {
            return false;
        }
    }
}
