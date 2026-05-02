/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A file-backed data format plugin for integration tests. Format name: "filebacked".
 * Writers persist doc IDs as lines in text files. Supports failure injection via static methods.
 */
public class FileBackedDataFormatPlugin extends Plugin implements DataFormatPlugin, SearchBackEndPlugin<Object> {

    public static final String FORMAT_NAME = "filebacked";

    private static final DataFormat DATA_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return FORMAT_NAME;
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    private static final AtomicInteger failOnNextNDocs = new AtomicInteger(0);
    private static final AtomicInteger failEveryNthDoc = new AtomicInteger(0);
    private static final AtomicInteger docCounter = new AtomicInteger(0);
    private static volatile boolean failOnNextRollback;
    private static volatile Path dataDirectory;

    public static void setFailOnNextNDocs(int n) {
        failOnNextNDocs.set(n);
    }

    public static void setFailEveryNthDoc(int n) {
        failEveryNthDoc.set(n);
        docCounter.set(0);
    }

    public static void setFailOnNextRollback(boolean fail) {
        failOnNextRollback = fail;
    }

    public static void clearFailure() {
        failOnNextNDocs.set(0);
        failEveryNthDoc.set(0);
        docCounter.set(0);
        failOnNextRollback = false;
    }

    public static Path getDataDirectory() {
        return dataDirectory;
    }

    /** Reads all entries from flushed filebacked data files. Each entry is "gen=N,rowId=M,fields={...}". */
    public static List<String> readAllEntries() throws IOException {
        Path dir = dataDirectory;
        if (dir == null || Files.exists(dir) == false) return List.of();
        List<String> entries = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            for (Path file : stream.filter(p -> p.getFileName().toString().endsWith(".txt")).toList()) {
                entries.addAll(Files.readAllLines(file));
            }
        }
        return entries;
    }

    /** Parses entries into a map of "gen:rowId" → "fields" for cross-format comparison. */
    public static java.util.Map<String, String> parseEntryMap() throws IOException {
        java.util.Map<String, String> map = new java.util.LinkedHashMap<>();
        for (String entry : readAllEntries()) {
            // entry format: gen=N,rowId=M,fields={...}
            String[] parts = entry.split(",", 3);
            String key = parts[0] + "," + parts[1]; // "gen=N,rowId=M"
            String fields = parts.length > 2 ? parts[2] : "";
            map.put(key, fields);
        }
        return map;
    }

    static WriteResult checkFailure() {
        int remaining;
        do {
            remaining = failOnNextNDocs.get();
            if (remaining <= 0) break;
        } while (failOnNextNDocs.compareAndSet(remaining, remaining - 1) == false);
        if (remaining > 0) {
            return new WriteResult.Failure(new IOException("filebacked injected failure"), -1, -1, -1);
        }
        int count = docCounter.incrementAndGet();
        int every = failEveryNthDoc.get();
        if (every > 0 && count % every == 0) {
            return new WriteResult.Failure(new IOException("filebacked injected failure"), -1, -1, -1);
        }
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return DATA_FORMAT;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
        return new FBEngine();
    }

    @Override
    public Map<String, java.util.function.Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings s, DataFormatRegistry r) {
        return Map.of(FORMAT_NAME, () -> new DataFormatDescriptor(FORMAT_NAME, new PrecomputedChecksumStrategy()));
    }

    @Override
    public String name() {
        return "filebacked-backend";
    }

    @Override
    public List<String> getSupportedFormats() {
        return List.of(FORMAT_NAME);
    }

    @Override
    public EngineReaderManager<?> createReaderManager(ReaderManagerConfig config) {
        return new MockReaderManager(FORMAT_NAME);
    }

    static class FBEngine implements IndexingExecutionEngine<DataFormat, DocumentInput<?>> {
        private final AtomicLong writerGen = new AtomicLong();
        private volatile Path directory;

        private Path ensureDirectory() {
            if (directory == null) {
                try {
                    directory = Files.createTempDirectory("filebacked");
                    dataDirectory = directory;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return directory;
        }

        @Override
        public Writer<DocumentInput<?>> createWriter(long writerGeneration) {
            return new FBWriter(writerGeneration, ensureDirectory());
        }

        @Override
        public DataFormat getDataFormat() {
            return DATA_FORMAT;
        }

        @Override
        public Merger getMerger() {
            return i -> new MergeResult(Map.of());
        }

        @Override
        public RefreshResult refresh(RefreshInput input) {
            List<Segment> segments = new ArrayList<>(input.existingSegments());
            segments.addAll(input.writerFiles());
            return new RefreshResult(segments);
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
            return new FBDocInput();
        }

        @Override
        public IndexStoreProvider getProvider() {
            return df -> null;
        }

        @Override
        public void close() {}
    }

    static class FBWriter implements Writer<DocumentInput<?>> {
        private final long generation;
        private final Path directory;
        private final String fileName;
        private final List<String> docs = new ArrayList<>();
        private final AtomicInteger localCount = new AtomicInteger();

        FBWriter(long generation, Path directory) {
            this.generation = generation;
            this.directory = directory;
            this.fileName = "data_gen" + generation + ".txt";
        }

        @Override
        public WriteResult addDoc(DocumentInput<?> d) {
            WriteResult failure = checkFailure();
            if (failure != null) return failure;
            long rowId = (d instanceof FBDocInput) ? ((FBDocInput) d).rowId : -1;
            String fields = (d instanceof FBDocInput) ? ((FBDocInput) d).fields.toString() : "{}";
            docs.add("gen=" + generation + ",rowId=" + rowId + ",fields=" + fields);
            return new WriteResult.Success(1L, 1L, localCount.incrementAndGet());
        }

        @Override
        public FileInfos flush() throws IOException {
            Path file = directory.resolve(fileName);
            Files.write(file, docs, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            WriterFileSet wfs = WriterFileSet.builder()
                .directory(directory)
                .writerGeneration(generation)
                .addFile(fileName)
                .addNumRows(docs.size())
                .build();
            return FileInfos.builder().putWriterFileSet(DATA_FORMAT, wfs).build();
        }

        @Override
        public void rollbackLastDoc() throws IOException {
            if (failOnNextRollback) {
                failOnNextRollback = false;
                throw new IOException("filebacked rollback injected failure");
            }
            if (docs.isEmpty() == false) docs.remove(docs.size() - 1);
        }

        @Override
        public void sync() {}

        @Override
        public long generation() {
            return generation;
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

    static class FBDocInput implements DocumentInput<Object> {
        long rowId = -1;
        final java.util.Map<String, String> fields = new java.util.LinkedHashMap<>();

        @Override
        public Object getFinalInput() {
            return null;
        }

        @Override
        public void addField(MappedFieldType fieldType, Object value) {
            if (fieldType != null && value != null) {
                fields.put(fieldType.name(), value.toString());
            }
        }

        @Override
        public void setRowId(String rowIdFieldName, long rowId) {
            this.rowId = rowId;
        }

        @Override
        public void close() {}
    }
}
