/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * An {@link IndexingExecutionEngine} that creates {@link FileBackedFailableWriter}
 * instances writing real files to disk. Supports failure injection at the engine
 * and writer level, and returns proper {@link RefreshResult} with {@link Segment}
 * objects referencing the flushed files.
 */
/**
 * An {@link IndexingExecutionEngine} that creates {@link FileBackedFailableWriter} instances
 * writing real files to disk. Supports engine-level failure injection and returns proper
 * {@link RefreshResult} carrying forward segments from {@link RefreshInput}.
 */
public class FileBackedFailableIndexingExecutionEngine implements IndexingExecutionEngine<DataFormat, DocumentInput<?>> {

    private final DataFormat dataFormat;
    private final Path directory;
    private final AtomicLong writerGen = new AtomicLong();
    private final List<FileBackedFailableWriter> createdWriters = new ArrayList<>();
    private volatile Supplier<WriteResult> defaultWriteResultSupplier;

    public FileBackedFailableIndexingExecutionEngine(String name, Path baseDir) throws IOException {
        this.directory = baseDir.resolve(name);
        Files.createDirectories(this.directory);
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

            @Override
            public String toString() {
                return "FileBackedFormat{" + name + "}";
            }
        };
    }

    public FileBackedFailableWriter getLastCreatedWriter() {
        return createdWriters.get(createdWriters.size() - 1);
    }

    public List<FileBackedFailableWriter> getAllWriters() {
        return List.copyOf(createdWriters);
    }

    public void setDefaultWriteResultSupplier(Supplier<WriteResult> supplier) {
        this.defaultWriteResultSupplier = supplier;
    }

    public Path getDirectory() {
        return directory;
    }

    @Override
    public Writer<DocumentInput<?>> createWriter(long writerGeneration) {
        FileBackedFailableWriter w = new FileBackedFailableWriter(dataFormat, writerGeneration, directory);
        if (defaultWriteResultSupplier != null) {
            w.writeResultSupplier = defaultWriteResultSupplier;
        }
        createdWriters.add(w);
        return w;
    }

    @Override
    public RefreshResult refresh(RefreshInput input) {
        // Carry forward existing segments + new writer segments
        List<Segment> segments = new ArrayList<>(input.existingSegments());
        segments.addAll(input.writerFiles());
        return new RefreshResult(segments);
    }

    @Override
    public Merger getMerger() {
        return i -> new MergeResult(Map.of());
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

    public static List<String> readFlushedFile(Path filePath) throws IOException {
        if (Files.exists(filePath) == false) return Collections.emptyList();
        return Files.readAllLines(filePath);
    }
}
