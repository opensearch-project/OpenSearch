/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;

/**
 * A mock {@link IndexingExecutionEngine} for testing purposes.
 */
public class MockIndexingExecutionEngine implements IndexingExecutionEngine<DataFormat, MockDocumentInput> {
    private final MockDataFormat dataFormat;
    private final Path directory;
    private final AtomicLong seqNo = new AtomicLong(0);
    private final AtomicLong writerGeneration = new AtomicLong(0);

    public MockIndexingExecutionEngine(MockDataFormat dataFormat) {
        this.dataFormat = dataFormat;
        this.directory = createTempDir();
    }

    @Override
    public Writer<MockDocumentInput> createWriter(WriterConfig config) {
        return new MockWriter(config.writerGeneration(), dataFormat, directory, seqNo);
    }

    @Override
    public Merger getMerger() {
        return new MockMerger(dataFormat, directory);
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) {
        List<Segment> segments = new ArrayList<>(refreshInput.existingSegments());
        segments.addAll(refreshInput.writerFiles());
        return new RefreshResult(segments);
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
    public MockDocumentInput newDocumentInput() {
        return new MockDocumentInput();
    }

    @Override
    public IndexStoreProvider getProvider() {
        return null;
    }

    @Override
    public void close() {
        // no-op for mock
    }

    @Override
    public Map<DataFormat, EngineReaderManager<?>> buildReaderManager(ReaderManagerConfig config) throws IOException {
        return Map.of(getDataFormat(), new MockReaderManager(getDataFormat().name()));
    }
}
