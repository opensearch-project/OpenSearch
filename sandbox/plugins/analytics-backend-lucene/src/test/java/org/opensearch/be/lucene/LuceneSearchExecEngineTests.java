/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceBridge;
import org.opensearch.analytics.spi.ArrowBatchSourceBridgeHolder;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LuceneSearchExecEngineTests extends OpenSearchTestCase {

    public void testTransfersPerExecutionDocValuesFactoryToInstalledExecutor() throws Exception {
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            ByteBuffersDirectory directory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))
        ) {
            addDocument(writer, 11L);
            addDocument(writer, 22L);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                IndexSearcher searcher = new IndexSearcher(reader);
                ArrowBatchSourcePlan plan = new ArrowBatchSourcePlan(
                    "input-0",
                    new byte[] { 1 },
                    List.of(new InputColumn("x", ColumnKind.LONG))
                );
                Task task = new Task(91L, "test", "arrow-source", "arrow-source", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
                TestTracker tracker = new TestTracker();
                ShardScanExecutionContext context = new ShardScanExecutionContext("index", task, null);
                context.setAllocator(allocator);
                context.setDelegationThreadTracker(tracker);
                LuceneSearcherState state = new LuceneSearcherState(searcher, new MatchAllDocsQuery(), List.of(), plan);
                LuceneSearchExecEngine engine = new LuceneSearchExecEngine(state);
                AtomicBoolean executorCalled = new AtomicBoolean();
                ArrowBatchSourceBridge bridge = (resultAllocator, receivedPlan, sourceFactory, receivedTask, receivedTracker) -> {
                    executorCalled.set(true);
                    assertSame(allocator, resultAllocator);
                    assertSame(plan, receivedPlan);
                    assertSame(task, receivedTask);
                    assertSame(tracker, receivedTracker);
                    try (sourceFactory; ArrowBatchSource source = sourceFactory.open(new int[] { 0 })) {
                        try (VectorSchemaRoot root = source.nextBatch()) {
                            assertEquals(2, root.getRowCount());
                            BigIntVector values = (BigIntVector) root.getVector("x");
                            assertEquals(11L, values.get(0));
                            assertEquals(22L, values.get(1));
                        }
                        assertNull(source.nextBatch());
                    } catch (Exception exception) {
                        throw new RuntimeException(exception);
                    }
                    return new EmptyResultStream();
                };

                ArrowBatchSourceBridgeHolder.install(bridge);
                try (EngineResultStream stream = engine.execute(context)) {
                    assertFalse(stream.iterator().hasNext());
                } finally {
                    ArrowBatchSourceBridgeHolder.remove(bridge);
                }

                assertTrue(executorCalled.get());
                assertEquals(initialRefCount, reader.getRefCount());
            }
        }
    }

    private static void addDocument(IndexWriter writer, long value) throws Exception {
        Document document = new Document();
        document.add(new NumericDocValuesField("x", value));
        writer.addDocument(document);
    }

    private static final class TestTracker implements DelegationThreadTracker {
        @Override
        public long trackStart() {
            return Thread.currentThread().threadId();
        }

        @Override
        public void trackEnd(long threadId) {}
    }

    private static final class EmptyResultStream implements EngineResultStream {
        @Override
        public java.util.Iterator<org.opensearch.analytics.backend.EngineResultBatch> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public void close() {}
    }
}
