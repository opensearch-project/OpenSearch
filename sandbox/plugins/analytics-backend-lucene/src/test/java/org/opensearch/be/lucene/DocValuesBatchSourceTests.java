/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DocValuesBatchSourceTests extends OpenSearchTestCase {

    public void testProjectionNullsAndEof() throws Exception {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document first = new Document();
            first.add(new SortedNumericDocValuesField("n", 10));
            first.add(new SortedSetDocValuesField("k", new BytesRef("a")));
            writer.addDocument(first);

            Document second = new Document();
            second.add(new SortedSetDocValuesField("k", new BytesRef("b")));
            writer.addDocument(second);

            Document third = new Document();
            third.add(new SortedNumericDocValuesField("n", 30));
            writer.addDocument(third);
            writer.commit();

            try (
                DirectoryReader reader = DirectoryReader.open(writer);
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("n", ColumnKind.LONG), new InputColumn("k", ColumnKind.KEYWORD)),
                    allocator,
                    null
                )
            ) {
                try (ArrowBatchSource source = factory.open(new int[] { 0, 1 }); VectorSchemaRoot root = source.nextBatch()) {
                    assertEquals(3, root.getRowCount());
                    assertEquals(10L, root.getVector("n").getObject(0));
                    assertEquals("a", root.getVector("k").getObject(0).toString());
                    assertNull(root.getVector("n").getObject(1));
                    assertEquals("b", root.getVector("k").getObject(1).toString());
                    assertEquals(30L, root.getVector("n").getObject(2));
                    assertNull(root.getVector("k").getObject(2));
                    assertNull(source.nextBatch());
                }
                try (ArrowBatchSource source = factory.open(new int[] { 1 }); VectorSchemaRoot root = source.nextBatch()) {
                    assertEquals(1, root.getFieldVectors().size());
                    assertEquals("k", root.getVector(0).getName());
                    assertEquals(3, root.getRowCount());
                    assertNull(source.nextBatch());
                }
                IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> factory.open(new int[] { 2 }));
                assertTrue(error.getMessage(), error.getMessage().contains("outside input schema"));
                assertEquals(0L, factory.metrics().get("direct_batches").longValue());
                assertEquals(1L, factory.metrics().get("fallback_batches").longValue());
                assertEquals(2L, factory.metrics().get("batches").longValue());
                assertEquals(6L, factory.metrics().get("rows").longValue());
                assertEquals(3L, factory.metrics().get("null_values").longValue());
            }
        }
    }

    public void testTwoPhaseQueryUsesConfirmedMatches() throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)
        ) {
            Document match = new Document();
            match.add(new TextField("text", "a b", Field.Store.NO));
            match.add(new NumericDocValuesField("n", 1));
            writer.addDocument(match);

            Document approximationOnly = new Document();
            approximationOnly.add(new TextField("text", "a x b", Field.Store.NO));
            approximationOnly.add(new NumericDocValuesField("n", 2));
            writer.addDocument(approximationOnly);
            writer.commit();

            try (
                DirectoryReader reader = DirectoryReader.open(writer);
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new PhraseQuery("text", "a", "b"),
                    List.of(new InputColumn("n", ColumnKind.LONG)),
                    allocator,
                    null
                );
                ArrowBatchSource source = factory.open(new int[] { 0 });
                VectorSchemaRoot root = source.nextBatch()
            ) {
                assertEquals(1, root.getRowCount());
                assertEquals(1L, root.getVector("n").getObject(0));
                assertNull(source.nextBatch());
            }
        }
    }

    public void testFactoryRetainsReader() throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)
        ) {
            Document document = new Document();
            document.add(new NumericDocValuesField("n", 42));
            writer.addDocument(document);
            writer.commit();

            DirectoryReader reader = DirectoryReader.open(writer);
            DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                new IndexSearcher(reader),
                new MatchAllDocsQuery(),
                List.of(new InputColumn("n", ColumnKind.LONG)),
                allocator,
                null
            );
            reader.close();
            try (factory; ArrowBatchSource source = factory.open(new int[] { 0 }); VectorSchemaRoot root = source.nextBatch()) {
                assertEquals(42L, root.getVector("n").getObject(0));
            }
        }
    }

    public void testRejectsMultiValuedColumns() throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)
        ) {
            Document document = new Document();
            document.add(new SortedNumericDocValuesField("numbers", 1));
            document.add(new SortedNumericDocValuesField("numbers", 2));
            document.add(new SortedSetDocValuesField("keywords", new BytesRef("a")));
            document.add(new SortedSetDocValuesField("keywords", new BytesRef("b")));
            writer.addDocument(document);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertMultiValuedRejected(reader, allocator, new InputColumn("numbers", ColumnKind.LONG), "numeric");
                assertMultiValuedRejected(reader, allocator, new InputColumn("keywords", ColumnKind.KEYWORD), "keyword");
            }
        }
    }

    public void testCancellationClosesDecodedBatch() throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)
        ) {
            Document document = new Document();
            document.add(new NumericDocValuesField("n", 1));
            writer.addDocument(document);
            writer.commit();

            AtomicInteger checks = new AtomicInteger();
            CancellableTask task = new CancellableTask(1, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of()) {
                @Override
                public boolean isCancelled() {
                    return checks.incrementAndGet() > 1;
                }

                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return false;
                }
            };
            try (
                DirectoryReader reader = DirectoryReader.open(writer);
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("n", ColumnKind.LONG)),
                    allocator,
                    task
                );
                ArrowBatchSource source = factory.open(new int[] { 0 })
            ) {
                expectThrows(TaskCancelledException.class, source::nextBatch);
                assertEquals(0L, allocator.getAllocatedMemory());
            }
        }
    }

    private static void assertMultiValuedRejected(DirectoryReader reader, RootAllocator allocator, InputColumn column, String kind)
        throws Exception {
        try (
            DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                new IndexSearcher(reader),
                new MatchAllDocsQuery(),
                List.of(column),
                allocator,
                null
            );
            ArrowBatchSource source = factory.open(new int[] { 0 })
        ) {
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, source::nextBatch);
            assertTrue(error.getMessage(), error.getMessage().contains("multi-valued " + kind + " doc values"));
        }
    }
}
