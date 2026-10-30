/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
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
import org.apache.lucene.util.NumericUtils;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.be.lucene.DocValuesBatchSourceFactory;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class LuceneDocValuesBatchSourceTests extends OpenSearchTestCase {

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
            }
        }
    }

    public void testScalarFieldTypes() throws Exception {
        byte[] binary = new byte[] { 0, 1, (byte) 0xFF };
        byte[] ip = InetAddressPoint.encode(InetAddress.getByName("192.0.2.1"));
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document first = new Document();
            first.add(new NumericDocValuesField("boolean", 1L));
            first.add(new NumericDocValuesField("float", NumericUtils.floatToSortableInt(1.25F)));
            first.add(new NumericDocValuesField("double", NumericUtils.doubleToSortableLong(-2.5D)));
            first.add(new BinaryDocValuesField("binary", new BytesRef(binary)));
            first.add(new SortedSetDocValuesField("ip", new BytesRef(ip)));
            writer.addDocument(first);
            writer.addDocument(new Document());
            writer.commit();

            List<InputColumn> columns = List.of(
                new InputColumn("boolean", ColumnKind.BOOLEAN),
                new InputColumn("float", ColumnKind.FLOAT),
                new InputColumn("double", ColumnKind.DOUBLE),
                new InputColumn("binary", ColumnKind.BINARY),
                new InputColumn("ip", ColumnKind.IP)
            );
            try (
                DirectoryReader reader = DirectoryReader.open(writer);
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    columns,
                    allocator,
                    null
                );
                ArrowBatchSource source = factory.open(new int[] { 0, 1, 2, 3, 4 });
                VectorSchemaRoot root = source.nextBatch()
            ) {
                assertEquals(2, root.getRowCount());
                assertEquals(true, root.getVector("boolean").getObject(0));
                assertEquals(1.25F, (Float) root.getVector("float").getObject(0), 0F);
                assertEquals(-2.5D, (Double) root.getVector("double").getObject(0), 0D);
                assertArrayEquals(binary, (byte[]) root.getVector("binary").getObject(0));
                assertArrayEquals(ip, (byte[]) root.getVector("ip").getObject(0));
                for (InputColumn column : columns) {
                    assertNull(root.getVector(column.name()).getObject(1));
                }
                assertNull(source.nextBatch());
            }
        }
    }

    public void testMultiValuedFieldTypes() throws Exception {
        byte[] firstIp = InetAddressPoint.encode(InetAddress.getByName("192.0.2.1"));
        byte[] secondIp = InetAddressPoint.encode(InetAddress.getByName("2001:db8::1"));
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document first = new Document();
            first.add(new SortedNumericDocValuesField("longs", 1L));
            first.add(new SortedNumericDocValuesField("longs", 2L));
            first.add(new SortedNumericDocValuesField("timestamps", 1_000L));
            first.add(new SortedNumericDocValuesField("timestamps", 2_000L));
            first.add(new SortedNumericDocValuesField("booleans", 0L));
            first.add(new SortedNumericDocValuesField("booleans", 1L));
            first.add(new SortedNumericDocValuesField("floats", NumericUtils.floatToSortableInt(-1.5F)));
            first.add(new SortedNumericDocValuesField("floats", NumericUtils.floatToSortableInt(2.25F)));
            first.add(new SortedNumericDocValuesField("doubles", NumericUtils.doubleToSortableLong(-3.5D)));
            first.add(new SortedNumericDocValuesField("doubles", NumericUtils.doubleToSortableLong(4.75D)));
            first.add(new SortedSetDocValuesField("keywords", new BytesRef("a")));
            first.add(new SortedSetDocValuesField("keywords", new BytesRef("b")));
            first.add(new SortedSetDocValuesField("ips", new BytesRef(firstIp)));
            first.add(new SortedSetDocValuesField("ips", new BytesRef(secondIp)));
            writer.addDocument(first);
            writer.addDocument(new Document());
            writer.commit();

            List<InputColumn> columns = List.of(
                new InputColumn("longs", ColumnKind.LONG, true),
                new InputColumn("timestamps", ColumnKind.TIMESTAMP, true),
                new InputColumn("booleans", ColumnKind.BOOLEAN, true),
                new InputColumn("floats", ColumnKind.FLOAT, true),
                new InputColumn("doubles", ColumnKind.DOUBLE, true),
                new InputColumn("keywords", ColumnKind.KEYWORD, true),
                new InputColumn("ips", ColumnKind.IP, true)
            );
            try (
                DirectoryReader reader = DirectoryReader.open(writer);
                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    columns,
                    allocator,
                    null
                );
                ArrowBatchSource source = factory.open(new int[] { 0, 1, 2, 3, 4, 5, 6 });
                VectorSchemaRoot root = source.nextBatch()
            ) {
                ListVector longs = (ListVector) root.getVector("longs");
                int longStart = longs.getElementStartIndex(0);
                assertEquals(longStart + 2, longs.getElementEndIndex(0));
                assertEquals(1L, ((BigIntVector) longs.getDataVector()).get(longStart));
                assertEquals(2L, ((BigIntVector) longs.getDataVector()).get(longStart + 1));

                ListVector timestamps = (ListVector) root.getVector("timestamps");
                int timestampStart = timestamps.getElementStartIndex(0);
                assertEquals(1_000L, ((TimeStampMilliVector) timestamps.getDataVector()).get(timestampStart));
                assertEquals(2_000L, ((TimeStampMilliVector) timestamps.getDataVector()).get(timestampStart + 1));

                ListVector booleans = (ListVector) root.getVector("booleans");
                int booleanStart = booleans.getElementStartIndex(0);
                assertEquals(0, ((BitVector) booleans.getDataVector()).get(booleanStart));
                assertEquals(1, ((BitVector) booleans.getDataVector()).get(booleanStart + 1));

                ListVector floats = (ListVector) root.getVector("floats");
                int floatStart = floats.getElementStartIndex(0);
                assertEquals(-1.5F, ((Float4Vector) floats.getDataVector()).get(floatStart), 0F);
                assertEquals(2.25F, ((Float4Vector) floats.getDataVector()).get(floatStart + 1), 0F);

                ListVector doubles = (ListVector) root.getVector("doubles");
                int doubleStart = doubles.getElementStartIndex(0);
                assertEquals(-3.5D, ((Float8Vector) doubles.getDataVector()).get(doubleStart), 0D);
                assertEquals(4.75D, ((Float8Vector) doubles.getDataVector()).get(doubleStart + 1), 0D);

                ListVector keywords = (ListVector) root.getVector("keywords");
                int keywordStart = keywords.getElementStartIndex(0);
                ViewVarCharVector keywordValues = (ViewVarCharVector) keywords.getDataVector();
                assertEquals("a", keywordValues.getObject(keywordStart).toString());
                assertEquals("b", keywordValues.getObject(keywordStart + 1).toString());

                ListVector ips = (ListVector) root.getVector("ips");
                int ipStart = ips.getElementStartIndex(0);
                VarBinaryVector ipValues = (VarBinaryVector) ips.getDataVector();
                assertArrayEquals(firstIp, ipValues.get(ipStart));
                assertArrayEquals(secondIp, ipValues.get(ipStart + 1));

                for (InputColumn column : columns) {
                    assertTrue(((ListVector) root.getVector(column.name())).isNull(1));
                }
                assertNull(source.nextBatch());
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

    public void testRejectsMultiValuedColumnsDeclaredScalar() throws Exception {
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

    public void testCancelledTaskDoesNotAllocateBatch() throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)
        ) {
            Document document = new Document();
            document.add(new NumericDocValuesField("n", 1));
            writer.addDocument(document);
            writer.commit();

            AnalyticsShardTask task = new AnalyticsShardTask(1, "test", "test", "test", TaskId.EMPTY_TASK_ID, Map.of());
            task.cancel("test cancellation");
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

    public void testCooperativeCancellation() throws Exception {
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)
        ) {
            Document document = new Document();
            document.add(new NumericDocValuesField("n", 1));
            writer.addDocument(document);
            writer.commit();

            try (
                DirectoryReader reader = DirectoryReader.open(writer);
                DocValuesBatchSourceFactory factory = new DocValuesBatchSourceFactory(
                    new IndexSearcher(reader),
                    new MatchAllDocsQuery(),
                    List.of(new InputColumn("n", ColumnKind.LONG)),
                    allocator,
                    null
                );
                ArrowBatchSource source = factory.open(new int[] { 0 })
            ) {
                source.cancel();
                expectThrows(TaskCancelledException.class, source::nextBatch);
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
