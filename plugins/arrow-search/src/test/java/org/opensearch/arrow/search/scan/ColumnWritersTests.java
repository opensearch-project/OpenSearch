/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.Column.ColumnType;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ColumnWritersTests extends OpenSearchTestCase {

    private static final int DOCS = 100;

    public void testGrowingKeywordRowsLeavesTheirDataBufferAlone() {
        try (BufferAllocator allocator = new RootAllocator(); VarCharVector vector = new VarCharVector("k", allocator)) {
            // Presized for a few large values, like a batch that follows rows of large keywords.
            vector.setInitialCapacity(16, 1024);
            vector.allocateNew();
            long data = vector.getDataBuffer().capacity();
            ColumnWriters.ensureCapacity(vector, 4096);
            assertTrue(vector.getValueCapacity() >= 4096);
            assertEquals("rows need offsets and validity, not data", data, vector.getDataBuffer().capacity());
        }
    }

    public void testKeywordValuesStopAtTheAllowance() throws IOException {
        Column column = new Column("k", ColumnType.UTF8, Set.of("keyword"));
        // Distinct values of 1KB, so a pass of 100 docs would copy and write 100KB.
        withSegment(column.name(), doc -> List.of(value(doc, 1024)), leaf -> {
            ColumnWriters.Scratch scratch = new ColumnWriters.Scratch();
            ColumnWriters.LeafColumnWriter writer = open(column, new KeywordFieldMapper.KeywordFieldType(column.name()), leaf, scratch);
            try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = root(column, allocator)) {
                ColumnWriters.Allowance allowance = new ColumnWriters.Allowance();
                allowance.reset(10 * 1024, DOCS);
                int written = writer.write(docs(), DOCS, root.getVector(0), 0, allowance);
                assertEquals("the doc that overdraws the allowance is the last written", 11, written);
                ColumnWriters.setRowCount(root, written);
                for (int doc = 0; doc < written; doc++) {
                    assertEquals(value(doc, 1024), new BytesRef(((VarCharVector) root.getVector(0)).get(doc)));
                }
                assertThat(((VarCharVector) root.getVector(0)).getDataBuffer().capacity(), lessThan(32L * 1024));
                assertThat("values copied to the heap follow the allowance", scratch.bytes.length, lessThan(16 * 1024));
            }
        });
    }

    public void testEachColumnStopsAtTheRowWhoseValuesOverdrawTheAllowance() throws IOException {
        Column column = new Column("k", ColumnType.UTF8, Set.of("keyword"));
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(column.name());
        // Two columns of 1KB values, then one of 16KB values.
        withSegment(column.name(), doc -> List.of(value(doc, 1024)), small -> {
            withSegment(column.name(), doc -> List.of(value(doc, 16 * 1024)), large -> {
                ColumnWriters.Scratch scratch = new ColumnWriters.Scratch();
                try (
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot first = root(column, allocator);
                    VectorSchemaRoot second = root(column, allocator);
                    VectorSchemaRoot third = root(column, allocator)
                ) {
                    ColumnWriters.Allowance allowance = new ColumnWriters.Allowance();
                    allowance.reset(10 * 1024 + 512, DOCS);
                    int written = open(column, fieldType, small, scratch).write(docs(), DOCS, first.getVector(0), 0, allowance);
                    assertEquals(11, written);
                    allowance.nextColumn(written);
                    written = open(column, fieldType, small, scratch).write(docs(), written, second.getVector(0), 0, allowance);
                    assertEquals("rows of 2KB overdraw the allowance at the sixth", 6, written);
                    allowance.nextColumn(written);
                    written = open(column, fieldType, large, scratch).write(docs(), written, third.getVector(0), 0, allowance);
                    assertEquals(1, written);
                    assertThat(((VarCharVector) third.getVector(0)).getDataBuffer().capacity(), lessThan(64L * 1024));
                    assertThat("values copied to the heap follow the allowance", scratch.bytes.length, lessThan(32 * 1024));
                }
            });
        });
    }

    public void testALargeValueCountsTowardsItsOwnRowOnly() throws IOException {
        Column column = new Column("k", ColumnType.UTF8, Set.of("keyword"));
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType(column.name());
        // A column of 100-byte values except for one 8KB value, then a column of 1KB values.
        withSegment(column.name(), doc -> List.of(value(doc, doc == 3 ? 8192 : 100)), first -> {
            withSegment(column.name(), doc -> List.of(value(doc, 1024)), second -> {
                ColumnWriters.Scratch scratch = new ColumnWriters.Scratch();
                try (
                    BufferAllocator allocator = new RootAllocator();
                    VectorSchemaRoot a = root(column, allocator);
                    VectorSchemaRoot b = root(column, allocator)
                ) {
                    ColumnWriters.Allowance allowance = new ColumnWriters.Allowance();
                    allowance.reset(10 * 1024, DOCS);
                    int written = open(column, fieldType, first, scratch).write(docs(), DOCS, a.getVector(0), 0, allowance);
                    assertEquals(22, written);
                    allowance.nextColumn(written);
                    written = open(column, fieldType, second, scratch).write(docs(), written, b.getVector(0), 0, allowance);
                    assertEquals("the fourth row, with the large value, overdraws the allowance", 4, written);
                }
            });
        });
    }

    public void testKeywordListsStopAtTheAllowance() throws IOException {
        Column column = new Column("k", ColumnType.UTF8, Set.of("keyword"), true);
        // 50 distinct values of 100 bytes per doc: about 5KB per row.
        withSegment(column.name(), doc -> {
            List<BytesRef> values = new ArrayList<>();
            for (int v = 0; v < 50; v++) {
                values.add(value(doc * 50 + v, 100));
            }
            return values;
        }, leaf -> {
            ColumnWriters.Scratch scratch = new ColumnWriters.Scratch();
            ColumnWriters.LeafColumnWriter writer = open(column, new KeywordFieldMapper.KeywordFieldType(column.name()), leaf, scratch);
            try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = root(column, allocator)) {
                ColumnWriters.Allowance allowance = new ColumnWriters.Allowance();
                allowance.reset(8 * 1024, DOCS);
                int written = writer.write(docs(), DOCS, root.getVector(0), 0, allowance);
                assertEquals(2, written);
                ColumnWriters.setRowCount(root, written);
                ListVector list = (ListVector) root.getVector(0);
                assertEquals(100, list.getDataVector().getValueCount());
                assertEquals(value(99, 100), new BytesRef(((VarCharVector) list.getDataVector()).get(99)));
                assertThat("values copied to the heap follow the allowance", scratch.bytes.length, lessThan(16 * 1024));
            }
        });
    }

    public void testIpValuesThatAreNotAddressesFailTheScanAsBadData() throws IOException {
        Column column = new Column("client_ip", ColumnType.IP, Set.of("ip"));
        // Field masking replaces the values of any field, including an ip field, with a hash.
        withSegment(column.name(), doc -> List.of(new BytesRef("4f8e2bd6c1a07e3355b26d1f0e9c7a48" + doc)), leaf -> {
            ColumnWriters.LeafColumnWriter writer = open(
                column,
                new IpFieldMapper.IpFieldType(column.name()),
                leaf,
                new ColumnWriters.Scratch()
            );
            try (BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot root = root(column, allocator)) {
                ColumnWriters.Allowance allowance = new ColumnWriters.Allowance();
                allowance.reset(1 << 20, DOCS);
                ArrowScanDataException e = expectThrows(
                    ArrowScanDataException.class,
                    () -> writer.write(docs(), DOCS, root.getVector(0), 0, allowance)
                );
                assertThat(e.getMessage(), containsString("field [client_ip] has values on this shard that are not ip addresses"));
            }
        });
    }

    private static BytesRef value(int n, int length) {
        StringBuilder value = new StringBuilder(String.format(Locale.ROOT, "%08d", n));
        while (value.length() < length) {
            value.append('x');
        }
        return new BytesRef(value);
    }

    private static int[] docs() {
        int[] docs = new int[DOCS];
        for (int doc = 0; doc < DOCS; doc++) {
            docs[doc] = doc;
        }
        return docs;
    }

    @FunctionalInterface
    private interface LeafTest {
        void run(LeafReaderContext leaf) throws IOException;
    }

    /** Runs {@code test} on one segment of {@link #DOCS} docs, each with the given sorted set doc values of {@code field}. */
    private static void withSegment(String field, IntFunction<List<BytesRef>> values, LeafTest test) throws IOException {
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (int doc = 0; doc < DOCS; doc++) {
                    Document document = new Document();
                    for (BytesRef value : values.apply(doc)) {
                        document.add(new SortedSetDocValuesField(field, value));
                    }
                    writer.addDocument(document);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                test.run(reader.leaves().get(0));
            }
        }
    }

    private static ColumnWriters.LeafColumnWriter open(
        Column column,
        MappedFieldType fieldType,
        LeafReaderContext leaf,
        ColumnWriters.Scratch scratch
    ) throws IOException {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.fieldType(column.name())).thenReturn(fieldType);
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getMapperService()).thenReturn(mapperService);
        return ColumnWriters.open(column, leaf, context, true, scratch);
    }

    private static VectorSchemaRoot root(Column column, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(Column.toSchema(List.of(column)), allocator);
        root.getVector(0).setInitialCapacity(16);
        root.allocateNew();
        return root;
    }
}
