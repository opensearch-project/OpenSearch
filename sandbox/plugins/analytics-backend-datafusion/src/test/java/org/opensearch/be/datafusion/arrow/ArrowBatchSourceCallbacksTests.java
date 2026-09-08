/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.ColumnKind;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory.InputColumn;
import org.opensearch.be.lucene.DocValuesBatchSourceFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class ArrowBatchSourceCallbacksTests extends OpenSearchTestCase {

    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testProjectionBatchExportEofAndRelease() throws Exception {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document first = new Document();
            first.add(new NumericDocValuesField("number", 11L));
            first.add(new SortedDocValuesField("keyword", new BytesRef("a")));
            writer.addDocument(first);
            Document second = new Document();
            second.add(new NumericDocValuesField("number", 22L));
            second.add(new SortedDocValuesField("keyword", new BytesRef("b")));
            writer.addDocument(second);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = factory(
                    reader,
                    List.of(new InputColumn("number", ColumnKind.LONG), new InputColumn("keyword", ColumnKind.KEYWORD))
                );
                try (
                    ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
                    Arena arena = Arena.ofConfined();
                    ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)
                ) {
                    MemorySegment error = arena.allocate(256L);
                    int sourceKey = ArrowBatchSourceCallbacks.createSource(
                        registration.bindingId(),
                        MemorySegment.ofArray(new int[] { 1, 0 }),
                        2L,
                        error,
                        error.byteSize()
                    );
                    assertTrue(sourceKey > 0);
                    assertEquals(
                        2L,
                        ArrowBatchSourceCallbacks.nextBatch(
                            registration.bindingId(),
                            sourceKey,
                            MemorySegment.ofAddress(array.memoryAddress()),
                            MemorySegment.ofAddress(cSchema.memoryAddress()),
                            error,
                            error.byteSize()
                        )
                    );
                    try (CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
                        Schema schema = Data.importSchema(allocator, cSchema, dictionaries);
                        try (VectorSchemaRoot imported = VectorSchemaRoot.create(schema, allocator)) {
                            Data.importIntoVectorSchemaRoot(allocator, array, imported, dictionaries);
                            assertEquals(
                                List.of("keyword", "number"),
                                imported.getSchema().getFields().stream().map(f -> f.getName()).toList()
                            );
                            assertEquals("a", imported.getVector("keyword").getObject(0).toString());
                            assertEquals("b", imported.getVector("keyword").getObject(1).toString());
                            assertEquals(11L, imported.getVector("number").getObject(0));
                            assertEquals(22L, imported.getVector("number").getObject(1));
                        }
                    }
                    assertEquals(
                        0L,
                        ArrowBatchSourceCallbacks.nextBatch(
                            registration.bindingId(),
                            sourceKey,
                            MemorySegment.NULL,
                            MemorySegment.NULL,
                            error,
                            error.byteSize()
                        )
                    );
                    ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), sourceKey);
                }
                assertEquals(initialRefCount, reader.getRefCount());
                expectThrows(IllegalStateException.class, () -> factory.open(new int[] { 0 }));
            }
        }
    }

    public void testInvalidProjectionReturnsTerminatedError() throws Exception {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = factory(reader, List.of(new InputColumn("number", ColumnKind.LONG)));
                try (
                    ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
                    Arena arena = Arena.ofConfined()
                ) {
                    MemorySegment error = arena.allocate(32L);
                    error.fill((byte) 'x');
                    assertEquals(
                        -1,
                        ArrowBatchSourceCallbacks.createSource(
                            registration.bindingId(),
                            MemorySegment.ofArray(new int[] { 1 }),
                            1L,
                            error,
                            error.byteSize()
                        )
                    );
                    assertEquals(0, error.get(ValueLayout.JAVA_BYTE, error.byteSize() - 1L));
                    String message = readCString(error, error.byteSize());
                    assertTrue(message, message.contains("IllegalArgument"));
                }
                assertEquals(initialRefCount, reader.getRefCount());
            }
        }
    }

    public void testMultiValuedSourceErrorIsReturned() throws Exception {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document document = new Document();
            document.add(new SortedNumericDocValuesField("number", 1L));
            document.add(new SortedNumericDocValuesField("number", 2L));
            writer.addDocument(document);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = factory(reader, List.of(new InputColumn("number", ColumnKind.LONG)));
                try (
                    ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
                    Arena arena = Arena.ofConfined()
                ) {
                    MemorySegment error = arena.allocate(256L);
                    int sourceKey = ArrowBatchSourceCallbacks.createSource(
                        registration.bindingId(),
                        MemorySegment.ofArray(new int[] { 0 }),
                        1L,
                        error,
                        error.byteSize()
                    );
                    assertTrue(sourceKey > 0);
                    assertEquals(
                        ArrowBatchSourceCallbacks.ERROR,
                        ArrowBatchSourceCallbacks.nextBatch(
                            registration.bindingId(),
                            sourceKey,
                            MemorySegment.NULL,
                            MemorySegment.NULL,
                            error,
                            error.byteSize()
                        )
                    );
                    assertTrue(readCString(error, error.byteSize()).contains("multi-valued numeric doc values"));
                    ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), sourceKey);
                }
                assertEquals(initialRefCount, reader.getRefCount());
            }
        }
    }

    public void testCancellationReturnsCancelledStatus() throws Exception {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            Document document = new Document();
            document.add(new NumericDocValuesField("number", 1L));
            writer.addDocument(document);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = factory(reader, List.of(new InputColumn("number", ColumnKind.LONG)));
                try (
                    ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
                    Arena arena = Arena.ofConfined()
                ) {
                    MemorySegment error = arena.allocate(128L);
                    int sourceKey = ArrowBatchSourceCallbacks.createSource(
                        registration.bindingId(),
                        MemorySegment.ofArray(new int[] { 0 }),
                        1L,
                        error,
                        error.byteSize()
                    );
                    assertTrue(sourceKey > 0);
                    ArrowBatchSourceCallbacks.cancelSource(registration.bindingId(), sourceKey);
                    assertEquals(
                        ArrowBatchSourceCallbacks.CANCELLED,
                        ArrowBatchSourceCallbacks.nextBatch(
                            registration.bindingId(),
                            sourceKey,
                            MemorySegment.NULL,
                            MemorySegment.NULL,
                            error,
                            error.byteSize()
                        )
                    );
                    ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), sourceKey);
                }
                assertEquals(initialRefCount, reader.getRefCount());
            }
        }
    }

    public void testRegistrationCloseRejectsNewPullAndIsIdempotent() throws Exception {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(writer); Arena arena = Arena.ofConfined()) {
                int initialRefCount = reader.getRefCount();
                DocValuesBatchSourceFactory factory = factory(reader, List.of(new InputColumn("number", ColumnKind.LONG)));
                ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
                MemorySegment error = arena.allocate(128L);
                int sourceKey = ArrowBatchSourceCallbacks.createSource(
                    registration.bindingId(),
                    MemorySegment.ofArray(new int[] { 0 }),
                    1L,
                    error,
                    error.byteSize()
                );
                assertTrue(sourceKey > 0);

                registration.close();
                registration.close();

                assertEquals(
                    ArrowBatchSourceCallbacks.ERROR,
                    ArrowBatchSourceCallbacks.nextBatch(
                        registration.bindingId(),
                        sourceKey,
                        MemorySegment.NULL,
                        MemorySegment.NULL,
                        error,
                        error.byteSize()
                    )
                );
                assertTrue(readCString(error, error.byteSize()).contains("binding is closed"));
                ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), sourceKey);
                assertEquals(initialRefCount, reader.getRefCount());
                expectThrows(IllegalStateException.class, () -> factory.open(new int[] { 0 }));
            }
        }
    }

    private DocValuesBatchSourceFactory factory(DirectoryReader reader, List<InputColumn> columns) throws Exception {
        return new DocValuesBatchSourceFactory(new IndexSearcher(reader), new MatchAllDocsQuery(), columns, allocator, null);
    }

    private static String readCString(MemorySegment segment, long capacity) {
        byte[] bytes = segment.reinterpret(capacity).toArray(ValueLayout.JAVA_BYTE);
        int length = 0;
        while (length < bytes.length && bytes[length] != 0) {
            length++;
        }
        return new String(Arrays.copyOf(bytes, length), StandardCharsets.UTF_8);
    }
}
