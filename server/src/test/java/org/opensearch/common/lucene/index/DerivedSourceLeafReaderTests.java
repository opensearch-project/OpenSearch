/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomCodec;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DerivedSourceLeafReaderTests extends OpenSearchTestCase {

    private Directory dir;
    private IndexWriter writer;
    private DirectoryReader directoryReader;
    private LeafReader leafReader;
    private DerivedSourceLeafReader reader;
    private static final byte[] TEST_SOURCE = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
    private final CheckedFunction<Integer, BytesReference, IOException> sourceProvider = docId -> new BytesArray(TEST_SOURCE);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(random(), null).setCodec(new RandomCodec(random()));
        writer = new IndexWriter(dir, config);

        Document doc = new Document();
        doc.add(new StoredField("_source", TEST_SOURCE));
        writer.addDocument(doc);
        writer.commit();

        directoryReader = DirectoryReader.open(writer);
        leafReader = directoryReader.leaves().get(0).reader();
        reader = new DerivedSourceLeafReader(leafReader, sourceProvider);
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(directoryReader, writer, dir);
        super.tearDown();
    }

    public void testStoredFields() throws IOException {
        StoredFields storedFields = reader.storedFields();
        assertNotNull("StoredFields should not be null", storedFields);
        assertTrue(
            "StoredFields should be DerivedSourceStoredFields",
            storedFields instanceof DerivedSourceStoredFieldsReader.DerivedSourceStoredFields
        );
    }

    public void testGetSequentialStoredFieldsReaderWithCodecReader() throws IOException {
        assumeTrue("Test requires CodecReader", leafReader instanceof CodecReader);

        StoredFieldsReader sequentialReader = reader.getSequentialStoredFieldsReader();
        assertNotNull("Sequential reader should not be null", sequentialReader);
        assertTrue(
            "Sequential reader should be DerivedSourceStoredFieldsReader",
            sequentialReader instanceof DerivedSourceStoredFieldsReader
        );
    }

    public void testGetSequentialStoredFieldsReaderWithSequentialReader() throws IOException {
        // Create a wrapped SequentialStoredFieldsLeafReader
        LeafReader sequentialLeafReader = new SequentialStoredFieldsLeafReader(leafReader) {
            @Override
            protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                return reader;
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }

            @Override
            public StoredFieldsReader getSequentialStoredFieldsReader() throws IOException {
                return ((CodecReader) in).getFieldsReader();
            }
        };

        DerivedSourceLeafReader sequentialDerivedReader = new DerivedSourceLeafReader(sequentialLeafReader, sourceProvider);
        StoredFieldsReader sequentialReader = sequentialDerivedReader.getSequentialStoredFieldsReader();

        assertNotNull("Sequential reader should not be null", sequentialReader);
        assertTrue(
            "Sequential reader should be DerivedSourceStoredFieldsReader",
            sequentialReader instanceof DerivedSourceStoredFieldsReader
        );
    }

    public void testGetSequentialStoredFieldsReaderWithInvalidReader() {
        LeafReader invalidReader = new FilterLeafReader(leafReader) {
            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };

        DerivedSourceLeafReader invalidDerivedReader = new DerivedSourceLeafReader(invalidReader, sourceProvider);

        expectThrows(IOException.class, invalidDerivedReader::getSequentialStoredFieldsReader);
    }

    public void testGetCoreAndReaderCacheHelper() {
        assertEquals("Core cache helper should match input reader", leafReader.getCoreCacheHelper(), reader.getCoreCacheHelper());
        assertEquals("Reader cache helper should match input reader", leafReader.getReaderCacheHelper(), reader.getReaderCacheHelper());
    }

    public void testWithRandomDocuments() throws IOException {
        Directory randomDir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(random(), null).setCodec(new RandomCodec(random()))
            .setMergePolicy(NoMergePolicy.INSTANCE); // Prevent automatic merges

        IndexWriter randomWriter = new IndexWriter(randomDir, config);

        int numDocs = randomIntBetween(1, 10);
        Map<Integer, byte[]> docIdToSource = new HashMap<>();

        for (int i = 0; i < numDocs; i++) {
            byte[] source = randomByteArrayOfLength(randomIntBetween(10, 50));
            docIdToSource.put(i, source);
            Document doc = new Document();
            doc.add(new StoredField("_source", source));
            randomWriter.addDocument(doc);
        }

        // Force merge into a single segment
        randomWriter.forceMerge(1);
        randomWriter.commit();

        DirectoryReader randomDirectoryReader = DirectoryReader.open(randomWriter);
        assertEquals("Should have exactly one segment", 1, randomDirectoryReader.leaves().size());

        LeafReader randomLeafReader = randomDirectoryReader.leaves().get(0).reader();
        DerivedSourceLeafReader randomDerivedReader = new DerivedSourceLeafReader(
            randomLeafReader,
            docId -> new BytesArray(docIdToSource.get(docId))
        );

        StoredFields storedFields = randomDerivedReader.storedFields();
        for (int docId = 0; docId < numDocs; docId++) {
            final int currentDocId = docId;
            StoredFieldVisitor visitor = new StoredFieldVisitor() {
                @Override
                public Status needsField(FieldInfo fieldInfo) {
                    return fieldInfo.name.equals("_source") ? Status.YES : Status.NO;
                }

                @Override
                public void binaryField(FieldInfo fieldInfo, byte[] value) {
                    assertArrayEquals("Source content should match for doc " + currentDocId, docIdToSource.get(currentDocId), value);
                }
            };
            storedFields.document(docId, visitor);
        }

        IOUtils.close(randomDirectoryReader, randomWriter, randomDir);
    }
}
