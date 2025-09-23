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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class DerivedSourceStoredFieldsReaderTests extends OpenSearchTestCase {

    private Directory dir;
    private IndexWriter writer;
    private StoredFieldsReader delegate;
    private DerivedSourceStoredFieldsReader reader;
    private static final byte[] TEST_SOURCE = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
    private static final int TEST_DOC_ID = 0;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random()));
        writer = new IndexWriter(dir, config);

        Document doc = new Document();
        doc.add(new StoredField("_source", TEST_SOURCE));
        writer.addDocument(doc);
        writer.commit();

        DirectoryReader dirReader = DirectoryReader.open(writer);
        delegate = new StoredFieldsReader() {
            private final StoredFields storedFields = dirReader.leaves().get(0).reader().storedFields();

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                storedFields.document(docID, visitor);
            }

            @Override
            public StoredFieldsReader clone() {
                return this;
            }

            @Override
            public void checkIntegrity() throws IOException {}

            @Override
            public void close() throws IOException {
                dirReader.close();
            }
        };

        reader = new DerivedSourceStoredFieldsReader(delegate, docId -> new BytesArray(TEST_SOURCE));
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(reader, writer, dir);
        super.tearDown();
    }

    public void testClone() {
        StoredFieldsReader cloned = reader.clone();
        assertNotNull("Cloned reader should not be null", cloned);
        assertTrue(
            "Cloned reader should be instance of DerivedSourceStoredFieldsReader",
            cloned instanceof DerivedSourceStoredFieldsReader
        );
    }

    public void testCheckIntegrity() throws IOException {
        // Should not throw exception
        reader.checkIntegrity();
    }

    public void testGetMergeInstance() {
        StoredFieldsReader mergeInstance = reader.getMergeInstance();
        assertNotNull("Merge instance should not be null", mergeInstance);
    }

    public void testDocumentWithSourceFieldRequested() throws IOException {
        final AtomicBoolean sourceCalled = new AtomicBoolean(false);
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return fieldInfo.name.equals("_source") ? Status.YES : Status.NO;
            }

            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) {
                sourceCalled.set(true);
                assertEquals("Field name should be _source", "_source", fieldInfo.name);
                assertArrayEquals("Source field content should match", TEST_SOURCE, value);
            }
        };

        reader.document(TEST_DOC_ID, visitor);
        assertTrue("Source field should have been called", sourceCalled.get());
    }

    public void testDocumentWithoutSourceFieldRequested() throws IOException {
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return Status.NO;
            }

            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) {
                fail("Binary field should not be called when source is not requested");
            }
        };
        reader.document(TEST_DOC_ID, visitor);
    }

    public void testSourceProviderThrowsException() {
        reader = new DerivedSourceStoredFieldsReader(delegate, docId -> { throw new IOException("Test exception"); });
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return Status.YES;
            }
        };

        expectThrows(IOException.class, () -> reader.document(TEST_DOC_ID, visitor));
    }
}
