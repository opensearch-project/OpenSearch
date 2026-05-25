/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DerivedSourceDirectoryReaderTests extends OpenSearchTestCase {

    private Directory dir;
    private IndexWriter writer;
    private DirectoryReader directoryReader;
    private DerivedSourceDirectoryReader reader;
    private static final byte[] TEST_SOURCE = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(random(), null);
        writer = new IndexWriter(dir, config);

        Document doc = new Document();
        doc.add(new StoredField("_source", TEST_SOURCE));
        writer.addDocument(doc);
        writer.commit();

        directoryReader = DirectoryReader.open(writer);
        reader = DerivedSourceDirectoryReader.wrap(directoryReader, leafReaderContext -> docId -> new BytesArray(TEST_SOURCE));
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            IOUtils.close(reader, directoryReader, writer, dir);
        } finally {
            super.tearDown();
        }
    }

    public void testWrap() throws IOException {
        assertNotNull("Wrapped reader should not be null", reader);
        List<LeafReaderContext> leaves = reader.leaves();
        assertFalse("Should have at least one leaf", leaves.isEmpty());
        assertTrue("Leaf should be DerivedSourceLeafReader", leaves.get(0).reader() instanceof DerivedSourceLeafReader);
    }

    public void testDoWrapDirectoryReader() throws IOException {
        DirectoryReader wrapped = reader.doWrapDirectoryReader(directoryReader);
        assertNotNull("Wrapped reader should not be null", wrapped);
        assertTrue("Should be DerivedSourceDirectoryReader", wrapped instanceof DerivedSourceDirectoryReader);
    }

    public void testGetReaderCacheHelper() {
        assertEquals("Cache helper should match input reader", directoryReader.getReaderCacheHelper(), reader.getReaderCacheHelper());
    }

    public void testSourceProviderCalls() throws IOException {
        AtomicInteger sourceProviderCalls = new AtomicInteger(0);
        Map<String, Integer> leafCalls = new HashMap<>();

        DerivedSourceDirectoryReader.LeafSourceProviderFactory countingSourceProvider = leafReaderContext -> {
            String leafKey = leafReaderContext.reader().toString();
            return docId -> {
                sourceProviderCalls.incrementAndGet();
                leafCalls.merge(leafKey, 1, Integer::sum);
                return new BytesArray(TEST_SOURCE);
            };
        };

        DerivedSourceDirectoryReader countingReader = DerivedSourceDirectoryReader.wrap(directoryReader, countingSourceProvider);

        // Access stored fields for all documents in all leaves
        for (LeafReaderContext context : countingReader.leaves()) {
            StoredFields storedFields = context.reader().storedFields();
            for (int i = 0; i < context.reader().maxDoc(); i++) {
                storedFields.document(i, new StoredFieldVisitor() {
                    @Override
                    public Status needsField(FieldInfo fieldInfo) {
                        return fieldInfo.name.equals("_source") ? Status.YES : Status.NO;
                    }
                });
            }
        }

        assertTrue("Source provider should be called", sourceProviderCalls.get() > 0);
        assertFalse("Should have leaf calls recorded", leafCalls.isEmpty());
    }

    public void testSourceProviderFactoryReceivesOriginalLeafContextsInOrder() throws IOException {
        Directory multiDir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(random(), null).setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter multiWriter = new IndexWriter(multiDir, config);

        for (int i = 0; i < 3; i++) {
            Document doc = new Document();
            doc.add(new StoredField("_source", TEST_SOURCE));
            multiWriter.addDocument(doc);
            multiWriter.flush();
        }

        DirectoryReader multiReader = null;
        DerivedSourceDirectoryReader derivedReader = null;
        try {
            multiReader = DirectoryReader.open(multiWriter);
            assertTrue("Should have multiple segments", multiReader.leaves().size() > 1);

            List<LeafReaderContext> receivedContexts = new ArrayList<>();
            derivedReader = DerivedSourceDirectoryReader.wrap(multiReader, leafReaderContext -> {
                receivedContexts.add(leafReaderContext);
                return docId -> new BytesArray(TEST_SOURCE);
            });

            List<LeafReaderContext> originalContexts = multiReader.leaves();
            assertEquals(originalContexts.size(), receivedContexts.size());
            for (int i = 0; i < originalContexts.size(); i++) {
                assertSame(
                    "Source provider should receive original leaf context in order",
                    originalContexts.get(i),
                    receivedContexts.get(i)
                );
            }
        } finally {
            IOUtils.close(derivedReader, multiReader, multiWriter, multiDir);
        }
    }

    public void testWithMultipleSegments() throws IOException {
        // Create index with multiple segments
        Directory multiDir = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig(random(), null).setMaxBufferedDocs(2) // Force multiple segments
            .setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter multiWriter = new IndexWriter(multiDir, config);

        int numDocs = randomIntBetween(5, 20);
        Map<Integer, byte[]> docIdToSource = new HashMap<>();

        // Add documents in multiple segments
        for (int i = 0; i < numDocs; i++) {
            byte[] source = randomByteArrayOfLength(randomIntBetween(10, 100));
            docIdToSource.put(i, source);
            Document doc = new Document();
            doc.add(new StoredField("_source", source));
            multiWriter.addDocument(doc);
            if (rarely()) {
                multiWriter.commit(); // Force new segment
            }
        }
        multiWriter.commit();

        DirectoryReader multiReader = DirectoryReader.open(multiWriter);
        assertTrue("Should have multiple segments", multiReader.leaves().size() > 1);

        DerivedSourceDirectoryReader derivedReader = DerivedSourceDirectoryReader.wrap(
            multiReader,
            leafReaderContext -> docId -> new BytesArray(docIdToSource.get(leafReaderContext.docBase + docId))
        );

        int processedDocs = 0;
        // Verify all documents across all segments
        for (LeafReaderContext context : derivedReader.leaves()) {
            StoredFields storedFields = context.reader().storedFields();
            for (int i = 0; i < context.reader().maxDoc(); i++) {
                final int globalDocId = context.docBase + i;
                final int localDocId = i;
                StoredFieldVisitor visitor = new StoredFieldVisitor() {
                    @Override
                    public Status needsField(FieldInfo fieldInfo) {
                        return fieldInfo.name.equals("_source") ? Status.YES : Status.NO;
                    }

                    @Override
                    public void binaryField(FieldInfo fieldInfo, byte[] value) {
                        assertArrayEquals("Source content should match for doc " + globalDocId, docIdToSource.get(globalDocId), value);
                    }
                };
                storedFields.document(localDocId, visitor);
                processedDocs++;
            }
        }

        assertEquals("Should have processed all documents", numDocs, processedDocs);
        IOUtils.close(derivedReader, multiReader, multiWriter, multiDir);
    }

}
