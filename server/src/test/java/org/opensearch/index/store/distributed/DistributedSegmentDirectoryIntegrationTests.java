/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedSegmentDirectoryIntegrationTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory baseDirectory;
    private DistributedSegmentDirectory distributedDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        baseDirectory = FSDirectory.open(tempDir);
        distributedDirectory = new DistributedSegmentDirectory(baseDirectory, tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        if (distributedDirectory != null) {
            distributedDirectory.close();
        }
        super.tearDown();
    }

    public void testIndexWriterWithDistributedDirectory() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            // Add some documents
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "This is document " + i, Field.Store.YES));
                writer.addDocument(doc);
            }
            
            writer.commit();
        }
        
        // Verify files were created and distributed
        String[] files = distributedDirectory.listAll();
        assertTrue("Should have created segment files", files.length > 0);
        
        // Check that files are distributed across directories
        boolean hasSegmentsFile = false;
        boolean hasDistributedFiles = false;
        
        for (String file : files) {
            if (file.startsWith("segments_")) {
                hasSegmentsFile = true;
                assertEquals("segments_N should be in directory 0", 0, distributedDirectory.getDirectoryIndex(file));
            } else {
                hasDistributedFiles = true;
            }
        }
        
        assertTrue("Should have segments file", hasSegmentsFile);
        assertTrue("Should have other distributed files", hasDistributedFiles);
    }

    public void testIndexReaderWithDistributedDirectory() throws IOException {
        // First create an index
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "Document content " + i, Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();
        }
        
        // Now read the index
        try (IndexReader reader = DirectoryReader.open(distributedDirectory)) {
            assertEquals("Should have 5 documents", 5, reader.numDocs());
            
            IndexSearcher searcher = new IndexSearcher(reader);
            
            // Search for a specific document
            TermQuery query = new TermQuery(new Term("id", "2"));
            TopDocs results = searcher.search(query, 10);
            
            assertEquals("Should find one document", 1, results.totalHits.value);
            
            Document doc = searcher.doc(results.scoreDocs[0].doc);
            assertEquals("Should find correct document", "2", doc.get("id"));
        }
    }

    public void testIndexWriterWithMerging() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setMaxBufferedDocs(2); // Force frequent segment creation
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            // Add documents to create multiple segments
            for (int i = 0; i < 20; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "Content for document " + i, Field.Store.YES));
                writer.addDocument(doc);
                
                if (i % 5 == 0) {
                    writer.commit(); // Create multiple commits
                }
            }
            
            // Force merge to test segment merging with distributed files
            writer.forceMerge(1);
            writer.commit();
        }
        
        // Verify the index is still readable after merging
        try (IndexReader reader = DirectoryReader.open(distributedDirectory)) {
            assertEquals("Should have all 20 documents after merge", 20, reader.numDocs());
            
            // Verify we can search
            IndexSearcher searcher = new IndexSearcher(reader);
            TermQuery query = new TermQuery(new Term("id", "15"));
            TopDocs results = searcher.search(query, 10);
            
            assertEquals("Should find document after merge", 1, results.totalHits.value);
        }
    }

    public void testIndexDeletion() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            // Add documents
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "Document " + i, Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();
            
            // Delete some documents
            writer.deleteDocuments(new Term("id", "5"));
            writer.deleteDocuments(new Term("id", "7"));
            writer.commit();
        }
        
        // Verify deletions
        try (IndexReader reader = DirectoryReader.open(distributedDirectory)) {
            assertEquals("Should have 8 documents after deletion", 8, reader.numDocs());
            
            IndexSearcher searcher = new IndexSearcher(reader);
            
            // Verify deleted documents are not found
            TermQuery query1 = new TermQuery(new Term("id", "5"));
            TopDocs results1 = searcher.search(query1, 10);
            assertEquals("Deleted document should not be found", 0, results1.totalHits.value);
            
            TermQuery query2 = new TermQuery(new Term("id", "7"));
            TopDocs results2 = searcher.search(query2, 10);
            assertEquals("Deleted document should not be found", 0, results2.totalHits.value);
            
            // Verify non-deleted documents are still found
            TermQuery query3 = new TermQuery(new Term("id", "3"));
            TopDocs results3 = searcher.search(query3, 10);
            assertEquals("Non-deleted document should be found", 1, results3.totalHits.value);
        }
    }

    public void testConcurrentIndexing() throws IOException, InterruptedException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            int numThreads = 4;
            int docsPerThread = 25;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch latch = new CountDownLatch(numThreads);
            AtomicInteger errorCount = new AtomicInteger(0);
            
            // Start concurrent indexing threads
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < docsPerThread; i++) {
                            Document doc = new Document();
                            String docId = threadId + "_" + i;
                            doc.add(new StringField("id", docId, Field.Store.YES));
                            doc.add(new StringField("thread", String.valueOf(threadId), Field.Store.YES));
                            doc.add(new TextField("content", "Content from thread " + threadId + " doc " + i, Field.Store.YES));
                            
                            writer.addDocument(doc);
                            
                            // Occasionally commit
                            if (i % 10 == 0) {
                                writer.commit();
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error in indexing thread " + threadId, e);
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            
            // Wait for all threads to complete
            assertTrue("All threads should complete", latch.await(30, TimeUnit.SECONDS));
            assertEquals("No errors should occur during concurrent indexing", 0, errorCount.get());
            
            writer.commit();
            executor.shutdown();
        }
        
        // Verify all documents were indexed correctly
        try (IndexReader reader = DirectoryReader.open(distributedDirectory)) {
            int expectedDocs = 4 * 25; // 4 threads * 25 docs each
            assertEquals("Should have all documents from concurrent indexing", expectedDocs, reader.numDocs());
            
            IndexSearcher searcher = new IndexSearcher(reader);
            
            // Verify documents from each thread
            for (int t = 0; t < 4; t++) {
                TermQuery query = new TermQuery(new Term("thread", String.valueOf(t)));
                TopDocs results = searcher.search(query, 100);
                assertEquals("Should have 25 documents from thread " + t, 25, results.totalHits.value);
            }
        }
    }

    public void testIndexOptimization() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setMaxBufferedDocs(5); // Create multiple segments
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            // Add many documents to create multiple segments
            for (int i = 0; i < 50; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "Document content " + i, Field.Store.YES));
                writer.addDocument(doc);
                
                if (i % 10 == 0) {
                    writer.commit();
                }
            }
            
            // Get file count before optimization
            String[] filesBefore = distributedDirectory.listAll();
            int filesBeforeCount = filesBefore.length;
            
            // Optimize (force merge to 1 segment)
            writer.forceMerge(1);
            writer.commit();
            
            // Get file count after optimization
            String[] filesAfter = distributedDirectory.listAll();
            int filesAfterCount = filesAfter.length;
            
            // After optimization, we should have fewer files (merged segments)
            assertTrue("Should have fewer files after optimization", filesAfterCount <= filesBeforeCount);
        }
        
        // Verify index is still functional after optimization
        try (IndexReader reader = DirectoryReader.open(distributedDirectory)) {
            assertEquals("Should still have all 50 documents", 50, reader.numDocs());
            assertEquals("Should have only 1 segment after optimization", 1, reader.leaves().size());
        }
    }

    public void testLargeDocuments() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        
        try (IndexWriter writer = new IndexWriter(distributedDirectory, config)) {
            // Create documents with large content
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                
                // Create large content (about 1MB per document)
                StringBuilder largeContent = new StringBuilder();
                for (int j = 0; j < 10000; j++) {
                    largeContent.append("This is a large document with lots of content. Document ID: ").append(i).append(" Line: ").append(j).append(". ");
                }
                
                doc.add(new TextField("content", largeContent.toString(), Field.Store.YES));
                writer.addDocument(doc);
            }
            
            writer.commit();
        }
        
        // Verify large documents can be read
        try (IndexReader reader = DirectoryReader.open(distributedDirectory)) {
            assertEquals("Should have 5 large documents", 5, reader.numDocs());
            
            IndexSearcher searcher = new IndexSearcher(reader);
            TermQuery query = new TermQuery(new Term("id", "2"));
            TopDocs results = searcher.search(query, 10);
            
            assertEquals("Should find the large document", 1, results.totalHits.value);
            
            Document doc = searcher.doc(results.scoreDocs[0].doc);
            String content = doc.get("content");
            assertTrue("Large document content should be preserved", content.length() > 100000);
            assertTrue("Content should contain expected text", content.contains("Document ID: 2"));
        }
    }
}