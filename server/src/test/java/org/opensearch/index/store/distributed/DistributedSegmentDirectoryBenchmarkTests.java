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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DistributedSegmentDirectoryBenchmarkTests extends OpenSearchTestCase {

    private static final int BENCHMARK_DOCS = 1000;
    private static final int BENCHMARK_ITERATIONS = 3;

    public void testFileDistributionValidation() throws IOException {
        Path tempDir = createTempDir();
        Directory baseDir = FSDirectory.open(tempDir);
        DistributedSegmentDirectory distributedDir = new DistributedSegmentDirectory(baseDir, tempDir);
        
        // Create an index to generate various file types
        createTestIndex(distributedDir, 100);
        
        String[] files = distributedDir.listAll();
        assertTrue("Should have created multiple files", files.length > 5);
        
        // Analyze file distribution
        Map<Integer, Integer> distributionCount = new HashMap<>();
        int segmentsFileCount = 0;
        
        for (String file : files) {
            int dirIndex = distributedDir.getDirectoryIndex(file);
            distributionCount.put(dirIndex, distributionCount.getOrDefault(dirIndex, 0) + 1);
            
            if (file.startsWith("segments_")) {
                segmentsFileCount++;
                assertEquals("segments_N files should be in directory 0", 0, dirIndex);
            }
        }
        
        assertTrue("Should have segments files", segmentsFileCount > 0);
        assertTrue("Should use multiple directories", distributionCount.size() > 1);
        
        // Check distribution is reasonably balanced (no directory should have > 70% of files)
        int totalFiles = files.length;
        for (Map.Entry<Integer, Integer> entry : distributionCount.entrySet()) {
            double percentage = (double) entry.getValue() / totalFiles;
            assertTrue("Directory " + entry.getKey() + " has " + (percentage * 100) + "% of files, should be < 70%", 
                      percentage < 0.7);
        }
        
        logger.info("File distribution across directories: {}", distributionCount);
        distributedDir.close();
    }

    public void testHashingDistribution() throws IOException {
        DefaultFilenameHasher hasher = new DefaultFilenameHasher();
        
        // Test with common Lucene file extensions
        String[] testFiles = {
            "_0.cfe", "_0.cfs", "_0.si", "_0.fnm", "_0.fdt", "_0.fdx",
            "_0.tim", "_0.tip", "_0.doc", "_0.pos", "_0.pay", "_0.nvd", "_0.nvm",
            "_1.cfe", "_1.cfs", "_1.si", "_1.fnm", "_1.fdt", "_1.fdx",
            "_1.tim", "_1.tip", "_1.doc", "_1.pos", "_1.pay", "_1.nvd", "_1.nvm",
            "_2.cfe", "_2.cfs", "_2.si", "_2.fnm", "_2.fdt", "_2.fdx"
        };
        
        Map<Integer, Integer> distribution = new HashMap<>();
        
        for (String file : testFiles) {
            int dirIndex = hasher.getDirectoryIndex(file);
            distribution.put(dirIndex, distribution.getOrDefault(dirIndex, 0) + 1);
        }
        
        logger.info("Hash distribution for {} files: {}", testFiles.length, distribution);
        
        // Should use multiple directories
        assertTrue("Should distribute across multiple directories", distribution.size() > 1);
        
        // No single directory should have more than 60% of files
        for (Map.Entry<Integer, Integer> entry : distribution.entrySet()) {
            double percentage = (double) entry.getValue() / testFiles.length;
            assertTrue("Directory " + entry.getKey() + " should not have > 60% of files, has " + (percentage * 100) + "%", 
                      percentage <= 0.6);
        }
    }

    public void testConcurrentAccessValidation() throws IOException, InterruptedException {
        Path tempDir = createTempDir();
        Directory baseDir = FSDirectory.open(tempDir);
        DistributedSegmentDirectory distributedDir = new DistributedSegmentDirectory(baseDir, tempDir);
        
        // Create initial index
        createTestIndex(distributedDir, 100);
        
        // Test concurrent read operations
        int numThreads = 4;
        int operationsPerThread = 25;
        
        Thread[] threads = new Thread[numThreads];
        final Exception[] exceptions = new Exception[numThreads];
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        // Test various operations concurrently
                        String[] files = distributedDir.listAll();
                        assertTrue("Should have files", files.length > 0);
                        
                        // Test file length operations
                        for (String file : files) {
                            if (!file.startsWith("segments_")) {
                                long length = distributedDir.fileLength(file);
                                assertTrue("File length should be positive", length > 0);
                            }
                        }
                        
                        // Test search operations
                        try (IndexReader reader = DirectoryReader.open(distributedDir)) {
                            IndexSearcher searcher = new IndexSearcher(reader);
                            TermQuery query = new TermQuery(new Term("id", String.valueOf(j % 100)));
                            TopDocs results = searcher.search(query, 10);
                            assertTrue("Should find results", results.totalHits.value > 0);
                        }
                    }
                } catch (Exception e) {
                    exceptions[threadId] = e;
                }
            });
            threads[i].start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(30000); // 30 second timeout
        }
        
        // Check for exceptions
        for (int i = 0; i < numThreads; i++) {
            if (exceptions[i] != null) {
                fail("Thread " + i + " failed with exception: " + exceptions[i].getMessage());
            }
        }
        
        distributedDir.close();
    }

    private void createTestIndex(Directory directory, int numDocs) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "This is document " + i + " with some content", Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.commit();
        }
    }
}