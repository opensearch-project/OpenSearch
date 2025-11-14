/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class IndexFileDeleterTests extends OpenSearchTestCase {

    private IndexFileDeleter indexFileDeleter;
    private CompositeIndexingExecutionEngine mockEngine;
    private ShardPath shardPath;
    private CatalogSnapshot catalogSnapshot;
    private Map<Long, CatalogSnapshot> catalogSnapshotMap;
    private AtomicLong catalogSnapshotId;
    private AtomicLong lastCommittedSnapshotId;
    private Set<String> deletedFiles;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId(new Index("test", "test-uuid"), 0);
        Path shardDir = tempDir.resolve("test-uuid").resolve("0");
        shardPath = new ShardPath(false, shardDir, shardDir, shardId);
        
        mockEngine = mock(CompositeIndexingExecutionEngine.class);
        catalogSnapshotId = new AtomicLong(0);
        lastCommittedSnapshotId = new AtomicLong(0);
        catalogSnapshotMap = new HashMap<>();
        deletedFiles = new HashSet<>();

        
        // Mock engine deleteFiles to track deleted files
        doAnswer(invocation -> {
            Map<String, Collection<String>> filesToDelete = invocation.getArgument(0);
            filesToDelete.values().forEach(deletedFiles::addAll);
            return null;
        }).when(mockEngine).deleteFiles(any());
        
        catalogSnapshot = null;
        indexFileDeleter = new IndexFileDeleter(mockEngine, null, shardPath);
        CatalogSnapshot.setIndexFileDeleter(indexFileDeleter);
        CatalogSnapshot.setCatalogSnapshotMap(catalogSnapshotMap);
    }

    public void testMultipleDataFormats() {
        Map<String, List<WriterFileSet>> files = Map.of(
                "parquet", createWriterFileSet("dir1", "file1.parquet"),
                "lucene", createWriterFileSet("dir2", "file1.lucene")
        );

        simulateRefresh(files);

        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").size());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("lucene").size());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet").get());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("lucene").get("dir2/file1.lucene").get());
    }

    public void testRefreshCreatesNewSnapshotAndAddsReferences() {
        // Simulate refresh creating new snapshot
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file1.parquet")));
        
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").size());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet").get());
    }

    public void testMultipleSnapshotsWithOverlappingFiles() {
        // First refresh
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file1.parquet", "file2.parquet")));
        
        // Second refresh with overlapping files
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file2.parquet", "file3.parquet")));

        // After first refresh refCounts: file1(1), file2 (1)
        // After second refresh refcounts: file1(0, delete should be called), file2(1), file3(1)
        assertNull(indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet"));
        assertTrue(deletedFiles.contains("dir1/file1.parquet"));
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file2.parquet").get());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file3.parquet").get());
    }

    public void testFileDeletionDuringSearch() throws IOException {
        // First refresh
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file1.parquet", "file2.parquet")));

        Closeable searchContext = startSearch();

        // Second refresh with overlapping files
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file2.parquet", "file3.parquet")));

        // since we have a active search request, files from previous snapshot won't be deleted
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet").get());
        assertEquals(2, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file2.parquet").get());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file3.parquet").get());

        searchContext.close();

        // After search is closed, files from previous snapshot should be deleted
        assertNull(indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet"));
        assertTrue(deletedFiles.contains("dir1/file1.parquet"));
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file2.parquet").get());
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file3.parquet").get());
    }

    public void testDeletionsWthFlush() {
        // Refresh and then flush
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file1.parquet", "file2.parquet")));
        simulateFlush();

        // Refresh again
        simulateRefresh(Map.of("parquet", createWriterFileSet("dir1", "file2.parquet", "file3.parquet")));

        // Since file1 is part of last commited data(flushed) it will not be deleted even if it is not part of current snapshot
        assertEquals(1, indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet").get());
        assertFalse(deletedFiles.contains("dir1/file1.parquet"));

        simulateFlush();
        // After flush, file1 should be deleted since it is now no more part of last commited data and neither current snapshot as well
        assertNull(indexFileDeleter.getFileRefCounts().get("parquet").get("dir1/file1.parquet"));
        assertTrue(deletedFiles.contains("dir1/file1.parquet"));
    }


    private void simulateRefresh(Map<String, List<WriterFileSet>> files) {
        // Create RefreshResult
        RefreshResult refreshResult = mock(RefreshResult.class);
        Map<DataFormat, List<WriterFileSet>> refreshedFiles = new HashMap<>();
        
        files.forEach((formatName, fileSets) -> {
            DataFormat dataFormat = mock(DataFormat.class);
            when(dataFormat.name()).thenReturn(formatName);
            refreshedFiles.put(dataFormat, fileSets);
        });
        
        when(refreshResult.getRefreshedFiles()).thenReturn(refreshedFiles);

        CatalogSnapshot prevSnap = catalogSnapshot;

        // Create new snapshot
        long id = catalogSnapshotId.incrementAndGet();
        catalogSnapshot = new CatalogSnapshot(refreshResult, id);
        catalogSnapshotMap.put(id, catalogSnapshot);

        // Release previous snapshot if exists
        if (prevSnap != null) {
            prevSnap.decRef();
        }
    }

    private void simulateFlush() {
        CatalogSnapshot prevCommitedSnapshot = null;
        if (lastCommittedSnapshotId.get() != 0L) {
            prevCommitedSnapshot = catalogSnapshotMap.get(lastCommittedSnapshotId.get());
        }
        //flushing increases the refCount of current snapshot and decreases the refCount of previously flushed snapshot
        catalogSnapshot.incRef();
        lastCommittedSnapshotId.set(catalogSnapshotId.get());
        if (prevCommitedSnapshot != null) {
            prevCommitedSnapshot.decRef();
        }
    }

    private Closeable startSearch() {
        // simulating search behaviour - acquiring snapshot and returning a closeable to release it
        CatalogSnapshot currentSearchSnapshot = catalogSnapshot;
        currentSearchSnapshot.incRef();
        return currentSearchSnapshot::decRef;
    }

    private List<WriterFileSet> createWriterFileSet(String directory, String... files) {
        WriterFileSet.Builder builder = WriterFileSet.builder()
            .directory(Path.of(directory))
            .writerGeneration(1L);
        
        for (String file : files) {
            builder.addFile(file);
        }
        
        return Collections.singletonList(builder.build());
    }
}