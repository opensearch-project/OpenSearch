/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FullFileCachedIndexInput;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class CompositeDirectoryTests extends BaseRemoteSegmentStoreDirectoryTests {
    private FileCache fileCache;
    private FSDirectory localDirectory;
    private CompositeDirectory compositeDirectory;

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();
        localDirectory = mock(FSDirectory.class);
        fileCache = mock(FileCache.class);
        compositeDirectory = new CompositeDirectory(localDirectory, remoteSegmentStoreDirectory, fileCache);
    }

    public void testListAll() throws IOException {
        when(localDirectory.listAll()).thenReturn(new String[]{});
        String[] actualFileNames = compositeDirectory.listAll();
        String[] expectedFileNames = new String[] {};
        assertArrayEquals(expectedFileNames, actualFileNames);

        populateMetadata();
        when(localDirectory.listAll()).thenReturn(new String[] { "_1.cfe", "_2.cfe", "_0.cfe_block_7", "_0.cfs_block_7" });

        actualFileNames = compositeDirectory.listAll();
        expectedFileNames = new String[] { "_0.cfe", "_0.cfs", "_0.si", "_1.cfe", "_2.cfe", "segments_1" };
        assertArrayEquals(expectedFileNames, actualFileNames);
    }

    public void testDeleteFile() throws IOException {
        Path basePath = mock(Path.class);
        Path resolvedPath = mock(Path.class);
        when(basePath.resolve(anyString())).thenReturn(resolvedPath);
        when(localDirectory.getDirectory()).thenReturn(basePath);

        compositeDirectory.deleteFile("_0.tmp");
        verify(localDirectory).deleteFile("_0.tmp");

        compositeDirectory.deleteFile("_0.si");
        verify(fileCache).remove(resolvedPath);
    }

    public void testFileLength() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        Path basePath = mock(Path.class);
        Path resolvedPath = mock(Path.class);
        when(basePath.resolve("_0.si")).thenReturn(resolvedPath);
        when(localDirectory.getDirectory()).thenReturn(basePath);
        when(localDirectory.fileLength("_0.si")).thenReturn(7L);

        // File present locally
        CachedIndexInput indexInput = mock(CachedIndexInput.class);
        when(fileCache.get(resolvedPath)).thenReturn(indexInput);
        assertEquals(compositeDirectory.fileLength("_0.si"), 7L);
        verify(localDirectory).fileLength(startsWith("_0.si"));

        // File not present locally
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploadedSegments = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();
        assertTrue(uploadedSegments.containsKey("_0.si"));
        when(fileCache.get(resolvedPath)).thenReturn(null);
        assertEquals(compositeDirectory.fileLength("_0.si"), uploadedSegments.get("_0.si").getLength());
    }

    public void testCreateOutput() throws IOException {
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(localDirectory.createOutput("_0.si", IOContext.DEFAULT)).thenReturn(indexOutput);
        IndexOutput actualIndexOutput = compositeDirectory.createOutput("_0.si", IOContext.DEFAULT);
        assert actualIndexOutput instanceof CloseableFilterIndexOutput;
        verify(localDirectory).createOutput("_0.si", IOContext.DEFAULT);
    }

    public void testSync() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        Collection<String> names = List.of("_0.cfe", "_0.cfs", "_1.cfe", "_1.cfs", "_2.nvm", "segments_1");
        compositeDirectory.sync(names);
        verify(localDirectory).sync(List.of("_1.cfe", "_1.cfs", "_2.nvm"));
    }

    public void testRename() throws IOException {
        Path basePath = mock(Path.class);
        Path resolvedPathOldFile = mock(Path.class);
        Path resolvedPathNewFile = mock(Path.class);
        when(basePath.resolve("old_file_name")).thenReturn(resolvedPathOldFile);
        when(basePath.resolve("new_file_name")).thenReturn(resolvedPathNewFile);
        when(localDirectory.getDirectory()).thenReturn(basePath);
        CachedIndexInput indexInput = mock(CachedIndexInput.class);
        when(fileCache.get(resolvedPathNewFile)).thenReturn(indexInput);
        compositeDirectory.rename("old_file_name", "new_file_name");
        verify(localDirectory).rename("old_file_name", "new_file_name");
        verify(fileCache).remove(resolvedPathOldFile);
        verify(fileCache).put(eq(resolvedPathNewFile), any(FullFileCachedIndexInput.class));
    }

    public void testOpenInput() throws IOException {
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        Path basePath = mock(Path.class);
        Path resolvedPathInCache = mock(Path.class);
        Path resolvedPathNotInCache = mock(Path.class);
        when(basePath.resolve("_0.si")).thenReturn(resolvedPathInCache);
        when(basePath.resolve("_0.cfs")).thenReturn(resolvedPathNotInCache);
        when(localDirectory.getDirectory()).thenReturn(basePath);
        CachedIndexInput cachedIndexInput = mock(CachedIndexInput.class);
        IndexInput localIndexInput = mock(IndexInput.class);
        IndexInput indexInput = mock(IndexInput.class);
        when(fileCache.get(resolvedPathInCache)).thenReturn(cachedIndexInput);
        when(fileCache.compute(eq(resolvedPathInCache), any())).thenReturn(cachedIndexInput);
        when(cachedIndexInput.getIndexInput()).thenReturn(indexInput);
        when(indexInput.clone()).thenReturn(indexInput);
        when(fileCache.get(resolvedPathNotInCache)).thenReturn(null);

        // Temp file, read directly form local directory
        when(localDirectory.openInput("_0.tmp", IOContext.DEFAULT)).thenReturn(localIndexInput);
        assertEquals(compositeDirectory.openInput("_0.tmp", IOContext.DEFAULT), localIndexInput);
        verify(localDirectory).openInput("_0.tmp", IOContext.DEFAULT);

        // File present in file cache
        assertEquals(compositeDirectory.openInput("_0.si", IOContext.DEFAULT), indexInput);

        // File present in Remote
        IndexInput indexInput1 = compositeDirectory.openInput("_0.cfs", IOContext.DEFAULT);
        assert indexInput1 instanceof OnDemandBlockSnapshotIndexInput;
    }

    public void testClose() throws IOException {
        Path basePath = mock(Path.class);
        Path resolvedPath1 = mock(Path.class);
        Path resolvedPath2 = mock(Path.class);
        when(basePath.resolve("_0.si")).thenReturn(resolvedPath1);
        when(basePath.resolve("_0.cfs")).thenReturn(resolvedPath2);
        when(localDirectory.getDirectory()).thenReturn(basePath);
        when(localDirectory.listAll()).thenReturn(new String[] { "_0.si", "_0.cfs" });
        compositeDirectory.close();
        verify(localDirectory).close();
        verify(fileCache).remove(resolvedPath1);
        verify(fileCache).remove(resolvedPath2);
    }

    public void testAfterSyncToRemote() throws IOException {
        Path basePath = mock(Path.class);
        Path resolvedPath = mock(Path.class);
        when(basePath.resolve(anyString())).thenReturn(resolvedPath);
        when(localDirectory.getDirectory()).thenReturn(basePath);
        Collection<String> files = Arrays.asList("_0.si", "_0.cfs");
        compositeDirectory.afterSyncToRemote(files);
        verify(fileCache, times(files.size())).decRef(resolvedPath);
    }
}
