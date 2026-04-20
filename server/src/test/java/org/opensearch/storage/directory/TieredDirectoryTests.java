/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.storage.indexinput.SwitchableIndexInput;
import org.opensearch.storage.indexinput.SwitchableIndexInputWrapper;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;

/**
 * Tests for {@link TieredDirectory}.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class TieredDirectoryTests extends TieredStorageBaseTestCase {

    private FileCache fileCache;
    private FSDirectory localDirectory;
    private TieredDirectory tieredDirectory;

    private static final String[] LOCAL_FILES = new String[] {
        "_1.cfe",
        "_1.cfe_block_0",
        "_1.cfe_block_1",
        "_2.cfe",
        "_0.cfe_block_7",
        "_0.cfs_block_7",
        "_x.abc_block_0",
        "temp_file.tmp" };
    private static final String FILE_PRESENT_LOCALLY = "_1.cfe";
    private static final String FILE_RENAMED = "_1_new.cfe";
    private static final String FILE_PRESENT_IN_REMOTE_ONLY = "_0.si";
    private static final String NON_EXISTENT_FILE = "non_existent_file";
    private static final String TEMP_FILE = "temp_file.tmp";

    @Before
    public void setup() throws IOException {
        setupRemoteSegmentStoreDirectory();
        populateMetadata();
        remoteSegmentStoreDirectory.init();
        localDirectory = FSDirectory.open(createTempDir());
        removeExtraFSFiles();
        int concurrencyLevel = randomIntBetween(1, 2);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, concurrencyLevel);
        tieredDirectory = new TieredDirectory(localDirectory, remoteSegmentStoreDirectory, fileCache, threadPool);
        addFilesToDirectory(LOCAL_FILES);
    }

    public void testListAll() throws IOException {
        String[] actualFileNames = tieredDirectory.listAll();
        String[] expectedFileNames = new String[] {
            "_0.cfe",
            "_0.cfs",
            "_0.si",
            "_1.cfe",
            "_2.cfe",
            "_x.abc",
            "segments_1",
            "temp_file.tmp" };
        assertArrayEquals(expectedFileNames, actualFileNames);
    }

    public void testDeleteFile() throws IOException {
        assertTrue(existsInTieredDirectory(TEMP_FILE));
        tieredDirectory.deleteFile(TEMP_FILE);
        assertFalse(existsInTieredDirectory(TEMP_FILE));

        assertFalse(existsInTieredDirectory(NON_EXISTENT_FILE));
        Exception exception = null;
        try {
            tieredDirectory.deleteFile(NON_EXISTENT_FILE);
        } catch (Exception e) {
            exception = e;
        }
        assertNull(exception);

        assertTrue(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertTrue(existsInLocalDirectory(FILE_PRESENT_LOCALLY));
        assertTrue(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertTrue(existsInFileCache(getFilePath(localDirectory, FILE_PRESENT_LOCALLY)));
        tieredDirectory.deleteFile(FILE_PRESENT_LOCALLY);
        assertFalse(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertFalse(existsInLocalDirectory(FILE_PRESENT_LOCALLY));
        assertFalse(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertFalse(existsInFileCache(getFilePath(localDirectory, FILE_PRESENT_LOCALLY)));

        assertTrue(existsInTieredDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
        assertTrue(existsInRemoteDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
        tieredDirectory.deleteFile(FILE_PRESENT_IN_REMOTE_ONLY);
        assertTrue(existsInTieredDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
        assertTrue(existsInRemoteDirectory(FILE_PRESENT_IN_REMOTE_ONLY));
    }

    public void testRename() throws IOException {
        assertThrows(NoSuchFileException.class, () -> tieredDirectory.rename(NON_EXISTENT_FILE, FILE_RENAMED));

        assertTrue(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertTrue(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertFalse(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_RENAMED)));
        tieredDirectory.rename(FILE_PRESENT_LOCALLY, FILE_RENAMED);
        assertFalse(existsInTieredDirectory(FILE_PRESENT_LOCALLY));
        assertFalse(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        assertTrue(existsInFileCache(getFilePathSwitchable(localDirectory, FILE_RENAMED)));
    }

    public void testOpenInput() throws IOException {
        populateData();

        assertFalse(existsInTieredDirectory(NON_EXISTENT_FILE));
        assertThrows(NoSuchFileException.class, () -> tieredDirectory.openInput(NON_EXISTENT_FILE, IOContext.DEFAULT));

        assertTrue(existsInLocalDirectory(TEMP_FILE) && FileTypeUtils.isTempFile(TEMP_FILE));

        assertEquals(
            tieredDirectory.openInput(TEMP_FILE, IOContext.DEFAULT).toString(),
            localDirectory.openInput(TEMP_FILE, IOContext.DEFAULT).toString()
        );

        assertNotNull(fileCache.get(getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY)));
        IndexInput indexInput = tieredDirectory.openInput(FILE_PRESENT_LOCALLY, IOContext.DEFAULT);
        assertNotNull(indexInput);
        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        assertFalse(getSwitchableIndexInputFromWrapper(indexInput).isCachedFromRemote());

        assertNull(fileCache.get(getFilePathSwitchable(localDirectory, FILE_PRESENT_IN_REMOTE_ONLY)));
        indexInput = tieredDirectory.openInput(FILE_PRESENT_IN_REMOTE_ONLY, IOContext.DEFAULT);
        assertNotNull(indexInput);
        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        assertTrue(getSwitchableIndexInputFromWrapper(indexInput).isCachedFromRemote());
    }

    public void testIndexInputCleanup() throws IOException {
        populateData();
        Path path = getFilePathSwitchable(localDirectory, FILE_PRESENT_LOCALLY);
        assertEquals(1, (int) fileCache.getRef(path));
        IndexInput indexInput = tieredDirectory.openInput(FILE_PRESENT_LOCALLY, IOContext.DEFAULT);
        assertEquals(2, (int) fileCache.getRef(path));
        decRefToZero(path);
        createUnclosedClones(indexInput, path);
        triggerGarbageCollectionAndAssertClonesClosed(path);
    }

    public void testAfterSyncToRemote() throws IOException {
        populateData();
        String fileName = "_0.si";
        String fileNameBlock = "_0.si_block_0";
        addFilesToDirectory(new String[] { fileName });

        assertTrue(existsInLocalDirectory(fileName));
        assertEquals(1, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(1, (int) fileCache.getRef(getFilePath(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        IndexInput indexInput = tieredDirectory.openInput(fileName, IOContext.DEFAULT);

        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        assertEquals(2, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(2, (int) fileCache.getRef(getFilePath(localDirectory, fileName)));
        assertFalse(getSwitchableIndexInputFromWrapper(indexInput).isCachedFromRemote());

        indexInput.close();

        assertEquals(1, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(1, (int) fileCache.getRef(getFilePath(localDirectory, fileName)));

        tieredDirectory.afterSyncToRemote(fileName);

        assertFalse(existsInLocalDirectory(fileName));
        assertTrue(existsInRemoteDirectory(fileName));
        assertEquals(0, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        indexInput = tieredDirectory.openInput(fileName, IOContext.DEFAULT);

        assertEquals(1, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(1, (int) fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        indexInput.close();

        assertEquals(0, (int) fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertEquals(0, (int) fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));

        fileCache.prune();
        assertNull(fileCache.getRef(getFilePathSwitchable(localDirectory, fileName)));
        assertNull(fileCache.getRef(getFilePath(localDirectory, fileNameBlock)));
    }

    private void removeExtraFSFiles() throws IOException {
        HashSet<String> allFiles = new HashSet<>(Arrays.asList(localDirectory.listAll()));
        allFiles.stream().filter(FileTypeUtils::isExtraFSFile).forEach(file -> {
            try {
                localDirectory.deleteFile(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void addFilesToDirectory(String[] files) throws IOException {
        for (String file : files) {
            IndexOutput indexOutput = tieredDirectory.createOutput(file, IOContext.DEFAULT);
            indexOutput.close();
        }
    }

    private SwitchableIndexInput getSwitchableIndexInputFromWrapper(IndexInput indexInput) {
        assertTrue(indexInput instanceof SwitchableIndexInputWrapper);
        return (SwitchableIndexInput) (((FilterIndexInput) indexInput).getDelegate());
    }

    private void decRefToZero(Path path) {
        int refCount = fileCache.getRef(path);
        for (int i = 0; i < refCount; i++) {
            fileCache.decRef(path);
        }
    }

    private void createUnclosedClones(IndexInput indexInput, Path path) {
        IndexInput clone = indexInput.clone();
        IndexInput cloneOfClone = clone.clone();
        assertEquals(2, (int) fileCache.getRef(path));
    }

    private void triggerGarbageCollectionAndAssertClonesClosed(Path path) {
        try {
            assertBusy(() -> {
                System.gc();
                assertEquals("Expected refCount to drop to original count", (int) fileCache.getRef(path), 0);
            }, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Exception thrown while triggering gc", e);
            fail();
        }
    }

    private boolean existsInLocalDirectory(String name) throws IOException {
        return Arrays.asList(localDirectory.listAll()).contains(name);
    }

    private boolean existsInRemoteDirectory(String name) throws IOException {
        return Arrays.asList(remoteSegmentStoreDirectory.listAll()).contains(name);
    }

    private boolean existsInTieredDirectory(String name) throws IOException {
        return Arrays.asList(tieredDirectory.listAll()).contains(name);
    }

    private boolean existsInFileCache(Path path) {
        return (fileCache.get(path) != null);
    }
}
