/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.utils;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

/**
 * Tests for {@link DirectoryUtils}.
 *
 * @opensearch.experimental
 */
public class DirectoryUtilsTests extends OpenSearchTestCase {

    private Path tempDir;
    private FSDirectory fsDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        fsDirectory = FSDirectory.open(tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        fsDirectory.close();
        super.tearDown();
    }

    public void testUnwrapFSDirectoryDirect() {
        FSDirectory result = DirectoryUtils.unwrapFSDirectory(fsDirectory);
        assertSame(fsDirectory, result);
    }

    public void testUnwrapFSDirectorySingleWrapper() {
        FilterDirectory wrapped = new FilterDirectory(fsDirectory) {
        };
        FSDirectory result = DirectoryUtils.unwrapFSDirectory(wrapped);
        assertSame(fsDirectory, result);
    }

    public void testUnwrapFSDirectoryMultipleWrappers() {
        FilterDirectory inner = new FilterDirectory(fsDirectory) {
        };
        FilterDirectory outer = new FilterDirectory(inner) {
        };
        FSDirectory result = DirectoryUtils.unwrapFSDirectory(outer);
        assertSame(fsDirectory, result);
    }

    public void testUnwrapFSDirectoryThrowsWhenNoFSDirectory() {
        Directory nonFsDir = new ByteBuffersDirectory();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> DirectoryUtils.unwrapFSDirectory(nonFsDir));
        assertTrue(ex.getMessage().contains("Expected FSDirectory but got"));
    }

    public void testUnwrapFSDirectoryThrowsWhenWrappedNonFSDirectory() {
        Directory nonFsDir = new ByteBuffersDirectory();
        FilterDirectory wrapped = new FilterDirectory(nonFsDir) {
        };
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> DirectoryUtils.unwrapFSDirectory(wrapped));
        assertTrue(ex.getMessage().contains("Expected FSDirectory but got"));
    }

    public void testGetFilePath() {
        Path result = DirectoryUtils.getFilePath(fsDirectory, "test_file.si");
        assertEquals(tempDir.resolve("test_file.si"), result);
    }

    public void testGetFilePathWithWrappedDirectory() {
        FilterDirectory wrapped = new FilterDirectory(fsDirectory) {
        };
        Path result = DirectoryUtils.getFilePath(wrapped, "test_file.si");
        assertEquals(tempDir.resolve("test_file.si"), result);
    }

    public void testGetFilePathSwitchable() {
        Path result = DirectoryUtils.getFilePathSwitchable(fsDirectory, "test_file.si");
        assertEquals(tempDir.resolve("test_file.si" + DirectoryUtils.SWITCHABLE_PREFIX), result);
    }

    public void testGetFilePathSwitchableWithWrappedDirectory() {
        FilterDirectory wrapped = new FilterDirectory(fsDirectory) {
        };
        Path result = DirectoryUtils.getFilePathSwitchable(wrapped, "test_file.si");
        assertEquals(tempDir.resolve("test_file.si" + DirectoryUtils.SWITCHABLE_PREFIX), result);
    }
}
