/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.utils;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests for {@link DirectoryUtils}.
 */
public class DirectoryUtilsTests extends OpenSearchTestCase {

    public void testGetFSDirectoryDirect() throws IOException {
        Path tempDir = createTempDir();
        FSDirectory fsDir = FSDirectory.open(tempDir);
        assertSame("Should return the FSDirectory directly", fsDir, DirectoryUtils.getFSDirectory(fsDir));
        fsDir.close();
    }

    public void testGetFSDirectoryWrappedInFilterDirectory() throws IOException {
        Path tempDir = createTempDir();
        FSDirectory fsDir = FSDirectory.open(tempDir);
        FilterDirectory wrapped = new FilterDirectory(fsDir) {
        };
        assertSame("Should unwrap FilterDirectory to find FSDirectory", fsDir, DirectoryUtils.getFSDirectory(wrapped));
        wrapped.close();
    }

    public void testGetFSDirectoryDeepChain() throws IOException {
        Path tempDir = createTempDir();
        FSDirectory fsDir = FSDirectory.open(tempDir);
        FilterDirectory inner = new FilterDirectory(fsDir) {
        };
        FilterDirectory outer = new FilterDirectory(inner) {
        };
        assertSame("Should unwrap multiple FilterDirectory layers", fsDir, DirectoryUtils.getFSDirectory(outer));
        outer.close();
    }

    public void testGetFSDirectoryThrowsWhenNoFSDirectory() throws IOException {
        ByteBuffersDirectory ramDir = new ByteBuffersDirectory();
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> DirectoryUtils.getFSDirectory(ramDir));
        assertTrue("Should mention class name", ex.getMessage().contains("ByteBuffersDirectory"));
        ramDir.close();
    }

    public void testGetFilePathResolves() throws IOException {
        Path tempDir = createTempDir();
        FSDirectory fsDir = FSDirectory.open(tempDir);
        Path result = DirectoryUtils.getFilePath(fsDir, "test.txt");
        assertEquals("Should resolve file path", tempDir.resolve("test.txt"), result);
        fsDir.close();
    }

    public void testGetFilePathSwitchableResolves() throws IOException {
        Path tempDir = createTempDir();
        FSDirectory fsDir = FSDirectory.open(tempDir);
        Path result = DirectoryUtils.getFilePathSwitchable(fsDir, "test.txt");
        assertEquals("Should resolve switchable path", tempDir.resolve("test.txt" + DirectoryUtils.SWITCHABLE_PREFIX), result);
        fsDir.close();
    }
}
