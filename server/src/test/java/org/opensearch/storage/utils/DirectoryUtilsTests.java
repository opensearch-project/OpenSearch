/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.utils;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

/**
 * Unit tests for DirectoryUtils.
 */
public class DirectoryUtilsTests extends LuceneTestCase {

    private Path tempDir;
    private FSDirectory directory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        directory = FSDirectory.open(tempDir);
    }

    @After
    public void tearDown() throws Exception {
        directory.close();
        super.tearDown();
    }

    public void testGetFilePath() {
        Path result = DirectoryUtils.getFilePath(directory, "_0.cfe");
        assertEquals(tempDir.resolve("_0.cfe"), result);
    }

    public void testGetFilePathSwitchable() {
        Path result = DirectoryUtils.getFilePathSwitchable(directory, "_0.cfe");
        assertEquals(tempDir.resolve("_0.cfe_switchable"), result);
    }

    public void testSwitchablePrefix() {
        assertEquals("_switchable", DirectoryUtils.SWITCHABLE_PREFIX);
    }
}
