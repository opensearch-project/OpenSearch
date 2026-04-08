/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Unit tests for BlockFetchRequest.
 */
public class BlockFetchRequestTests extends LuceneTestCase {

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

    public void testBuildRequest() {
        BlockFetchRequest request = BlockFetchRequest.builder()
            .directory(directory)
            .fileName("_0.cfe")
            .blockFileName("_0.cfe_block_0")
            .blockStart(0)
            .blockSize(8388608)
            .build();

        assertEquals("_0.cfe", request.getFileName());
        assertEquals("_0.cfe_block_0", request.getBlockFileName());
        assertEquals(0, request.getBlockStart());
        assertEquals(8388608, request.getBlockSize());
        assertNotNull(request.getFilePath());
        assertNotNull(request.getDirectory());
    }

    public void testFilePathResolution() {
        BlockFetchRequest request = BlockFetchRequest.builder()
            .directory(directory)
            .fileName("_0.cfe")
            .blockFileName("_0.cfe_block_0")
            .blockStart(0)
            .blockSize(8388608)
            .build();

        Path expectedPath = tempDir.resolve("_0.cfe_block_0");
        assertEquals(expectedPath, request.getFilePath());
    }

    public void testToString() {
        BlockFetchRequest request = BlockFetchRequest.builder()
            .directory(directory)
            .fileName("_0.cfe")
            .blockFileName("_0.cfe_block_0")
            .blockStart(0)
            .blockSize(8388608)
            .build();

        String str = request.toString();
        assertTrue(str.contains("_0.cfe"));
        assertTrue(str.contains("_0.cfe_block_0"));
    }
}
