/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.opensearch.storage.directory.CleanerDaemonThreadLeakFilter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for BlockIndexInput.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class BlockIndexInputTests extends LuceneTestCase {

    private static final String FILE_NAME = "_1.cfe";
    private static final String BLOCK_FILE_0 = "_1.cfe_block_0";
    private static final String BLOCK_FILE_1 = "_1.cfe_block_1";
    private static final long TEST_FILE_SIZE = 10_485_760L; // 10MB
    private static final int BLOCK_SIZE_SHIFT = 23; // 8MB blocks
    private static final long BLOCK_SIZE = 1L << BLOCK_SIZE_SHIFT; // 8MB

    private Path tempDir;
    private FSDirectory directory;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        directory = FSDirectory.open(tempDir);
        // Create block files with test data
        createBlockFile(BLOCK_FILE_0, BLOCK_SIZE);
        createBlockFile(BLOCK_FILE_1, TEST_FILE_SIZE - BLOCK_SIZE);
    }

    @After
    public void tearDown() throws Exception {
        directory.close();
        super.tearDown();
    }

    private void createBlockFile(String name, long size) throws IOException {
        Path filePath = tempDir.resolve(name);
        try (OutputStream os = Files.newOutputStream(filePath)) {
            byte[] buffer = new byte[8192];
            Arrays.fill(buffer, (byte) 7);
            long written = 0;
            while (written < size) {
                int toWrite = (int) Math.min(buffer.length, size - written);
                os.write(buffer, 0, toWrite);
                written += toWrite;
            }
        }
    }

    public void testBuildBlockIndexInput() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .resourceDescription("test")
            .name(FILE_NAME)
            .localDirectory(directory)
            .fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT)
            .length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT)
            .build();
        assertNotNull(input);
        assertTrue(input.length() >= 0);
        input.close();
    }

    public void testBuilderValidation() {
        // Missing file name
        expectThrows(IllegalStateException.class, () -> BlockIndexInput.builder().resourceDescription("test")
            .localDirectory(directory).fileSize(TEST_FILE_SIZE).context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build());

        // Missing directory
        expectThrows(IllegalStateException.class, () -> BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).fileSize(TEST_FILE_SIZE).context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build());
    }

    public void testReadByte() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).localDirectory(directory).fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build();
        byte b = input.readByte();
        assertEquals(7, b);
        input.close();
    }

    public void testClone() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).localDirectory(directory).fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build();
        input.readByte(); // advance position
        BlockIndexInput clone = input.clone();
        assertNotNull(clone);
        assertEquals(input.length(), clone.length());
        clone.close();
        input.close();
    }

    public void testSlice() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).localDirectory(directory).fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build();
        IndexInput slice = input.slice("test-slice", 0, 1024);
        assertNotNull(slice);
        assertEquals(1024, slice.length());
        slice.close();
        input.close();
    }

    public void testSliceOutOfBounds() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).localDirectory(directory).fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build();
        expectThrows(IllegalArgumentException.class, () -> input.slice("bad", 0, TEST_FILE_SIZE + 1));
        input.close();
    }

    public void testReadBytes() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).localDirectory(directory).fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build();
        byte[] buffer = new byte[100];
        input.readBytes(buffer, 0, 100);
        for (byte b : buffer) {
            assertEquals(7, b);
        }
        input.close();
    }

    public void testSeekAndRead() throws IOException {
        BlockIndexInput input = BlockIndexInput.builder().resourceDescription("test")
            .name(FILE_NAME).localDirectory(directory).fileSize(TEST_FILE_SIZE)
            .context(IOContext.DEFAULT).length(TEST_FILE_SIZE).offset(0).blockSizeShift(BLOCK_SIZE_SHIFT).build();
        input.seek(100);
        assertEquals(100, input.getFilePointer());
        byte b = input.readByte();
        assertEquals(7, b);
        input.close();
    }
}
