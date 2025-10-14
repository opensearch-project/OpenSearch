/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link AbstractBlockIndexInput} covering all static methods,
 * builder validation, and core functionality.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class AbstractBlockIndexInputTests extends OpenSearchTestCase {
    private static final int ONE_MB_SHIFT = 20;
    private static final int ONE_MB = 1 << ONE_MB_SHIFT;
    private static final int TWO_MB = ONE_MB * 2;

    public void testBuilderDefaults() {
        AbstractBlockIndexInput.Builder<?> builder = AbstractBlockIndexInput.builder();
        assertEquals(builder.blockSizeShift, AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE_SHIFT);
        assertEquals(builder.blockSize, AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE);
    }

    public void testBuilderBlockSizeShiftValidation() {
        expectThrows(AssertionError.class, () -> { AbstractBlockIndexInput.builder().blockSizeShift(31); });
    }

    public void testGetActualBlockSizeThrowsErrorForNegativeBlockId() {
        expectThrows(AssertionError.class, () -> { AbstractBlockIndexInput.getActualBlockSize(-1, 23, 23); });
    }

    public void testStaticBlockCalculations() {
        assertEquals(1024, AbstractBlockIndexInput.getBlockSize(10));
        assertEquals(0, AbstractBlockIndexInput.getBlock(512, 10));
        assertEquals(1, AbstractBlockIndexInput.getBlock(1024, 10));
        assertEquals(512, AbstractBlockIndexInput.getBlockOffset(512, 10));
        assertEquals(0, AbstractBlockIndexInput.getBlockStart(0, 10));
        assertEquals(1024, AbstractBlockIndexInput.getBlockStart(1, 10));
    }

    public void testGetNumberOfBlocks() {
        assertEquals(1, AbstractBlockIndexInput.getNumberOfBlocks(1024, 10));
        assertEquals(2, AbstractBlockIndexInput.getNumberOfBlocks(1025, 10));
        assertEquals(2, AbstractBlockIndexInput.getNumberOfBlocks(2048, 10));
    }

    public void testGetActualBlockSize() {
        assertEquals(1024, AbstractBlockIndexInput.getActualBlockSize(0, 10, 2048));
        assertEquals(1024, AbstractBlockIndexInput.getActualBlockSize(1, 10, 2048));
        assertEquals(1, AbstractBlockIndexInput.getActualBlockSize(1, 10, 1025));
    }

    public void testFileNameUtilities() {
        assertTrue(AbstractBlockIndexInput.isBlockFilename("file_block_0"));
        assertFalse(AbstractBlockIndexInput.isBlockFilename("file.txt"));
        assertEquals("file_block_5", AbstractBlockIndexInput.getBlockFileName("file", 5));
        assertEquals("original", AbstractBlockIndexInput.getFileNameFromBlockFileName("original_block_3"));
        assertEquals("noblock", AbstractBlockIndexInput.getFileNameFromBlockFileName("noblock"));
    }

    public void testGetAllBlockIdsForFile() {
        List<Integer> blockIds = AbstractBlockIndexInput.getAllBlockIdsForFile(2048, 10);
        assertEquals(2, blockIds.size());
        assertEquals(Integer.valueOf(0), blockIds.get(0));
        assertEquals(Integer.valueOf(1), blockIds.get(1));
    }

    public void testSeekBeyondLength() throws IOException {
        try (AbstractBlockIndexInput indexInput = createTestIndexInput()) {
            expectThrows(EOFException.class, () -> indexInput.seek(TWO_MB + 1));
        }
    }

    public void testSliceValidation() throws IOException {
        try (AbstractBlockIndexInput indexInput = createTestIndexInput()) {
            expectThrows(IllegalArgumentException.class, () -> indexInput.slice("test", -1, 100));
            expectThrows(IllegalArgumentException.class, () -> indexInput.slice("test", 0, -1));
            expectThrows(IllegalArgumentException.class, () -> indexInput.slice("test", 0, TWO_MB + 1));
        }
    }

    public void testReadOperationsWithoutSeek() throws IOException {
        try (AbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.readByte();
        }
    }

    public void testReadBytesAcrossBlocks() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.seek(ONE_MB - 10);
            assertEquals(0, indexInput.getCurrentBlockId());
            byte[] buffer = new byte[20];
            indexInput.readBytes(buffer, 0, 20);
            assertEquals(1, indexInput.getCurrentBlockId());
        }
    }

    public void testRandomAccessReads() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.seek(0);
            assertEquals(0, indexInput.getCurrentBlockId());
            indexInput.readByte(ONE_MB + 100);
        }
    }

    public void testGetFilePointer() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            assertEquals(0, indexInput.getFilePointer());
            indexInput.seek(100);
            assertEquals(100, indexInput.getFilePointer());
        }
    }

    public void testLength() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            assertEquals(TWO_MB, indexInput.length());
        }
    }

    public void testMultiByteReads() throws IOException {
        try (AbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.seek(0);
            indexInput.readShort();
            indexInput.readInt();
            indexInput.readLong();
            indexInput.readVInt();
            indexInput.readVLong();
        }
    }

    public void testRandomAccessMultiByteReads() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.seek(0);
            indexInput.readShort(100);
            indexInput.readInt(200);
            indexInput.readLong(300);
            Assert.assertEquals(0, indexInput.getCurrentBlockId());
        }
    }

    public void testBlockTransitionDuringRead() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.seek(ONE_MB - 1);
            Assert.assertEquals(0, indexInput.getCurrentBlockId());
            indexInput.readByte();
            indexInput.readByte();
            Assert.assertEquals(1, indexInput.getCurrentBlockId());
        }
    }

    public void testSliceCreation() throws IOException {
        try (TestAbstractBlockIndexInput indexInput = createTestIndexInput()) {
            TestAbstractBlockIndexInput slice = (TestAbstractBlockIndexInput) indexInput.slice("test-slice", ONE_MB, 1000);
            slice.readByte();
            Assert.assertEquals(1, slice.getCurrentBlockId());
            assertNotNull(slice);
            slice.close();
        }
    }

    public void testCloneAndOriginalIndependence() throws IOException {
        try (AbstractBlockIndexInput indexInput = createTestIndexInput()) {
            indexInput.seek(0);
            AbstractBlockIndexInput clone = indexInput.clone();
            clone.seek(ONE_MB);
            assertEquals(0, indexInput.getFilePointer());
            assertTrue(clone.getFilePointer() > 0);
            assertEquals(ONE_MB, clone.getFilePointer());
            clone.close();
        }
    }

    public void testEmptyBlockIds() {
        List<Integer> blockIds = AbstractBlockIndexInput.getAllBlockIdsForFile(0, 10);
        assertEquals(0, blockIds.size());
    }

    public void testBlockCalculationEdgeCases() {
        assertEquals(0, AbstractBlockIndexInput.getBlock(0, 10));
        assertEquals(0, AbstractBlockIndexInput.getBlockOffset(0, 10));
        assertEquals(1023, AbstractBlockIndexInput.getBlockOffset(1023, 10));
    }

    private TestAbstractBlockIndexInput createTestIndexInput() {
        return new TestAbstractBlockIndexInput(false);
    }

    private static class TestAbstractBlockIndexInput extends AbstractBlockIndexInput {
        TestAbstractBlockIndexInput(boolean isClone) {
            super(
                builder().blockSizeShift(ONE_MB_SHIFT)
                    .offset(0)
                    .length(TWO_MB)
                    .isClone(isClone)
                    .resourceDescription(TestAbstractBlockIndexInput.class.getName())
            );
            cleanable.clean();
        }

        TestAbstractBlockIndexInput(boolean isClone, long offset, long length) {
            super(
                builder().blockSizeShift(ONE_MB_SHIFT)
                    .offset(0)
                    .length(TWO_MB)
                    .offset(offset)
                    .length(length)
                    .isClone(isClone)
                    .resourceDescription(TestAbstractBlockIndexInput.class.getName())
            );
            cleanable.clean();
        }

        @Override
        protected IndexInput fetchBlock(int blockId) throws IOException {
            return new ByteArrayIndexInput(
                "",
                new byte[(int) AbstractBlockIndexInput.getActualBlockSize(blockId, this.blockSizeShift, this.length)]
            );
        }

        @Override
        public TestAbstractBlockIndexInput clone() {
            return buildSlice("", 0L, length);
        }

        public TestAbstractBlockIndexInput buildSlice(String sliceDescription, long offset, long length) {
            return new TestAbstractBlockIndexInput(true, this.offset + offset, length);
        }

        private int getCurrentBlockId() {
            return currentBlockId;
        }
    }
}
