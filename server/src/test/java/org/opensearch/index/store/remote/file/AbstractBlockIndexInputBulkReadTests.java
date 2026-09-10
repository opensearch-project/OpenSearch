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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the bulk read methods ({@code readFloats}, {@code readInts}, {@code readLongs}) of
 * {@link AbstractBlockIndexInput}: values must match the underlying data whether or not the range crosses a
 * block boundary, and a range that fits in the current block must be served with a single bulk read on the
 * block instead of one element-wise read per value.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class AbstractBlockIndexInputBulkReadTests extends OpenSearchTestCase {

    private static final int BLOCK_SIZE_SHIFT = 16;
    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int NUM_BLOCKS = 4;
    private static final int FILE_LENGTH = BLOCK_SIZE * NUM_BLOCKS;

    private byte[] data;
    private ByteBuffer expected;
    private List<BulkReadCountingIndexInput> blocks;

    @Before
    public void setUpData() {
        data = new byte[FILE_LENGTH];
        random().nextBytes(data);
        expected = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        blocks = new ArrayList<>();
    }

    public void testReadFloatsWithinBlockIsSingleBulkRead() throws IOException {
        final int len = 1024;
        final int pos = randomIntBetween(0, BLOCK_SIZE - len * Float.BYTES);
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            float[] actual = new float[len];
            input.readFloats(actual, 0, len);
            assertFloats(pos, actual);
            assertEquals(pos + (long) len * Float.BYTES, input.getFilePointer());
            assertEquals(0, input.currentBlock());
            assertEquals(1, totalReadFloatsCalls());
            assertEquals(0, totalElementReads());
        }
    }

    public void testReadIntsWithinBlockIsSingleBulkRead() throws IOException {
        final int len = 512;
        final int pos = randomIntBetween(0, BLOCK_SIZE - len * Integer.BYTES);
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            int[] actual = new int[len];
            input.readInts(actual, 0, len);
            assertInts(pos, actual);
            assertEquals(pos + (long) len * Integer.BYTES, input.getFilePointer());
            assertEquals(1, totalReadIntsCalls());
            assertEquals(0, totalElementReads());
        }
    }

    public void testReadLongsWithinBlockIsSingleBulkRead() throws IOException {
        final int len = 256;
        final int pos = randomIntBetween(0, BLOCK_SIZE - len * Long.BYTES);
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            long[] actual = new long[len];
            input.readLongs(actual, 0, len);
            assertLongs(pos, actual);
            assertEquals(pos + (long) len * Long.BYTES, input.getFilePointer());
            assertEquals(1, totalReadLongsCalls());
            assertEquals(0, totalElementReads());
        }
    }

    public void testBulkReadWithoutSeekStartsAtBeginning() throws IOException {
        final int len = 64;
        try (CountingBlockIndexInput input = newInput()) {
            float[] actual = new float[len];
            input.readFloats(actual, 0, len);
            assertFloats(0, actual);
            assertEquals((long) len * Float.BYTES, input.getFilePointer());
            assertEquals(1, totalReadFloatsCalls());
        }
    }

    public void testBulkReadEndingExactlyAtBlockBoundary() throws IOException {
        final int len = 1024;
        final int pos = BLOCK_SIZE - len * Float.BYTES;
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            float[] actual = new float[len];
            input.readFloats(actual, 0, len);
            assertFloats(pos, actual);
            assertEquals(1, totalReadFloatsCalls());
            assertEquals(0, input.currentBlock());
            // the next byte comes from the following block
            assertEquals(data[BLOCK_SIZE], input.readByte());
            assertEquals(1, input.currentBlock());
        }
    }

    public void testReadFloatsAcrossBlockBoundary() throws IOException {
        final int len = 1024;
        final int pos = BLOCK_SIZE - randomIntBetween(1, len - 1) * Float.BYTES;
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            float[] actual = new float[len];
            input.readFloats(actual, 0, len);
            assertFloats(pos, actual);
            assertEquals(pos + (long) len * Float.BYTES, input.getFilePointer());
            assertEquals(1, input.currentBlock());
        }
    }

    public void testReadIntsAcrossBlockBoundary() throws IOException {
        final int len = 512;
        final int pos = BLOCK_SIZE - randomIntBetween(1, len - 1) * Integer.BYTES;
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            int[] actual = new int[len];
            input.readInts(actual, 0, len);
            assertInts(pos, actual);
            assertEquals(pos + (long) len * Integer.BYTES, input.getFilePointer());
            assertEquals(1, input.currentBlock());
        }
    }

    public void testReadLongsAcrossBlockBoundary() throws IOException {
        final int len = 256;
        final int pos = BLOCK_SIZE - randomIntBetween(1, len - 1) * Long.BYTES;
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            long[] actual = new long[len];
            input.readLongs(actual, 0, len);
            assertLongs(pos, actual);
            assertEquals(pos + (long) len * Long.BYTES, input.getFilePointer());
            assertEquals(1, input.currentBlock());
        }
    }

    public void testBulkReadSpanningMultipleBlocks() throws IOException {
        // longer than one block: exercises the element-wise fallback across more than one boundary
        final int len = (BLOCK_SIZE / Integer.BYTES) + 1000;
        final int pos = BLOCK_SIZE - 400;
        try (CountingBlockIndexInput input = newInput()) {
            input.seek(pos);
            int[] actual = new int[len];
            input.readInts(actual, 0, len);
            assertInts(pos, actual);
            assertEquals(pos + (long) len * Integer.BYTES, input.getFilePointer());
            assertEquals(2, input.currentBlock());
        }
    }

    public void testBulkReadOnSlice() throws IOException {
        final int sliceOffset = BLOCK_SIZE + 40;
        final int sliceLength = 8192;
        final int len = 64;
        try (CountingBlockIndexInput input = newInput()) {
            IndexInput slice = input.slice("slice", sliceOffset, sliceLength);
            slice.seek(8);
            float[] actual = new float[len];
            slice.readFloats(actual, 0, len);
            assertFloats(sliceOffset + 8, actual);
            assertEquals(8 + (long) len * Float.BYTES, slice.getFilePointer());
            assertEquals(1, totalReadFloatsCalls());
            slice.close();
        }
    }

    public void testRandomBulkReadsMatchUnderlyingData() throws IOException {
        try (CountingBlockIndexInput input = newInput()) {
            for (int i = 0; i < 200; i++) {
                final int len = randomIntBetween(1, 2048);
                switch (randomInt(2)) {
                    case 0: {
                        int pos = randomIntBetween(0, FILE_LENGTH - len * Float.BYTES);
                        input.seek(pos);
                        float[] actual = new float[len];
                        input.readFloats(actual, 0, len);
                        assertFloats(pos, actual);
                        break;
                    }
                    case 1: {
                        int pos = randomIntBetween(0, FILE_LENGTH - len * Integer.BYTES);
                        input.seek(pos);
                        int[] actual = new int[len];
                        input.readInts(actual, 0, len);
                        assertInts(pos, actual);
                        break;
                    }
                    default: {
                        int pos = randomIntBetween(0, FILE_LENGTH - len * Long.BYTES);
                        input.seek(pos);
                        long[] actual = new long[len];
                        input.readLongs(actual, 0, len);
                        assertLongs(pos, actual);
                        break;
                    }
                }
            }
        }
    }

    private void assertFloats(int pos, float[] actual) {
        for (int i = 0; i < actual.length; i++) {
            assertEquals("float " + i + " at " + pos, expected.getFloat(pos + i * Float.BYTES), actual[i], 0f);
        }
    }

    private void assertInts(int pos, int[] actual) {
        for (int i = 0; i < actual.length; i++) {
            assertEquals("int " + i + " at " + pos, expected.getInt(pos + i * Integer.BYTES), actual[i]);
        }
    }

    private void assertLongs(int pos, long[] actual) {
        for (int i = 0; i < actual.length; i++) {
            assertEquals("long " + i + " at " + pos, expected.getLong(pos + i * Long.BYTES), actual[i]);
        }
    }

    private int totalReadFloatsCalls() {
        return blocks.stream().mapToInt(b -> b.readFloatsCalls).sum();
    }

    private int totalReadIntsCalls() {
        return blocks.stream().mapToInt(b -> b.readIntsCalls).sum();
    }

    private int totalReadLongsCalls() {
        return blocks.stream().mapToInt(b -> b.readLongsCalls).sum();
    }

    /** Element-wise reads on the blocks: what the default DataInput bulk implementations degrade into. */
    private int totalElementReads() {
        return blocks.stream().mapToInt(b -> b.readByteCalls + b.readShortCalls + b.readIntCalls + b.readLongCalls).sum();
    }

    private CountingBlockIndexInput newInput() {
        return new CountingBlockIndexInput(false, 0, FILE_LENGTH);
    }

    private final class CountingBlockIndexInput extends AbstractBlockIndexInput {

        CountingBlockIndexInput(boolean isClone, long offset, long length) {
            super(
                builder().blockSizeShift(BLOCK_SIZE_SHIFT)
                    .offset(offset)
                    .length(length)
                    .isClone(isClone)
                    .resourceDescription(CountingBlockIndexInput.class.getName())
            );
        }

        @Override
        protected IndexInput fetchBlock(int blockId) {
            final int start = (int) getBlockStart(blockId, blockSizeShift);
            final int size = (int) getActualBlockSize(blockId, blockSizeShift, FILE_LENGTH);
            final BulkReadCountingIndexInput block = new BulkReadCountingIndexInput("block-" + blockId, data, start, size);
            blocks.add(block);
            return block;
        }

        @Override
        public CountingBlockIndexInput clone() {
            return new CountingBlockIndexInput(true, offset, length);
        }

        @Override
        protected CountingBlockIndexInput buildSlice(String sliceDescription, long sliceOffset, long sliceLength) {
            return new CountingBlockIndexInput(true, offset + sliceOffset, sliceLength);
        }

        int currentBlock() {
            return currentBlockId;
        }
    }
}
