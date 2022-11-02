/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * Class acts as a virtual file mechanism for the accessed files and only fetches the required blocks of the actual file.
 * Original/Main IndexInput file will be split using {@link OnDemandBlockIndexInput.Builder#DEFAULT_BLOCK_SIZE_SHIFT}. This class has all the
 * logic of how and when to fetch specific block of the main file. Each block is identified by {@link OnDemandBlockIndexInput#currentBlockId}.
 * <br>
 * This class delegate the responsibility of actually fetching the block when demanded to its subclasses using
 * {@link OnDemandBlockIndexInput#fetchBlock(int)}.
 *
 * @opensearch.internal
 */
abstract class OnDemandBlockIndexInput extends IndexInput implements RandomAccessInput {
    /**
     * Start offset of the virtual file : non-zero in the slice case
     */
    protected final long offset;
    /**
     * Length of the virtual file, smaller than actual file size if it's a slice
     */
    protected final long length;

    /**
     * Whether this index input is a clone or otherwise the root file before slicing
     */
    protected final boolean isClone;

    // Variables needed for block calculation and fetching logic
    /**
     * Block size shift (default value is 13 = 8KB)
     */
    protected final int blockSizeShift;

    /**
     * Fixed block size
     */
    protected final int blockSize;

    /**
     * Block mask
     */
    protected final int blockMask;

    // Variables for actual held open block
    /**
     * Current block for read, it should be a cloned block always
     */
    protected IndexInput currentBlock;

    /**
     * ID of the current block
     */
    protected int currentBlockId;

    OnDemandBlockIndexInput(Builder builder) {
        super(builder.resourceDescription);
        this.isClone = builder.isClone;
        this.offset = builder.offset;
        this.length = builder.length;
        this.blockSizeShift = builder.blockSizeShift;
        this.blockSize = builder.blockSize;
        this.blockMask = builder.blockMask;
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    protected abstract OnDemandBlockIndexInput buildSlice(String sliceDescription, long offset, long length);

    /**
     * Given a blockId, fetch it's IndexInput which might be partial/split/cloned one
     * @param blockId to fetch for
     * @return fetched IndexInput
     */
    protected abstract IndexInput fetchBlock(int blockId) throws IOException;

    @Override
    public OnDemandBlockIndexInput clone() {
        OnDemandBlockIndexInput clone = buildSlice("clone", offset, length());
        // Ensures that clones may be positioned at the same point as the blocked file they were cloned from
        if (currentBlock != null) {
            clone.currentBlock = currentBlock.clone();
            clone.currentBlockId = currentBlockId;
        }

        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this
            );
        }

        // The slice is seeked to the beginning.
        return buildSlice(sliceDescription, offset, length);
    }

    @Override
    public void close() throws IOException {
        // current block
        if (currentBlock != null) {
            currentBlock.close();
            currentBlock = null;
            currentBlockId = 0;
        }
    }

    @Override
    public long getFilePointer() {
        if (currentBlock == null) return 0L;
        return currentBlockStart() + currentBlockPosition() - offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (currentBlock == null) {
            // seek to the beginning
            seek(0);
        } else if (currentBlockPosition() >= blockSize) {
            int blockId = currentBlockId + 1;
            demandBlock(blockId);
        }
        return currentBlock.readByte();
    }

    @Override
    public short readShort() throws IOException {
        if (currentBlock != null && Short.BYTES <= (blockSize - currentBlockPosition())) {
            return currentBlock.readShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (currentBlock != null && Integer.BYTES <= (blockSize - currentBlockPosition())) {
            return currentBlock.readInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (currentBlock != null && Long.BYTES <= (blockSize - currentBlockPosition())) {
            return currentBlock.readLong();
        } else {
            return super.readLong();
        }
    }

    @Override
    public final int readVInt() throws IOException {
        if (currentBlock != null && 5 <= (blockSize - currentBlockPosition())) {
            return currentBlock.readVInt();
        } else {
            return super.readVInt();
        }
    }

    @Override
    public final long readVLong() throws IOException {
        if (currentBlock != null && 9 <= (blockSize - currentBlockPosition())) {
            return currentBlock.readVLong();
        } else {
            return super.readVLong();
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }

        seekInternal(pos + offset);
    }

    @Override
    public final byte readByte(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos)) {
            // the block contains the byte
            return ((RandomAccessInput) currentBlock).readByte(getBlockOffset(pos));
        } else {
            // the block does not have the byte, seek to the pos first
            seekInternal(pos);
            // then read the byte
            return currentBlock.readByte();
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos, Short.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) currentBlock).readShort(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readShort();
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos, Integer.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) currentBlock).readInt(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readInt();
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (currentBlock != null && isInCurrentBlockRange(pos, Long.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) currentBlock).readLong(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readLong();
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (currentBlock == null) {
            // lazy seek to the beginning
            seek(0);
        }

        int available = blockSize - currentBlockPosition();
        if (len <= available) {
            // the block contains enough data to satisfy this request
            currentBlock.readBytes(b, offset, len);
        } else {
            // the block does not have enough data. First serve all we've got.
            if (available > 0) {
                currentBlock.readBytes(b, offset, available);
                offset += available;
                len -= available;
            }

            // and now, read the remaining 'len' bytes:
            // len > blocksize example: FST <init>
            while (len > 0) {
                int blockId = currentBlockId + 1;
                int toRead = Math.min(len, blockSize);
                demandBlock(blockId);
                currentBlock.readBytes(b, offset, toRead);
                offset += toRead;
                len -= toRead;
            }
        }

    }

    /**
     * Seek to a block position, download the block if it's necessary
     * NOTE: the pos should be an adjusted position for slices
     */
    private void seekInternal(long pos) throws IOException {
        if (currentBlock == null || !isInCurrentBlockRange(pos)) {
            demandBlock(getBlock(pos));
        }
        currentBlock.seek(getBlockOffset(pos));
    }

    /**
     * Check if pos in current block range
     * NOTE: the pos should be an adjusted position for slices
     */
    private boolean isInCurrentBlockRange(long pos) {
        long offset = pos - currentBlockStart();
        return offset >= 0 && offset < blockSize;
    }

    /**
     * Check if [pos, pos + len) in current block range
     * NOTE: the pos should be an adjusted position for slices
     */
    private boolean isInCurrentBlockRange(long pos, int len) {
        long offset = pos - currentBlockStart();
        return offset >= 0 && (offset + len) <= blockSize;
    }

    private void demandBlock(int blockId) throws IOException {
        if (currentBlock != null && currentBlockId == blockId) return;

        // close the current block before jumping to the new block
        if (currentBlock != null) {
            currentBlock.close();
        }

        currentBlock = fetchBlock(blockId).clone();
        currentBlockId = blockId;
    }

    protected int getBlock(long pos) {
        return (int) (pos >>> blockSizeShift);
    }

    protected int getBlockOffset(long pos) {
        return (int) (pos & blockMask);
    }

    protected long getBlockStart(int blockId) {
        return (long) blockId << blockSizeShift;
    }

    protected long currentBlockStart() {
        return getBlockStart(currentBlockId);
    }

    protected int currentBlockPosition() {
        return (int) currentBlock.getFilePointer();
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        // Block size shift (default value is 13 = 8KB)
        public static final int DEFAULT_BLOCK_SIZE_SHIFT = 13;

        private String resourceDescription;
        private boolean isClone;
        private long offset;
        private long length;
        private int blockSizeShift = DEFAULT_BLOCK_SIZE_SHIFT;
        private int blockSize = 1 << blockSizeShift;
        private int blockMask = blockSize - 1;

        private Builder() {}

        public Builder resourceDescription(String resourceDescription) {
            this.resourceDescription = resourceDescription;
            return this;
        }

        public Builder isClone(boolean clone) {
            isClone = clone;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        public Builder blockSizeShift(int blockSizeShift) {
            assert blockSizeShift < 31 : "blockSizeShift must be < 31";
            this.blockSizeShift = blockSizeShift;
            this.blockSize = 1 << blockSizeShift;
            this.blockMask = blockSize - 1;
            return this;
        }
    }
}
