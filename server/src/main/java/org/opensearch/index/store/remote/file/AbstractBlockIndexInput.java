/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class acts as a virtual file mechanism for the accessed files and only fetches the required blocks of the actual file.
 * Original/Main IndexInput file will be split using {@link AbstractBlockIndexInput.Builder#DEFAULT_BLOCK_SIZE_SHIFT}. This class has all the
 * logic of how and when to fetch specific block of the main file. Each block is identified by {@link AbstractBlockIndexInput#currentBlockId}.
 * <br>
 * This class delegate the responsibility of actually fetching the block when demanded to its subclasses using
 * {@link AbstractBlockIndexInput#fetchBlock(int)}.
 * <p>
 * Like {@link IndexInput}, this class may only be used from one thread as it is not thread safe.
 * However, a cleaning action may run from another thread triggered by the {@link Cleaner}, but
 * this is okay because at that point the {@link AbstractBlockIndexInput} instance is phantom
 * reachable and therefore not able to be accessed by any other thread.
 *
 * @opensearch.internal
 */
public abstract class AbstractBlockIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger logger = LogManager.getLogger(AbstractBlockIndexInput.class);

    public static final String CLEANER_THREAD_NAME_PREFIX = "index-input-cleaner";

    /**
     * A single static Cleaner instance to ensure any unclosed clone of an
     * IndexInput is closed. This instance creates a single daemon thread on
     * which it performs the cleaning actions. For an already-closed IndexInput,
     * the cleaning action is a no-op. For an open IndexInput, the close action
     * will decrement a reference count.
     */
    protected static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory(CLEANER_THREAD_NAME_PREFIX));

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

    /**
     * Variables used for block calculation and fetching. blockSize must be a
     * power of two, and is defined as 2^blockShiftSize. blockMask is defined
     * as blockSize - 1 and is used to calculate the offset within a block.
     */
    protected final int blockSizeShift;
    protected final int blockSize;
    protected final int blockMask;

    /**
     * ID of the current block
     */
    protected int currentBlockId;

    private final BlockHolder blockHolder = new BlockHolder();
    protected final Cleaner.Cleanable cleanable;

    protected AbstractBlockIndexInput(Builder builder) {
        super(builder.resourceDescription);
        this.isClone = builder.isClone;
        this.offset = builder.offset;
        this.length = builder.length;
        this.blockSizeShift = builder.blockSizeShift;
        this.blockSize = builder.blockSize;
        this.blockMask = builder.blockMask;
        this.cleanable = CLEANER.register(this, blockHolder);
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    protected abstract AbstractBlockIndexInput buildSlice(String sliceDescription, long offset, long length);

    /**
     * Given a blockId, fetch it's IndexInput which might be partial/split/cloned one
     * @param blockId to fetch for
     * @return fetched IndexInput
     */
    protected abstract IndexInput fetchBlock(int blockId) throws IOException;

    @Override
    public abstract AbstractBlockIndexInput clone();

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
        blockHolder.close();
        currentBlockId = 0;
    }

    @Override
    public long getFilePointer() {
        if (blockHolder.block == null) return 0L;
        return currentBlockStart() + currentBlockPosition() - offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (blockHolder.block == null) {
            // seek to the beginning
            seek(0);
        } else if (currentBlockPosition() >= blockSize) {
            int blockId = currentBlockId + 1;
            demandBlock(blockId);
        }
        return blockHolder.block.readByte();
    }

    @Override
    public short readShort() throws IOException {
        if (blockHolder.block != null && Short.BYTES <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (blockHolder.block != null && Integer.BYTES <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (blockHolder.block != null && Long.BYTES <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readLong();
        } else {
            return super.readLong();
        }
    }

    @Override
    public final int readVInt() throws IOException {
        if (blockHolder.block != null && 5 <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readVInt();
        } else {
            return super.readVInt();
        }
    }

    @Override
    public final long readVLong() throws IOException {
        if (blockHolder.block != null && 9 <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readVLong();
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
        if (blockHolder.block != null && isInCurrentBlockRange(pos)) {
            // the block contains the byte
            return ((RandomAccessInput) blockHolder.block).readByte(getBlockOffset(pos));
        } else {
            // the block does not have the byte, seek to the pos first
            seekInternal(pos);
            // then read the byte
            return blockHolder.block.readByte();
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (blockHolder.block != null && isInCurrentBlockRange(pos, Short.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) blockHolder.block).readShort(getBlockOffset(pos));
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
        if (blockHolder.block != null && isInCurrentBlockRange(pos, Integer.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) blockHolder.block).readInt(getBlockOffset(pos));
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
        if (blockHolder.block != null && isInCurrentBlockRange(pos, Long.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) blockHolder.block).readLong(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readLong();
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (blockHolder.block == null) {
            // lazy seek to the beginning
            seek(0);
        }

        int available = blockSize - currentBlockPosition();
        if (len <= available) {
            // the block contains enough data to satisfy this request
            blockHolder.block.readBytes(b, offset, len);
        } else {
            // the block does not have enough data. First serve all we've got.
            if (available > 0) {
                blockHolder.block.readBytes(b, offset, available);
                offset += available;
                len -= available;
            }

            // and now, read the remaining 'len' bytes:
            // len > blocksize example: FST <init>
            while (len > 0) {
                int blockId = currentBlockId + 1;
                int toRead = Math.min(len, blockSize);
                demandBlock(blockId);
                blockHolder.block.readBytes(b, offset, toRead);
                offset += toRead;
                len -= toRead;
            }
        }

    }

    /**
     * Utility method to get the blockSize given blockSizeShift.
     * @param blockSizeShift blockSizeShift used to calculate blockSize.
     * @return returns blockSize
     */
    public static int getBlockSize(int blockSizeShift) {
        return 1 << blockSizeShift;
    }

    /**
     * Utility method to get the blockId corresponding to the file offset passed.
     * @param pos file offset whose blockId is requested.
     * @param blockSizeShift blockSizeShift used to calculate blockSize.
     * @return blockId for the given pos.
     */
    public static int getBlock(long pos, int blockSizeShift) {
        return (int) (pos >>> blockSizeShift);
    }

    /**
     * Utility method to convert file offset to block level offset.
     * @param pos fileOffset whose block offset is requested.
     * @param blockSizeShift blockSizeShift used to calculate blockSize.
     * @return returns block offset for the given pos.
     */
    public static long getBlockOffset(long pos, int blockSizeShift) {
        return (long) (pos & (getBlockSize(blockSizeShift) - 1));
    }

    /**
     * Utility method to get the starting file offset of the given block.
     * @param blockId blockId whose start offset is requested.
     * @param blockSizeShift blockSizeShift used to calculate blockSize.
     * @return  returns the file offset corresponding to the start of the block.
     */
    public static long getBlockStart(int blockId, int blockSizeShift) {
        return (long) blockId << blockSizeShift;
    }

    /**
     * Utility method to get the number of blocks in a file.
     * @param fileSize fileSize of the original file.
     * @param blockSizeShift blockSizeShift used to calculate blockSize.
     * @return returns the number of blocks in the file.
     */
    public static int getNumberOfBlocks(long fileSize, int blockSizeShift) {
        return (int) getBlock(fileSize - 1, blockSizeShift) + 1;
    }

    /**
     * Utility method get the size of the given blockId.
     * @param blockId blockId whose size is requested
     * @param blockSizeShift blockSizeShift used to calculate blockSize.
     * @param fileSize fileSize of the original file.
     * @return returns the size of the block whose blockId is passed.
     */
    public static long getActualBlockSize(int blockId, int blockSizeShift, long fileSize) {
        assert blockId >= 0 : "blockId cannot be negative";
        return (blockId != getBlock(fileSize - 1, blockSizeShift))
            ? getBlockSize(blockSizeShift)
            : getBlockOffset(fileSize - 1, blockSizeShift) + 1;
    }

    /**
     * Utility method to a list of blockIds for a given fileSize.
     * @param fileSize size of the file for which blockIds are requested.
     * @param blockSizeShift blockSizeShift (used to calculate blockSize) used to create blocks.
     * @return returns a list of integers representing blockIds.
     */
    public static List<Integer> getAllBlockIdsForFile(long fileSize, int blockSizeShift) {
        return IntStream.rangeClosed(0, getNumberOfBlocks(fileSize, blockSizeShift) - 1).boxed().collect(Collectors.toList());
    }

    /**
     * Utility method to validate if a given fileName is a blockFileName.
     * @param fileName fileName to check
     * @return returns true if the passed fileName is a valid block file name.
     */
    public static boolean isBlockFilename(String fileName) {
        return fileName.contains("_block_");
    }

    /**
     * Utility method to generate block file name for a given fileName and blockId as per naming convention.
     * @param fileName fileName whose block file name is required
     * @param blockId blockId of the file whose block file name is required
     * @return returns the blockFileName
     */
    public static String getBlockFileName(String fileName, int blockId) {
        return fileName + "_block_" + blockId;
    }

    /**
     * Utility method to get the original file name given the block file name. .
     * @param blockFileName name of the block file whose original file name is required.
     * @return returns the original file name, No op if blockFileName is not a valid name for a block file.
     */
    public static String getFileNameFromBlockFileName(String blockFileName) {
        return blockFileName.contains("_block_") ? blockFileName.substring(0, blockFileName.indexOf("_block_")) : blockFileName;
    }

    /**
     * Seek to a block position, download the block if it's necessary
     * NOTE: the pos should be an adjusted position for slices
     */
    private void seekInternal(long pos) throws IOException {
        if (blockHolder.block == null || !isInCurrentBlockRange(pos)) {
            demandBlock(getBlock(pos));
        }
        blockHolder.block.seek(getBlockOffset(pos));
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
        if (blockHolder.block != null && currentBlockId == blockId) return;

        // close the current block before jumping to the new block
        blockHolder.close();

        blockHolder.set(fetchBlock(blockId));
        currentBlockId = blockId;
    }

    protected void cloneBlock(AbstractBlockIndexInput other) {
        if (other.blockHolder.block != null) {
            this.blockHolder.set(other.blockHolder.block.clone());
            this.currentBlockId = other.currentBlockId;
        }
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
        return (int) blockHolder.block.getFilePointer();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link AbstractBlockIndexInput}. The default block size is 8MiB
     * (see {@link Builder#DEFAULT_BLOCK_SIZE_SHIFT}).
     */
    public static class Builder<T extends Builder<T>> {
        // Block size shift (default value is 23 == 2^23 == 8MiB)
        public static final int DEFAULT_BLOCK_SIZE_SHIFT = 23;
        public static final int DEFAULT_BLOCK_SIZE = 1 << DEFAULT_BLOCK_SIZE_SHIFT;;

        protected String resourceDescription;
        protected boolean isClone;
        protected long offset;
        protected long length;
        protected int blockSizeShift = DEFAULT_BLOCK_SIZE_SHIFT;
        protected int blockSize = 1 << blockSizeShift;
        protected int blockMask = blockSize - 1;

        protected Builder() {}

        @SuppressWarnings("unchecked")
        protected final T self() {
            return (T) this;
        }

        public T resourceDescription(String resourceDescription) {
            this.resourceDescription = Objects.requireNonNull(resourceDescription, "Resource description cannot be null");
            return self();
        }

        public T isClone(boolean clone) {
            this.isClone = clone;
            return self();
        }

        public T offset(long offset) {
            this.offset = offset;
            return self();
        }

        public T length(long length) {
            this.length = length;
            return self();
        }

        public T blockSizeShift(int blockSizeShift) {
            assert blockSizeShift < 31 : "blockSizeShift must be < 31";
            this.blockSizeShift = blockSizeShift;
            this.blockSize = 1 << blockSizeShift;
            this.blockMask = blockSize - 1;
            return self();
        }
    }

    /**
     * Simple class to hold the currently open IndexInput backing an instance
     * of an {@link AbstractBlockIndexInput}. Lucene may clone one of these
     * instances, and per the contract[1], the clones will never be closed.
     * However, closing the instances is critical for our reference counting.
     * Therefore, we are using the {@link Cleaner} mechanism from the JDK to
     * close these clones when they become phantom reachable. The clean action
     * must not hold a reference to the {@link AbstractBlockIndexInput} itself
     * (otherwise it would never become phantom reachable!) so we need a wrapper
     * instance to hold the current underlying IndexInput, while allowing it to
     * be changed out with different instances as {@link AbstractBlockIndexInput}
     * reads through the data.
     * <p>
     * This class implements {@link Runnable} so that it can be passed directly
     * to the cleaner to run its close action.
     * <p>
     * [1]: https://github.com/apache/lucene/blob/8340b01c3cc229f33584ce2178b07b8984daa6a9/lucene/core/src/java/org/apache/lucene/store/IndexInput.java#L32-L33
     */
    private static class BlockHolder implements Closeable, Runnable {
        private volatile IndexInput block;

        private void set(IndexInput block) {
            if (this.block != null) {
                throw new IllegalStateException("Previous block was not closed!");
            }
            this.block = Objects.requireNonNull(block);
        }

        @Override
        public void close() throws IOException {
            if (block != null) {
                block.close();
                block = null;
            }
        }

        @Override
        public void run() {
            try {
                close();
            } catch (IOException e) {
                // Exceptions thrown in the cleaning action are ignored,
                // so log and swallow the exception here
                logger.info("Exception thrown while closing block owned by phantom reachable instance", e);
            }
        }
    }
}
