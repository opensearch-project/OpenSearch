/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.store.remote.file;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.CachedFullFileIndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.filecache.FileCachedIndexInput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Measures the bulk read methods of the searchable snapshot read path. Every read seeks to a random,
 * aligned position and reads {@code length} values, the access pattern of a vector search reading one
 * vector per graph hop. Three inputs are compared over the same bytes:
 * <ul>
 *   <li>{@code mmap}: a plain {@link MMapDirectory} input, the local-store baseline;</li>
 *   <li>{@code fileCached}: a {@link FileCachedIndexInput} wrapping the mmap input, the type the file cache
 *   hands out for a downloaded block;</li>
 *   <li>{@code block}: an {@link AbstractBlockIndexInput} whose blocks are {@link FileCachedIndexInput}s
 *   served from a {@link FileCache}, the full chain used by {@code remote_snapshot} indexes once the
 *   blocks are cached locally.</li>
 * </ul>
 * With {@code access=random} every read seeks to an arbitrary aligned position in the 32 MB file, so most
 * reads on the {@code block} input also switch blocks (close the previous block, look the next one up in the
 * file cache, clone it). With {@code access=singleBlock} all reads stay inside the first block and measure
 * the read itself.
 */
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class BlockIndexInputBulkReadBenchmark {

    /** The block size used by searchable snapshots (8 MB). */
    private static final int BLOCK_SIZE_SHIFT = AbstractBlockIndexInput.Builder.DEFAULT_BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int NUM_BLOCKS = 4;
    private static final long FILE_LENGTH = (long) BLOCK_SIZE * NUM_BLOCKS;
    private static final String FILE_NAME = "vectors.vec";

    @Param({ "mmap", "fileCached", "block" })
    public String input;

    @Param({ "128", "1024" })
    public int length;

    @Param({ "random", "singleBlock" })
    public String access;

    private Path directory;
    private MMapDirectory mmapDirectory;
    private FileCache fileCache;
    private IndexInput indexInput;
    private float[] floats;
    private int[] ints;
    private long[] longs;
    private SplittableRandom random;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = Files.createTempDirectory("block-index-input-bulk-read");
        mmapDirectory = new MMapDirectory(directory);

        // one file with random content plus the same bytes split into block files, as the file cache stores them
        final byte[] block = new byte[BLOCK_SIZE];
        final SplittableRandom content = new SplittableRandom(42);
        try (IndexOutput out = mmapDirectory.createOutput(FILE_NAME, IOContext.DEFAULT)) {
            for (int blockId = 0; blockId < NUM_BLOCKS; blockId++) {
                content.nextBytes(block);
                out.writeBytes(block, 0, block.length);
                final String blockFileName = AbstractBlockIndexInput.getBlockFileName(FILE_NAME, blockId);
                try (IndexOutput blockOut = mmapDirectory.createOutput(blockFileName, IOContext.DEFAULT)) {
                    blockOut.writeBytes(block, 0, block.length);
                }
            }
        }

        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_LENGTH * 2);
        for (int blockId = 0; blockId < NUM_BLOCKS; blockId++) {
            final String blockFileName = AbstractBlockIndexInput.getBlockFileName(FILE_NAME, blockId);
            final Path blockPath = directory.resolve(blockFileName);
            final IndexInput blockInput = mmapDirectory.openInput(blockFileName, IOContext.DEFAULT);
            fileCache.put(
                blockPath,
                new CachedFullFileIndexInput(fileCache, blockPath, new FileCachedIndexInput(fileCache, blockPath, blockInput))
            );
        }

        switch (input) {
            case "mmap":
                indexInput = mmapDirectory.openInput(FILE_NAME, IOContext.DEFAULT);
                break;
            case "fileCached":
                indexInput = new FileCachedIndexInput(
                    fileCache,
                    directory.resolve(FILE_NAME),
                    mmapDirectory.openInput(FILE_NAME, IOContext.DEFAULT)
                );
                break;
            case "block":
                indexInput = new CachedBlockIndexInput(false, 0, FILE_LENGTH);
                break;
            default:
                throw new IllegalArgumentException(input);
        }

        floats = new float[length];
        ints = new int[length];
        longs = new long[length];
        random = new SplittableRandom(0);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        indexInput.close();
        fileCache.clear();
        mmapDirectory.close();
        IOUtils.rm(directory);
    }

    @Benchmark
    public float[] readFloats() throws IOException {
        indexInput.seek(nextOffset(Float.BYTES));
        indexInput.readFloats(floats, 0, length);
        return floats;
    }

    @Benchmark
    public int[] readInts() throws IOException {
        indexInput.seek(nextOffset(Integer.BYTES));
        indexInput.readInts(ints, 0, length);
        return ints;
    }

    @Benchmark
    public long[] readLongs() throws IOException {
        indexInput.seek(nextOffset(Long.BYTES));
        indexInput.readLongs(longs, 0, length);
        return longs;
    }

    /** A random offset aligned to the read size, so reads never straddle a block boundary. */
    private long nextOffset(int bytesPerValue) {
        final long stride = (long) length * bytesPerValue;
        final long range = "singleBlock".equals(access) ? BLOCK_SIZE : FILE_LENGTH;
        return random.nextLong(range / stride) * stride;
    }

    /**
     * Serves blocks the way {@code OnDemandBlockSnapshotIndexInput} does once a block is in the file cache:
     * look the block file up in the {@link FileCache} and return a clone of the cached
     * {@link FileCachedIndexInput} (see {@code TransferManager#fetchBlob}).
     */
    private final class CachedBlockIndexInput extends AbstractBlockIndexInput {

        CachedBlockIndexInput(boolean isClone, long offset, long length) {
            super(builder().blockSizeShift(BLOCK_SIZE_SHIFT).offset(offset).length(length).isClone(isClone).resourceDescription(FILE_NAME));
        }

        @Override
        protected IndexInput fetchBlock(int blockId) throws IOException {
            final Path blockPath = directory.resolve(getBlockFileName(FILE_NAME, blockId));
            final CachedIndexInput cacheEntry = fileCache.get(blockPath); // increments the reference count
            try {
                return cacheEntry.getIndexInput().clone();
            } finally {
                fileCache.decRef(blockPath);
            }
        }

        @Override
        public CachedBlockIndexInput clone() {
            return new CachedBlockIndexInput(true, offset, length);
        }

        @Override
        protected CachedBlockIndexInput buildSlice(String sliceDescription, long sliceOffset, long sliceLength) {
            return new CachedBlockIndexInput(true, offset + sliceOffset, sliceLength);
        }
    }
}
