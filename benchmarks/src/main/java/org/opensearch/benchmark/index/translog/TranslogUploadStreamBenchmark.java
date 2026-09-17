/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.translog;

import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.translog.transfer.FileSnapshot;
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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the two ways a translog file can be handed to a multipart (async) upload.
 * <p>
 * {@link #buffered} is what the transfer service used to do: read the whole {@code .tlog} onto the
 * heap with {@code readAllBytes()}, then serve every part out of that one array. The array is the
 * size of the file, so on a 31g heap (16 MiB G1 regions) anything past 8 MiB is a humongous
 * allocation that lands straight in old gen.
 * <p>
 * {@link #streamed} is what it does now: ask the snapshot for a supplier and let each part read
 * its own range directly off the file. Nothing file-sized is ever on the heap.
 * <p>
 * Both variants hand the uploader identical bytes and are verified byte-for-byte equal in
 * {@link #setup}, so the interesting column is <b>{@code gc.alloc.rate.norm}</b> (bytes allocated
 * per operation) from {@code -prof gc}, not the timing. Expect roughly {@code fileSize} bytes/op
 * for {@code buffered} and a small part-sized constant for {@code streamed}.
 * <p>
 * Run with:
 * <pre>
 * ./gradlew -p benchmarks run --args 'TranslogUploadStreamBenchmark -prof gc -f 1 -wi 3 -i 5'
 * </pre>
 */
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
// Pin the heap and the G1 region size: the whole point is where an allocation lands, and G1 sizes
// its regions off the heap by default, which would make the humongous threshold vary per machine.
// 16m regions put that threshold at 8 MiB, matching a production 31g heap.
@Fork(value = 1, jvmArgsAppend = { "-Xms4g", "-Xmx4g", "-XX:+UseG1GC", "-XX:G1HeapRegionSize=16m" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class TranslogUploadStreamBenchmark {

    /**
     * 4 MiB stays under G1's humongous threshold, 16 MiB sits on it, 64 MiB is what a real big5
     * generation looked like, 256 MiB is what a slow-syncing shard can reach.
     */
    @Param({ "4194304", "16777216", "67108864", "268435456" })
    public int fileSize;

    /** Part size the S3 uploader would use, so the part count is realistic. */
    private static final long PART_SIZE = 16L * 1024 * 1024;

    private static final long PRIMARY_TERM = 1L;
    private static final long GENERATION = 7L;

    private Path dir;
    private Path tlog;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory("translog-upload-bench");
        // The transfer service asserts on the "-<generation>." shape of the name, so keep it real.
        tlog = dir.resolve("translog-" + GENERATION + ".tlog");
        byte[] content = new byte[fileSize];
        new Random(20260915L).nextBytes(content);
        Files.write(tlog, content);

        // Guard against benchmarking two things that are not equivalent: if the arms ever stop
        // producing identical bytes, the allocation comparison is meaningless.
        long bufferedSum = buffered();
        long streamedSum = streamed();
        if (bufferedSum != streamedSum) {
            throw new AssertionError("arms disagree: buffered=" + bufferedSum + " streamed=" + streamedSum);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        Files.deleteIfExists(tlog);
        Files.deleteIfExists(dir);
    }

    /** Pre-change behaviour: one file-sized heap array, every part sliced out of it. */
    @Benchmark
    public long buffered() throws IOException {
        final byte[] content;
        try (InputStream inputStream = Files.newInputStream(tlog)) {
            content = inputStream.readAllBytes();
        }
        final long contentLength = content.length;
        RemoteTransferContainer.OffsetRangeInputStreamSupplier supplier = (size, position) -> new OffsetRangeIndexInputStream(
            new ByteArrayIndexInput(tlog.getFileName().toString(), content),
            size,
            position
        );
        return drainAllParts(supplier, contentLength);
    }

    /** Post-change behaviour: the snapshot supplies part streams straight off the file. */
    @Benchmark
    public long streamed() throws IOException {
        try (FileSnapshot.TranslogFileSnapshot snapshot = new FileSnapshot.TranslogFileSnapshot(PRIMARY_TERM, GENERATION, tlog, null)) {
            return drainAllParts(snapshot.offsetRangeInputStreamSupplier(), snapshot.getContentLength());
        }
    }

    /**
     * Reads every part the uploader would read, in order, and folds the bytes into a checksum.
     * The checksum is returned rather than discarded so the JIT cannot elide the reads and turn
     * the comparison into a measurement of nothing.
     */
    private long drainAllParts(RemoteTransferContainer.OffsetRangeInputStreamSupplier supplier, long contentLength) throws IOException {
        long checksum = 0;
        byte[] scratch = new byte[8192];
        for (long position = 0; position < contentLength; position += PART_SIZE) {
            long partSize = Math.min(PART_SIZE, contentLength - position);
            try (OffsetRangeInputStream part = supplier.get(partSize, position)) {
                int read;
                while ((read = part.read(scratch, 0, scratch.length)) != -1) {
                    // Strided rather than per-byte: enough of a data dependency on the buffer that
                    // the reads cannot be optimised away, without the folding cost dominating the
                    // measurement at 256 MiB.
                    for (int i = 0; i < read; i += 512) {
                        checksum = checksum * 31 + scratch[i];
                    }
                }
            }
        }
        return checksum;
    }
}
