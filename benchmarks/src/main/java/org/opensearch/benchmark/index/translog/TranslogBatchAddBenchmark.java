/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.translog;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.LocalTranslog;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the CPU/allocation cost that a batched {@code Translog.add(List)} removes relative to adding the same
 * operations one at a time. A single add allocates a fresh {@code ReleasableBytesStreamOutput} per op, takes the
 * translog read lock per op, and enters the {@code TranslogWriter} monitor per op; the batch does each of those
 * once for the whole batch. The on-disk bytes are identical either way (verified by the unit test), so the
 * interesting columns are time per op (ns/op) and {@code gc.alloc.rate.norm} (B/op) from {@code -prof gc}.
 * <p>
 * Thread count is passed on the command line ({@code -t 1} and {@code -t 8}); all threads share one translog, so at
 * {@code -t 8} the read-lock and writer-monitor contention this change targets is exercised directly.
 * <p>
 * Run with:
 * <pre>
 * ./gradlew -p benchmarks run --args 'TranslogBatchAddBenchmark -prof gc -f 1 -wi 3 -i 5 -t 1'
 * ./gradlew -p benchmarks run --args 'TranslogBatchAddBenchmark -prof gc -f 1 -wi 3 -i 5 -t 8'
 * </pre>
 */
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class TranslogBatchAddBenchmark {

    /** Operations per add call. 1 is the control (batch == single). */
    @Param({ "1", "10", "100", "500" })
    public int batchSize;

    /** Approximate http_logs document source size in bytes. */
    private static final int SOURCE_SIZE = 300;
    private static final long PRIMARY_TERM = 1L;

    private Path dir;
    private Translog translog;
    private final AtomicLong seqNoAllocator = new AtomicLong(0);

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory("translog-batch-bench");
        final ShardId shardId = new ShardId(new Index("bench-index", "bench-uuid"), 0);
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build();
        final IndexMetadata indexMetadata = IndexMetadata.builder("bench-index")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        final TranslogConfig config = new TranslogConfig(
            shardId,
            dir,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "bench-node",
            false
        );
        final TranslogDeletionPolicy deletionPolicy = new DefaultTranslogDeletionPolicy(-1, -1, 0);
        final String translogUUID = Translog.createEmptyTranslog(
            dir,
            org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            PRIMARY_TERM
        );
        translog = new LocalTranslog(
            config,
            translogUUID,
            deletionPolicy,
            () -> org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED,
            () -> PRIMARY_TERM,
            seqNo -> {},
            TranslogOperationHelper.DEFAULT,
            null
        );
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (translog != null) {
            translog.close();
        }
        // best-effort cleanup of the generation files
        try (java.util.stream.Stream<Path> files = Files.walk(dir)) {
            files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {}
            });
        }
    }

    /** Build a fresh batch of ops with a thread-unique, monotonically increasing seqNo range. */
    private List<Translog.Operation> nextOps() {
        final long base = seqNoAllocator.getAndAdd(batchSize);
        final List<Translog.Operation> ops = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            final long seqNo = base + i;
            final byte[] source = new byte[SOURCE_SIZE];
            // cheap deterministic fill; keeps the array from being a shared constant the JIT could hoist
            source[0] = (byte) seqNo;
            source[SOURCE_SIZE - 1] = (byte) (seqNo >>> 8);
            final String id = Long.toString(seqNo);
            ops.add(new Translog.Index(id, seqNo, PRIMARY_TERM, source));
        }
        return ops;
    }

    /** Add the batch one operation at a time (the current bulk path). */
    @Benchmark
    @OperationsPerInvocation(1)
    public Object singleAddLoop() throws IOException {
        final List<Translog.Operation> ops = nextOps();
        Translog.Location last = null;
        for (int i = 0; i < ops.size(); i++) {
            last = translog.add(ops.get(i));
        }
        return last;
    }

    /** Add the whole batch in one call (the proposed path). */
    @Benchmark
    @OperationsPerInvocation(1)
    public Object batchAdd() throws IOException {
        return translog.add(nextOps());
    }
}
