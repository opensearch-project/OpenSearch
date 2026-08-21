/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.common.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.ShardCoreKeyMap;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@link ShardCoreKeyMap#getShardId} under concurrency.
 *
 * The map is node-global (one per {@code IndicesQueryCache}) and this method is reached once per
 * (query, segment) on every query cache lookup, via {@code IndicesQueryCache.OpenSearchLRUQueryCache#onHit}
 * and {@code #onMiss}. Run this on {@code main} and on the branch that drops {@code synchronized} from
 * the getter to compare; the interesting signal is how the numbers diverge as thread count rises.
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class ShardCoreKeyMapBenchmark {

    /**
     * Segments per shard. Production nodes routinely sit in the high hundreds to low thousands. Every value is at
     * least the highest thread count exercised below (64), so no two threads can seed onto the same core key.
     */
    @Param({ "64", "1024" })
    private int segments;

    private ShardCoreKeyMap map;
    private Object[] coreKeys;
    private Directory directory;
    private DirectoryReader reader;

    /** Per-thread cursor so threads walk different core keys rather than hammering one entry. */
    @State(Scope.Thread)
    public static class Cursor {
        int index;

        /**
         * Start each thread on a different core key. Without this every thread would begin at
         * {@code coreKeys[0]} and advance in lockstep, so they would contend on one map bin and add
         * cache-line coherence traffic that has nothing to do with the lock under test.
         */
        @Setup
        public void setup(ThreadParams threadParams) {
            index = threadParams.getThreadIndex();
        }
    }

    @Setup
    public void setup() throws IOException {
        directory = new ByteBuffersDirectory();
        // One commit per document, so each document lands in its own segment.
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < segments; i++) {
                Document document = new Document();
                document.add(new StringField("field", "value" + i, Field.Store.NO));
                writer.addDocument(document);
                writer.commit();
            }
        }

        reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("index", "_na_", 0));
        map = new ShardCoreKeyMap();
        coreKeys = new Object[reader.leaves().size()];
        int i = 0;
        for (LeafReaderContext context : reader.leaves()) {
            map.add(context.reader());
            coreKeys[i++] = context.reader().getCoreCacheHelper().getKey();
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    private ShardId lookup(Cursor cursor) {
        return map.getShardId(coreKeys[Math.floorMod(cursor.index++, coreKeys.length)]);
    }

    @Benchmark
    @Threads(1)
    public ShardId getShardId_01(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(2)
    public ShardId getShardId_02(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(4)
    public ShardId getShardId_04(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(8)
    public ShardId getShardId_08(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(16)
    public ShardId getShardId_16(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(32)
    public ShardId getShardId_32(Cursor cursor) {
        return lookup(cursor);
    }

    @Benchmark
    @Threads(64)
    public ShardId getShardId_64(Cursor cursor) {
        return lookup(cursor);
    }
}
