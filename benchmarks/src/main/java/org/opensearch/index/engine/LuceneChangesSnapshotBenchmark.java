/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.translog.Translog;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for reading history from LuceneChangesSnapshot.
 *
 * <p>The {@link Layout} parameter controls how the operations that get read are placed across docIDs and segments,
 * which is what determines whether a sequential read is possible at all:
 *
 * <ul>
 *   <li>{@code CONTIGUOUS} - an append-only history in one segment, read as one gapless ascending run of docIDs. This
 *       is the shape a sequential read is worth the most on.
 *   <li>{@code STRIDED} - ascending but non-adjacent docIDs, as produced by a seqNo range that covers only part of a
 *       segment's documents (for example an index with nested documents, whose child documents are excluded). The
 *       {@code stride} parameter sets how far apart the reads are. While consecutive reads still share a compression
 *       block a sequential read keeps paying off; once the stride exceeds a block's document count each read
 *       decompresses a block with nothing to amortize it against, and a per-document read is cheaper.
 *   <li>{@code INTERLEAVED} - segments flushed concurrently hold interleaved seqNo ranges, so reading in seqNo order
 *       alternates between leaves while each leaf's own docIDs ascend. {@code stride} sets the flush fan-out.
 * </ul>
 *
 * Run a single configuration with, for example:
 * <pre>
 * ./gradlew -p benchmarks run --args 'LuceneChangesSnapshotBenchmark -p layout=CONTIGUOUS -p docSizeBytes=256'
 * </pre>
 */
@Fork(value = 1, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class LuceneChangesSnapshotBenchmark {

    /**
     * How the operations that will be read are laid out across docIDs and segments. See the class javadoc.
     */
    public enum Layout {
        CONTIGUOUS,
        STRIDED,
        INTERLEAVED
    }

    /** Operations read per invocation. Every layout reads this many, only their placement differs. */
    @Param({ "20000" })
    private int numOps;

    /**
     * For {@code STRIDED}, how many docIDs apart consecutive reads are; for {@code INTERLEAVED}, how many segments the
     * seqNo range is interleaved across (the flush fan-out). {@code 1} makes both layouts contiguous.
     */
    @Param({ "2" })
    private int stride;

    @Param({ "CONTIGUOUS", "STRIDED", "INTERLEAVED" })
    private Layout layout;

    @Param({ "256", "2048" })
    private int docSizeBytes;

    @Param({ "BEST_SPEED", "BEST_COMPRESSION" })
    private String codecMode;

    private static final ShardId SHARD_ID = new ShardId("index", "_na_", 0);

    private Path indexPath;
    private Directory directory;
    private DirectoryReader reader;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        indexPath = Files.createTempDirectory("lucene-changes-snapshot-benchmark");
        // OpenSearch's HybridDirectory reads stored fields through NIOFS rather than mmap by default; see
        // IndexModule#INDEX_STORE_HYBRID_NIO_EXTENSIONS, which lists fdt and fdx.
        directory = new NIOFSDirectory(indexPath);
        buildIndex();
        reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(directory), SHARD_ID);
        verifyLayout();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        IOUtils.close(reader, directory);
        IOUtils.rm(indexPath);
    }

    /**
     * Drains the whole snapshot, the way the get-changes action does: the seqNo range query, the doc values reads and
     * the stored fields reads for every operation.
     */
    @Benchmark
    public void drainSnapshot(Blackhole bh) throws IOException {
        try (LuceneChangesSnapshot snapshot = newSnapshot()) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                bh.consume(op);
            }
        }
    }

    private LuceneChangesSnapshot newSnapshot() throws IOException {
        final Engine.Searcher searcher = new Engine.Searcher(
            "benchmark",
            reader,
            new BM25Similarity(),
            null,
            new UsageTrackingQueryCachingPolicy(),
            () -> {} // the reader outlives the snapshot and is closed by tearDown
        );
        return new LuceneChangesSnapshot(searcher, LuceneChangesSnapshot.DEFAULT_BATCH_SIZE, 0, numOps - 1, true, true);
    }

    private void buildIndex() throws IOException {
        final Lucene104Codec.Mode mode = Lucene104Codec.Mode.valueOf(codecMode);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Lucene104Codec(mode))
            // one segment per commit, so the layouts below are exactly what gets read
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setRAMBufferSizeMB(512);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            switch (layout) {
                case CONTIGUOUS:
                    // seqNo == docID, one segment
                    for (int i = 0; i < numOps; i++) {
                        writer.addDocument(newDocument(i));
                    }
                    break;
                case STRIDED:
                    // one segment; the range that gets read covers every stride-th docID, the rest hold seqNos above it
                    long filler = numOps;
                    for (int i = 0; i < numOps; i++) {
                        writer.addDocument(newDocument(i));
                        for (int gap = 1; gap < stride; gap++) {
                            writer.addDocument(newDocument(filler++));
                        }
                    }
                    break;
                case INTERLEAVED:
                    // segments with interleaved seqNo ranges, as concurrently flushed segments have
                    for (int segment = 0; segment < stride; segment++) {
                        for (int seqNo = segment; seqNo < numOps; seqNo += stride) {
                            writer.addDocument(newDocument(seqNo));
                        }
                        writer.commit();
                    }
                    break;
                default:
                    throw new AssertionError(layout);
            }
            writer.commit();
        }
    }

    private Document newDocument(long seqNo) {
        final Document doc = new Document();
        doc.add(new StoredField(IdFieldMapper.NAME, Uid.encodeId(Long.toString(seqNo))));
        doc.add(new StoredField(SourceFieldMapper.NAME, new BytesRef(source(seqNo))));
        doc.add(new LongPoint(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 1L));
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, 1L));
        return doc;
    }

    /**
     * A pseudo-random payload of the requested size. Random bytes would not compress at all and identical bytes would
     * compress perfectly, so this mixes a fixed pattern with random content to land somewhere realistic.
     */
    private byte[] source(long seqNo) {
        final byte[] bytes = new byte[docSizeBytes];
        final Random random = new Random(seqNo);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 8 == 0 ? random.nextInt(26) + 'a' : 'a' + (i % 26));
        }
        return bytes;
    }

    /**
     * Fails fast if the index does not have the shape the layout intends, so a run cannot quietly measure something
     * else. Checks that the expected operations are all present and readable, and that the docID sequence in read order
     * is gapless and ascending only for {@code CONTIGUOUS} - the property that decides whether a sequential read of
     * stored fields is possible. Deliberately asserts on the index rather than on the snapshot's internals, so this
     * benchmark also compiles and runs against a baseline checkout.
     */
    private void verifyLayout() throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        final TopDocs topDocs = searcher.search(
            LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, 0, numOps - 1),
            new TopFieldCollectorManager(new Sort(new SortField(SeqNoFieldMapper.NAME, SortField.Type.LONG)), numOps, null, 0, false)
        );
        if (topDocs.scoreDocs.length != numOps) {
            throw new IllegalStateException("expected " + numOps + " hits but got " + topDocs.scoreDocs.length);
        }
        boolean contiguous = true;
        for (int i = 0; i < topDocs.scoreDocs.length - 1; i++) {
            final ScoreDoc current = topDocs.scoreDocs[i];
            final ScoreDoc next = topDocs.scoreDocs[i + 1];
            if (current.doc + 1 != next.doc) {
                contiguous = false;
                break;
            }
        }
        if (contiguous != (layout == Layout.CONTIGUOUS)) {
            throw new IllegalStateException(
                "layout [" + layout + "] with stride [" + stride + "] produced a contiguous read order of [" + contiguous + "]"
            );
        }
        int read = 0;
        try (LuceneChangesSnapshot snapshot = newSnapshot()) {
            while (snapshot.next() != null) {
                read++;
            }
        }
        if (read != numOps) {
            throw new IllegalStateException("expected " + numOps + " operations but read " + read);
        }
    }
}
