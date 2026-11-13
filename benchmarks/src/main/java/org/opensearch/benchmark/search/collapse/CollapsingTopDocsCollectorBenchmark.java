/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.collapse;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
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
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmark for score-sorted field collapse. Compare {@code totalHitsThreshold=0}
 * (min competitive score enabled once the group heap is full) against
 * {@code Integer.MAX_VALUE} (always exhaustive, the previous collector behavior).
 *
 * <pre>
 * gradlew -p benchmarks run --args ' CollapsingTopDocsCollectorBenchmark'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CollapsingTopDocsCollectorBenchmark {

    @Param({ "100000" })
    public int numDocs;

    @Param({ "100" })
    public int numGroups;

    @Param({ "10" })
    public int topN;

    /**
     * 0 enables min-competitive-score pruning as soon as the group heap is full.
     * {@link Integer#MAX_VALUE} forces an accurate hit count and disables pruning.
     */
    @Param({ "0", "2147483647" })
    public int totalHitsThreshold;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Query query;
    private MappedFieldType fieldType;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("group", i % numGroups));
                int tf = (i % 32) + 1;
                for (int j = 0; j < tf; j++) {
                    doc.add(new TextField("text", "term", Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.commit();
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
        query = new TermQuery(new Term("text", "term"));
        fieldType = new NumberFieldMapper.NumberFieldType("group", NumberFieldMapper.NumberType.LONG);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    @Benchmark
    public void collapseByScore(Blackhole bh) throws IOException {
        CollapsingTopDocsCollector<?> collector = CollapsingTopDocsCollector.createNumeric(
            "group",
            fieldType,
            Sort.RELEVANCE,
            topN,
            totalHitsThreshold
        );
        searcher.search(query, collector);
        bh.consume(collector.getTopDocs());
    }
}
