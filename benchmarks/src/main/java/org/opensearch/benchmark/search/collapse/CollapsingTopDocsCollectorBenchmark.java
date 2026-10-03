/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.collapse;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterSortedDocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.KeywordFieldMapper;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Field-collapse collector microbenchmark. Keyword collapse caches
 * {@code SortedDocValues#lookupOrd} per segment ordinal; numeric collapse is the cheap
 * baseline (no terms dictionary).
 * <p>
 * {@code topN} is set above cardinality so the grouping heap never fills and every
 * matching doc goes through {@code GroupSelector.currentValue()}. For keyword mode,
 * setup fails the trial unless {@code lookupOrd} is called once per unique ordinal.
 *
 * <pre>
 * ./gradlew -p benchmarks run --args ' CollapsingTopDocsCollectorBenchmark'
 * </pre>
 */
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class CollapsingTopDocsCollectorBenchmark {

    @Param({ "16", "256", "4096" })
    public int cardinality;

    @Param({ "100000" })
    public int numDocs;

    @Param({ "keyword", "numeric" })
    public String collapseType;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private MappedFieldType fieldType;
    private Sort sort;
    private boolean keyword;
    private int topN;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        keyword = "keyword".equals(collapseType);
        // Heap never fills, so every hit calls currentValue() (the lookupOrd path).
        topN = cardinality + 1;
        directory = new ByteBuffersDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setRAMBufferSizeMB(256);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            Random random = new Random(42);
            BytesRef[] keywordValues = null;
            if (keyword) {
                keywordValues = new BytesRef[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    // UUID-length values, closer to real collapse keys than "g0"
                    keywordValues[i] = new BytesRef(String.format(Locale.ROOT, "collapse-key-%032d", i));
                }
            }
            Document doc = new Document();
            NumericDocValuesField sortField = new NumericDocValuesField("sort", 0);
            NumericDocValuesField numericGroup = new NumericDocValuesField("group", 0);
            SortedDocValuesField keywordGroup = new SortedDocValuesField("group", new BytesRef("placeholder"));
            doc.add(sortField);
            if (keyword) {
                doc.add(keywordGroup);
            } else {
                doc.add(numericGroup);
            }
            for (int i = 0; i < numDocs; i++) {
                int group = random.nextInt(cardinality);
                sortField.setLongValue(random.nextInt());
                if (keyword) {
                    keywordGroup.setBytesValue(keywordValues[group]);
                } else {
                    numericGroup.setLongValue(group);
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != 1) {
            throw new IllegalStateException("expected 1 segment after forceMerge, got " + reader.leaves().size());
        }
        searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        sort = new Sort(new SortField("sort", SortField.Type.INT));
        fieldType = keyword
            ? new KeywordFieldMapper.KeywordFieldType("group")
            : new NumberFieldMapper.NumberFieldType("group", NumberFieldMapper.NumberType.LONG);
        if (keyword) {
            verifyLookupOrdCounts();
        }
    }

    /**
     * Fail the trial unless keyword collapse resolves each ordinal once, not once per hit.
     */
    private void verifyLookupOrdCounts() throws IOException {
        AtomicInteger lookups = new AtomicInteger();
        DirectoryReader countingReader = new CountingLookupOrdDirectoryReader(reader, lookups, "group");
        IndexSearcher countingSearcher = new IndexSearcher(countingReader);
        countingSearcher.setQueryCache(null);
        CollapsingTopDocsCollector<?> collector = CollapsingTopDocsCollector.createKeyword("group", fieldType, sort, topN);
        countingSearcher.search(new MatchAllDocsQuery(), collector);
        CollapseTopFieldDocs topDocs = collector.getTopDocs();
        if (topDocs.scoreDocs.length != cardinality) {
            throw new IllegalStateException("expected " + cardinality + " collapsed hits, got " + topDocs.scoreDocs.length);
        }
        if (topDocs.totalHits.value() != numDocs) {
            throw new IllegalStateException("expected " + numDocs + " total hits, got " + topDocs.totalHits.value());
        }
        int actual = lookups.get();
        if (actual != cardinality) {
            throw new IllegalStateException(
                "lookupOrd calls=" + actual + " expected=" + cardinality + " (numDocs=" + numDocs + ", cardinality=" + cardinality + ")"
            );
        }
        // Do not close countingReader: FilterDirectoryReader.close() would close the inner reader.
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        reader.close();
        directory.close();
    }

    @Benchmark
    public CollapseTopFieldDocs collapse() {
        CollapsingTopDocsCollector<?> collector = keyword
            ? CollapsingTopDocsCollector.createKeyword("group", fieldType, sort, topN)
            : CollapsingTopDocsCollector.createNumeric("group", fieldType, sort, topN);
        try {
            searcher.search(new MatchAllDocsQuery(), collector);
            return collector.getTopDocs();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static final class CountingLookupOrdDirectoryReader extends FilterDirectoryReader {
        private final AtomicInteger lookupOrdCalls;
        private final String field;

        CountingLookupOrdDirectoryReader(DirectoryReader in, AtomicInteger lookupOrdCalls, String field) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        @Override
                        public SortedDocValues getSortedDocValues(String fieldName) throws IOException {
                            SortedDocValues dv = super.getSortedDocValues(fieldName);
                            if (dv == null || field.equals(fieldName) == false) {
                                return dv;
                            }
                            return new FilterSortedDocValues(dv) {
                                @Override
                                public BytesRef lookupOrd(int ord) throws IOException {
                                    lookupOrdCalls.incrementAndGet();
                                    return super.lookupOrd(ord);
                                }
                            };
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return in.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return in.getReaderCacheHelper();
                        }
                    };
                }
            });
            this.lookupOrdCalls = lookupOrdCalls;
            this.field = field;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new CountingLookupOrdDirectoryReader(in, lookupOrdCalls, field);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
