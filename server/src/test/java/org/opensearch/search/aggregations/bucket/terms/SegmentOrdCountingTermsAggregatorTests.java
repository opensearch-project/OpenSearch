/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.TextParams;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.nested.InternalNested;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.internal.SearchContext;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
import static org.opensearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.opensearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for the segment-ordinal counting path of {@link GlobalOrdinalsStringTermsAggregator}: with dense global ordinals,
 * the aggregator counts segment ordinals while it collects a segment and adds the counts to the global doc counts once
 * per segment. Every test compares the result with a brute-force count and checks, through the debug info, how many
 * segments took that path.
 */
public class SegmentOrdCountingTermsAggregatorTests extends AggregatorTestCase {

    private static final String FIELD = "kw";
    private static final String MULTI_FIELD = "kw_multi";
    private static final String TEXT_FIELD = "text";
    private static final String FILTER_FIELD = "flt";
    private static final String NUM_FIELD = "num";
    private static final String NESTED_PATH = "nested_object";
    private static final String NESTED_FIELD = "nested_kw";

    private final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();

    @Before
    public void forceDenseGlobalOrds() {
        // Dense global ords (not remap) and the standard aggregator (not the low-cardinality one), whatever the
        // field's cardinality.
        TermsAggregatorFactory.REMAP_GLOBAL_ORDS = false;
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = false;
    }

    @After
    public void resetFactory() {
        TermsAggregatorFactory.REMAP_GLOBAL_ORDS = null;
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = null;
    }

    /**
     * Random multi-segment index with single- and multi-valued keyword fields, deleted docs, docs without a value, and
     * queries that collect by range, by bit set and doc by doc.
     */
    public void testMatchesBruteForceCounts() throws IOException {
        try (Directory directory = newDirectory()) {
            final List<Document> docs = new ArrayList<>();
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                final int numSegments = randomIntBetween(2, 8);
                final int vocabulary = randomIntBetween(50, 5_000);
                int id = 0;
                for (int s = 0; s < numSegments; s++) {
                    // Each segment draws from its own slice of the vocabulary, so segment and global ordinals differ.
                    final int lo = randomIntBetween(0, vocabulary - 1);
                    final int hi = randomIntBetween(lo, vocabulary - 1);
                    final int docsInSegment = randomIntBetween(1, 3_000);
                    for (int d = 0; d < docsInSegment; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField("id", Integer.toString(id++), Field.Store.NO));
                        doc.add(new StringField(FILTER_FIELD, randomFrom("a", "b"), Field.Store.NO));
                        doc.add(new IntPoint(NUM_FIELD, d));
                        if (randomInt(9) > 0) { // 10% of docs have no value
                            doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(lo, hi))));
                        }
                        for (int m = randomIntBetween(0, 3); m > 0; m--) {
                            doc.add(new SortedSetDocValuesField(MULTI_FIELD, term(randomIntBetween(lo, hi))));
                        }
                        writer.addDocument(doc);
                        docs.add(doc);
                    }
                    writer.commit();
                }
                // Deleted docs: live docs are applied during collection and weight.count() != maxDoc.
                for (int i = 0; i < docs.size() / 20; i++) {
                    writer.deleteDocuments(new Term("id", Integer.toString(randomIntBetween(0, docs.size() - 1))));
                }
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                for (Query query : List.of(
                    new MatchAllDocsQuery(),
                    new TermQuery(new Term(FILTER_FIELD, "a")),
                    IntPoint.newRangeQuery(NUM_FIELD, 100, 1_500)
                )) {
                    for (String field : new String[] { FIELD, MULTI_FIELD }) {
                        final Run run = run(reader, query, termsBuilder(field, 100_000), null, null, keyword(field));
                        assertThat(query + " " + field, run.counts(), equalTo(bruteForce(reader, query, field)));
                        // Segments where the query matches nothing are skipped before collection (weight.count(ctx) == 0).
                        assertThat(
                            run.countedSegments(),
                            equalTo(expectedCountedSegments(reader, field, null, query, matches(reader, query)))
                        );
                        assertThat(run.debug().get("segment_ord_counting_disabled_by_breaker"), equalTo(false));
                    }
                }
            }
        }
    }

    /** Fewer buckets than terms, ordered by count: the top buckets and the other doc count come from the merged counts. */
    public void testTopBucketsAndOtherDocCount() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                final int numSegments = randomIntBetween(2, 6);
                for (int s = 0; s < numSegments; s++) {
                    for (int d = randomIntBetween(100, 2_000); d > 0; d--) {
                        final Document doc = new Document();
                        // Skewed, so the top buckets are mostly decided by counts summed over several segments.
                        final int t = (int) Math.floor(Math.pow(randomDouble(), 3) * 400);
                        doc.add(new SortedSetDocValuesField(FIELD, term(t)));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final int size = randomIntBetween(1, 20);
                final TermsAggregationBuilder builder = new TermsAggregationBuilder("terms").field(FIELD)
                    .executionHint("global_ordinals")
                    .size(size)
                    .shardSize(size)
                    .order(BucketOrder.count(false));
                final Run run = run(reader, new MatchAllDocsQuery(), builder, null, null, keyword(FIELD));

                final List<Map.Entry<String, Long>> all = new ArrayList<>(bruteForce(reader, new MatchAllDocsQuery(), FIELD).entrySet());
                all.sort(
                    (a, b) -> a.getValue().equals(b.getValue())
                        ? a.getKey().compareTo(b.getKey())
                        : Long.compare(b.getValue(), a.getValue())
                );
                final Map<String, Long> expectedTop = new TreeMap<>();
                long other = 0;
                for (int i = 0; i < all.size(); i++) {
                    if (i < size) {
                        expectedTop.put(all.get(i).getKey(), all.get(i).getValue());
                    } else {
                        other += all.get(i).getValue();
                    }
                }
                assertThat(run.counts(), equalTo(expectedTop));
                assertThat(run.otherDocCount(), equalTo(other));
                assertThat(
                    run.countedSegments(),
                    equalTo(expectedCountedSegments(reader, FIELD, null, new MatchAllDocsQuery(), ctx -> true))
                );
            }
        }
    }

    /**
     * Sub-aggregations get every doc with its global ordinal while doc counts go through segment ordinals. Covers
     * breadth-first (deferred) sub-aggregations, the production case for dense global ords, and depth-first ones.
     */
    public void testWithSubAggregation() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                int id = 0;
                final int numSegments = randomIntBetween(2, 6);
                for (int s = 0; s < numSegments; s++) {
                    for (int d = randomIntBetween(1, 2_000); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new StringField("id", Integer.toString(id++), Field.Store.NO));
                        doc.add(new StringField(FILTER_FIELD, randomFrom("a", "b"), Field.Store.NO));
                        if (randomInt(9) > 0) {
                            doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(0, 300))));
                        }
                        for (int m = randomIntBetween(0, 3); m > 0; m--) {
                            doc.add(new SortedSetDocValuesField(MULTI_FIELD, term(randomIntBetween(0, 300))));
                        }
                        doc.add(new NumericDocValuesField(NUM_FIELD, randomLongBetween(-1_000, 1_000_000)));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                for (int i = 0; i < id / 20; i++) {
                    writer.deleteDocuments(new Term("id", Integer.toString(randomIntBetween(0, id - 1))));
                }
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                for (Query query : List.of(new MatchAllDocsQuery(), new TermQuery(new Term(FILTER_FIELD, "a")))) {
                    for (String field : new String[] { FIELD, MULTI_FIELD }) {
                        for (Aggregator.SubAggCollectionMode mode : Aggregator.SubAggCollectionMode.values()) {
                            final TermsAggregationBuilder builder = termsBuilder(field, 1_000).collectMode(mode)
                                .subAggregation(new MaxAggregationBuilder("max").field(NUM_FIELD));
                            final IndexSearcher searcher = newIndexSearcher(reader);
                            final GlobalOrdinalsStringTermsAggregator aggregator = createAggregator(
                                query,
                                builder,
                                searcher,
                                createIndexSettings(),
                                keyword(field),
                                new NumberFieldMapper.NumberFieldType(NUM_FIELD, NumberFieldMapper.NumberType.LONG)
                            );
                            aggregator.preCollection();
                            searcher.search(query, aggregator);
                            aggregator.postCollection();
                            final Terms result = (Terms) aggregator.buildTopLevel();
                            final Map<String, List<Long>> actual = new TreeMap<>();
                            for (Terms.Bucket bucket : result.getBuckets()) {
                                final Max max = bucket.getAggregations().get("max");
                                actual.put(bucket.getKeyAsString(), List.of(bucket.getDocCount(), (long) max.getValue()));
                            }
                            assertThat(query + " " + field + " " + mode, actual, equalTo(bruteForceWithMax(reader, query, field)));
                            final Map<String, Object> debug = debug(aggregator);
                            assertThat(debug.get("collection_strategy"), equalTo("dense"));
                            // With sub-aggregations no segment is skipped before collection.
                            assertThat(
                                debug.get("segments_with_segment_ord_counting"),
                                equalTo(expectedCountedSegments(reader, field, null, query, ctx -> true))
                            );
                        }
                    }
                }
            }
        }
    }

    /**
     * A {@code missing} value inserts a term into every segment's ordinals and shifts the segment-to-global mapping.
     * Each run covers the three cases at once: the missing term is in some segments and not others, so it exists in
     * the segment, only in other segments; and runs with a term that is nowhere.
     */
    public void testMissingValue() throws IOException {
        for (String missingTerm : new String[] { "t000000", "t000150", "zzz" }) { // smallest real term, a middle one, none
            try (Directory directory = newDirectory()) {
                final Map<String, Long> expected = new TreeMap<>();
                try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                    final int numSegments = randomIntBetween(3, 6);
                    for (int s = 0; s < numSegments; s++) {
                        // Segment 0 has the missing term (if it is a real term), segment 1 doesn't, the rest at random.
                        final boolean containsMissingTerm = s == 0 || (s > 1 && randomBoolean());
                        for (int d = randomIntBetween(10, 1_000); d > 0; d--) {
                            final Document doc = new Document();
                            if (d > 1 && randomInt(4) == 0) {
                                expected.merge(missingTerm, 1L, Long::sum); // no value: counted under the missing term
                            } else {
                                BytesRef value = term(randomIntBetween(0, 300));
                                if (containsMissingTerm && d == 1) {
                                    // The segment's last doc holds the missing term, so the segment really contains it.
                                    value = new BytesRef(missingTerm.equals("zzz") ? "t000301" : missingTerm);
                                } else if (containsMissingTerm == false && value.utf8ToString().equals(missingTerm)) {
                                    value = term(301);
                                }
                                doc.add(new SortedSetDocValuesField(FIELD, value));
                                expected.merge(value.utf8ToString(), 1L, Long::sum);
                            }
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                }
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    final Run run = run(
                        reader,
                        new MatchAllDocsQuery(),
                        termsBuilder(FIELD, 1_000).missing(missingTerm),
                        null,
                        null,
                        keyword(FIELD)
                    );
                    assertThat(missingTerm, run.counts(), equalTo(expected));
                    assertThat(
                        missingTerm,
                        run.countedSegments(),
                        equalTo(expectedCountedSegments(reader, FIELD, missingTerm, new MatchAllDocsQuery(), ctx -> true))
                    );
                }
            }
        }
    }

    /**
     * A text field with field data: ordinals come from {@code PagedBytesIndexFieldData}, where a doc's
     * {@code docValueCount()} drops as its ordinals are read. A {@code missing} value is what makes this source eligible.
     */
    public void testTextFieldWithFielddataAndMissing() throws IOException {
        final FieldType type = TextParams.buildFieldType(() -> true, () -> false, () -> "docs", () -> false, () -> "no");
        try (Directory directory = newDirectory()) {
            final Map<String, Long> expected = new TreeMap<>();
            final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer(CharArraySet.EMPTY_SET)).setMergePolicy(
                NoMergePolicy.INSTANCE
            );
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                final int numSegments = randomIntBetween(2, 5);
                for (int s = 0; s < numSegments; s++) {
                    final int lo = randomIntBetween(0, 100);
                    for (int d = randomIntBetween(1, 500); d > 0; d--) {
                        final Document doc = new Document();
                        final Set<String> tokens = new TreeSet<>();
                        for (int t = randomIntBetween(0, 5); t > 0; t--) {
                            tokens.add(term(randomIntBetween(lo, lo + 200)).utf8ToString());
                        }
                        if (tokens.isEmpty()) {
                            expected.merge("missing_value", 1L, Long::sum);
                        } else {
                            doc.add(new Field(TEXT_FIELD, String.join(" ", tokens), type));
                            tokens.forEach(token -> expected.merge(token, 1L, Long::sum));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final TextFieldMapper.TextFieldType fieldType = new TextFieldMapper.TextFieldType(TEXT_FIELD);
                fieldType.setFielddata(true);
                final TermsAggregationBuilder builder = termsBuilder(TEXT_FIELD, 10_000).userValueTypeHint(ValueType.STRING)
                    .missing("missing_value");
                final Run run = run(reader, new MatchAllDocsQuery(), builder, null, null, fieldType);
                assertThat(run.counts(), equalTo(expected));
                assertThat(
                    run.countedSegments(),
                    equalTo(expectedCountedSegments(reader, TEXT_FIELD, "missing_value", new MatchAllDocsQuery(), ctx -> true))
                );
                // Without missing, text field data reports that it can't map segment to global ordinals: original path.
                final TermsAggregationBuilder noMissing = termsBuilder(TEXT_FIELD, 10_000).userValueTypeHint(ValueType.STRING);
                final Run original = run(reader, new MatchAllDocsQuery(), noMissing, null, null, fieldType);
                expected.remove("missing_value");
                assertThat(original.counts(), equalTo(expected));
                assertThat(original.countedSegments(), equalTo(0));
            }
        }
    }

    /** Segments with a {@code _doc_count} field use the original path; the others count segment ordinals. */
    public void testDocCountFieldSegmentsUseOriginalPath() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                final int numSegments = randomIntBetween(2, 6);
                for (int s = 0; s < numSegments; s++) {
                    // Segment 0 has the field and segment 1 doesn't, so every run mixes both paths.
                    final boolean withDocCount = s == 0 || (s > 1 && randomBoolean());
                    for (int d = randomIntBetween(1, 500); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(0, 200))));
                        if (withDocCount && (d == 1 || randomBoolean())) {
                            doc.add(new NumericDocValuesField(DocCountFieldMapper.NAME, randomIntBetween(1, 10)));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final Run run = run(reader, new MatchAllDocsQuery(), termsBuilder(FIELD, 100_000), null, null, keyword(FIELD));
                assertThat(run.counts(), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                final int expected = expectedCountedSegments(
                    reader,
                    FIELD,
                    null,
                    new MatchAllDocsQuery(),
                    ctx -> ctx.reader().getFieldInfos().fieldInfo(DocCountFieldMapper.NAME) == null
                );
                assertThat(run.countedSegments(), equalTo(expected));
                assertThat(expected, lessThan(reader.leaves().size()));
            }
        }
    }

    /**
     * A segment with every term has the global ordinals as its own, so there is no mapping to save. The aggregator still
     * counts it by segment ordinal when the query is known to match many docs, but not for a selective query, which would
     * mostly pay for counts sized to every global ordinal. A segment that lacks terms is always counted.
     */
    public void testSegmentWithEveryTerm() throws IOException {
        final int numTerms = randomIntBetween(64, 2_000);
        final Query rare = new TermQuery(new Term(FILTER_FIELD, "rare"));
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                for (int i = 0; i < numTerms; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedSetDocValuesField(FIELD, term(i)));
                    doc.add(new StringField(FILTER_FIELD, i % 16 == 0 ? "rare" : "common", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    final Run all = run(reader, new MatchAllDocsQuery(), termsBuilder(FIELD, 100_000), null, null, keyword(FIELD));
                    assertThat(all.counts(), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                    assertThat(all.countedSegments(), equalTo(1));
                    final Run selective = run(reader, rare, termsBuilder(FIELD, 100_000), null, null, keyword(FIELD));
                    assertThat(selective.counts(), equalTo(bruteForce(reader, rare, FIELD)));
                    assertThat(selective.countedSegments(), equalTo(0));
                }
                for (int i = 0; i < numTerms; i += 3) {
                    final Document doc = new Document();
                    doc.add(new SortedSetDocValuesField(FIELD, term(i)));
                    doc.add(new StringField(FILTER_FIELD, "rare", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final Run selective = run(reader, rare, termsBuilder(FIELD, 100_000), null, null, keyword(FIELD));
                assertThat(selective.counts(), equalTo(bruteForce(reader, rare, FIELD)));
                assertThat(selective.countedSegments(), equalTo(1));
            }
        }
    }

    /**
     * The gate for a segment with every term: no weight (terms under a filter aggregation), a count the weight can't
     * compute (a segment with deletions), and the boundary of a quarter as many matching docs as ordinals.
     */
    public void testSegmentWithEveryTermGate() throws IOException {
        final int numTerms = 4 * randomIntBetween(16, 500);
        final int quarter = numTerms / 4;
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                for (int i = 0; i < numTerms; i++) {
                    final Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                    doc.add(new SortedSetDocValuesField(FIELD, term(i)));
                    doc.add(new StringField(FILTER_FIELD, i < quarter ? "quarter" : "rest", Field.Store.NO));
                    doc.add(new StringField("almost", i < quarter - 1 ? "yes" : "no", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    final Query quarterQuery = new TermQuery(new Term(FILTER_FIELD, "quarter"));
                    final Query almostQuery = new TermQuery(new Term("almost", "yes"));
                    assertThat(
                        run(reader, quarterQuery, termsBuilder(FIELD, 100_000), null, null, keyword(FIELD)).countedSegments(),
                        equalTo(1)
                    );
                    assertThat(
                        run(reader, almostQuery, termsBuilder(FIELD, 100_000), null, null, keyword(FIELD)).countedSegments(),
                        equalTo(0)
                    );

                    // A sub-aggregation of a filter aggregation gets no weight, so it can't know how many docs match.
                    final IndexSearcher searcher = newIndexSearcher(reader);
                    final FilterAggregationBuilder filter = new FilterAggregationBuilder("f", QueryBuilders.matchAllQuery()).subAggregation(
                        termsBuilder(FIELD, 100_000)
                    );
                    final Aggregator filterAgg = createAggregator(
                        new MatchAllDocsQuery(),
                        filter,
                        searcher,
                        createIndexSettings(),
                        keyword(FIELD)
                    );
                    filterAgg.preCollection();
                    searcher.search(new MatchAllDocsQuery(), filterAgg);
                    filterAgg.postCollection();
                    final InternalFilter result = (InternalFilter) filterAgg.buildTopLevel();
                    assertThat(toMap(result.getAggregations().get("terms")), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                    assertThat(debug(filterAgg.subAggregator("terms")).get("segments_with_segment_ord_counting"), equalTo(0));
                }
                // With a deleted doc the term query's count is unknown (-1).
                writer.deleteDocuments(new Term("id", "0"));
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().get(0).reader().hasDeletions(), equalTo(true));
                final Query rest = new TermQuery(new Term(FILTER_FIELD, "rest"));
                final Run run = run(reader, rest, termsBuilder(FIELD, 100_000), null, null, keyword(FIELD));
                assertThat(run.counts(), equalTo(bruteForce(reader, rest, FIELD)));
                assertThat(run.countedSegments(), equalTo(0));
            }
        }
    }

    /**
     * Segments answered from term frequencies add to the doc counts without calling the bucket consumer. The pending
     * segment is flushed before that, so its buckets become non-empty first, as on the original path.
     */
    public void testFlushesBeforeTermFrequencyPrecompute() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                // Segment 0 has a deletion, so it is collected (by segment ordinal); segment 1 is answered from term
                // frequencies. They share t050..t099.
                for (int[] range : new int[][] { { 0, 100 }, { 50, 150 } }) {
                    for (int i = range[0]; i < range[1]; i++) {
                        final Document doc = new Document();
                        doc.add(new StringField("id", range[0] + "-" + i, Field.Store.NO));
                        doc.add(new StringField(FIELD, term(i), Field.Store.NO));
                        doc.add(new SortedSetDocValuesField(FIELD, term(i)));
                        writer.addDocument(doc);
                    }
                    if (range[0] == 0) {
                        writer.deleteDocuments(new Term("id", "0-0"));
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final LongAdder calls = new LongAdder();
                final MultiBucketConsumer consumer = new MultiBucketConsumer(
                    MultiBucketConsumerService.DEFAULT_MAX_BUCKETS,
                    new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                    calls,
                    false,
                    1
                ) {
                };
                final Run run = run(reader, new MatchAllDocsQuery(), termsBuilder(FIELD, 100_000), null, consumer, keyword(FIELD));
                assertThat(run.counts(), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                assertThat(run.countedSegments(), equalTo(1));
                // Segment 0's 99 live terms, including the 50 that segment 1 also has.
                assertThat(calls.sum(), equalTo(99L));
            }
        }
    }

    /** LowCardinality counts segment ordinals too, and reports it. */
    public void testLowCardinalityReportsSegmentOrdCounting() throws IOException {
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = true;
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                for (int s = randomIntBetween(1, 4); s > 0; s--) {
                    for (int d = randomIntBetween(1, 500); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(0, 50))));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final Aggregator aggregator = createAggregator(
                    new MatchAllDocsQuery(),
                    termsBuilder(FIELD, 1_000),
                    searcher,
                    createIndexSettings(),
                    keyword(FIELD)
                );
                assertThat(aggregator, instanceOf(GlobalOrdinalsStringTermsAggregator.LowCardinality.class));
                aggregator.preCollection();
                searcher.search(new MatchAllDocsQuery(), aggregator);
                aggregator.postCollection();
                assertThat(toMap((Terms) aggregator.buildTopLevel()), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                assertThat(debug(aggregator).get("segments_with_segment_ord_counting"), equalTo(reader.leaves().size()));
            }
        }
    }

    /**
     * The counts are released after collection; collecting again allocates them again, as when a streaming request
     * collects the next batch of segments after building the previous one.
     */
    public void testCollectsAgainAfterPostCollection() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                for (int s = randomIntBetween(2, 4); s > 0; s--) {
                    final int lo = randomIntBetween(0, 250);
                    for (int d = randomIntBetween(1, 1_000); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(lo, lo + 250))));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final GlobalOrdinalsStringTermsAggregator aggregator = createAggregator(
                    new MatchAllDocsQuery(),
                    termsBuilder(FIELD, 100_000),
                    newIndexSearcher(reader),
                    createIndexSettings(),
                    keyword(FIELD)
                );
                aggregator.preCollection();
                for (LeafReaderContext ctx : reader.leaves()) {
                    final LeafBucketCollector leaf = aggregator.getLeafCollector(ctx);
                    for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                        leaf.collect(doc);
                    }
                    aggregator.postCollection();
                }
                assertThat(toMap((Terms) aggregator.buildTopLevel()), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                assertThat(
                    debug(aggregator).get("segments_with_segment_ord_counting"),
                    equalTo(expectedCountedSegments(reader, FIELD, null, null, ctx -> true))
                );
            }
        }
    }

    /** Include/exclude keeps the original path, which filters each global ordinal as it collects. */
    public void testIncludeExcludeUsesOriginalPath() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                for (int s = randomIntBetween(2, 4); s > 0; s--) {
                    for (int d = randomIntBetween(10, 1_000); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(0, 300))));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final TermsAggregationBuilder builder = termsBuilder(FIELD, 100_000).includeExclude(new IncludeExclude("t0001.*", null));
                final Run run = run(reader, new MatchAllDocsQuery(), builder, null, null, keyword(FIELD));
                final Map<String, Long> expected = new TreeMap<>(bruteForce(reader, new MatchAllDocsQuery(), FIELD));
                expected.keySet().removeIf(key -> key.startsWith("t0001") == false);
                assertThat(run.counts(), equalTo(expected));
                assertThat(run.countedSegments(), equalTo(0));
            }
        }
    }

    /** terms under a nested aggregation: a single-bucket parent, so dense global ordinals on the nested docs. */
    public void testUnderNestedAggregation() throws IOException {
        try (Directory directory = newDirectory()) {
            final Map<String, Long> expected = new TreeMap<>();
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                int id = 0;
                final int numSegments = randomIntBetween(2, 5);
                for (int s = 0; s < numSegments; s++) {
                    for (int p = randomIntBetween(1, 200); p > 0; p--) {
                        final List<Document> block = new ArrayList<>();
                        final BytesRef uid = Uid.encodeId(Integer.toString(id++));
                        for (int c = randomIntBetween(0, 5); c > 0; c--) {
                            final Document child = new Document();
                            child.add(new Field(IdFieldMapper.NAME, uid, IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                            child.add(new Field(NestedPathFieldMapper.NAME, NESTED_PATH, NestedPathFieldMapper.Defaults.FIELD_TYPE));
                            final BytesRef value = term(randomIntBetween(0, 300));
                            child.add(new SortedSetDocValuesField(NESTED_FIELD, value));
                            expected.merge(value.utf8ToString(), 1L, Long::sum);
                            block.add(child);
                        }
                        final Document parent = new Document();
                        parent.add(new Field(IdFieldMapper.NAME, uid, IdFieldMapper.Defaults.FIELD_TYPE));
                        parent.add(sequenceIDFields.primaryTerm);
                        block.add(parent);
                        writer.addDocuments(block);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                final NestedAggregationBuilder builder = new NestedAggregationBuilder("nested", NESTED_PATH).subAggregation(
                    termsBuilder(NESTED_FIELD, 1_000)
                );
                final IndexSearcher searcher = newIndexSearcher(reader);
                final Query query = new FieldExistsQuery(PRIMARY_TERM_NAME); // root docs only
                final Aggregator nested = createAggregator(query, builder, searcher, createIndexSettings(), keyword(NESTED_FIELD));
                nested.preCollection();
                searcher.search(query, nested);
                nested.postCollection();
                final InternalNested result = (InternalNested) nested.buildTopLevel();
                assertThat(toMap(result.getAggregations().get("terms")), equalTo(expected));
                assertThat(
                    debug(nested.subAggregator("terms")).get("segments_with_segment_ord_counting"),
                    equalTo(expectedCountedSegments(reader, NESTED_FIELD, null, null, ctx -> true))
                );
            }
        }
    }

    /**
     * Each segment is searched as two partitions, so the aggregator gets two leaf collectors for the same segment, as
     * with intra-segment search. Counts from both partitions must add up.
     */
    public void testSameSegmentInSeveralPartitions() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                for (int s = randomIntBetween(2, 4); s > 0; s--) {
                    final int lo = randomIntBetween(0, 250);
                    for (int d = randomIntBetween(2, 2_000); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(lo, lo + 250))));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final PartitioningSearcher searcher = new PartitioningSearcher(reader);
                final Query query = new MatchAllDocsQuery();
                final GlobalOrdinalsStringTermsAggregator aggregator = createAggregator(
                    query,
                    termsBuilder(FIELD, 1_000),
                    searcher,
                    createIndexSettings(),
                    keyword(FIELD)
                );
                aggregator.preCollection();
                searcher.searchInTwoPartitionsPerSegment(query, aggregator);
                aggregator.postCollection();
                assertThat(toMap((Terms) aggregator.buildTopLevel()), equalTo(bruteForce(reader, query, FIELD)));
                assertThat(
                    debug(aggregator).get("segments_with_segment_ord_counting"),
                    equalTo(2 * expectedCountedSegments(reader, FIELD, null, query, ctx -> true))
                );
            }
        }
    }

    /**
     * The bucket consumer sees one call per bucket that goes from empty to non-empty, as with the original path, and
     * checks the breaker at least once every 1024 calls.
     */
    public void testBucketConsumerSeesEveryNewBucket() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                final int numSegments = randomIntBetween(2, 4);
                for (int s = 0; s <= numSegments; s++) {
                    final int lo = randomIntBetween(0, 2_000);
                    for (int d = randomIntBetween(2_000, 6_000); d > 0; d--) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(randomIntBetween(lo, lo + 5_000))));
                        if (s == numSegments) {
                            // One segment takes the original path, which reports its new buckets one at a time.
                            doc.add(new NumericDocValuesField(DocCountFieldMapper.NAME, 1));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                final LongAdder calls = new LongAdder();
                final AtomicInteger breakerChecks = new AtomicInteger();
                final CircuitBreaker countingBreaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
                    @Override
                    public double addEstimateBytesAndMaybeBreak(long bytes, String label) {
                        if (bytes == 0 && "allocated_buckets".equals(label)) {
                            breakerChecks.incrementAndGet();
                        }
                        return 0;
                    }
                };
                final AtomicInteger largestBatch = new AtomicInteger();
                final MultiBucketConsumer consumer = new MultiBucketConsumer(
                    MultiBucketConsumerService.DEFAULT_MAX_BUCKETS,
                    countingBreaker,
                    calls,
                    false,
                    1
                ) {
                    @Override
                    public void acceptZeros(int count) {
                        largestBatch.accumulateAndGet(count, Math::max);
                        super.acceptZeros(count);
                    }
                };
                final Run run = run(reader, new MatchAllDocsQuery(), termsBuilder(FIELD, 100_000), null, consumer, keyword(FIELD));
                assertThat(run.counts(), equalTo(bruteForce(reader, new MatchAllDocsQuery(), FIELD)));
                assertThat(run.countedSegments(), greaterThan(0));
                // One call per bucket that went from empty to non-empty, whichever path counted it.
                assertThat(calls.sum(), equalTo((long) run.counts().size()));
                assertThat((long) breakerChecks.get(), greaterThanOrEqualTo(calls.sum() / 1024));
                assertThat(largestBatch.get(), lessThanOrEqualTo(1024));
                assertThat(run.countedSegments(), lessThan(reader.leaves().size())); // the _doc_count segment didn't
            }
        }
    }

    /**
     * Segment A is counted. Then the request breaker is lowered so segment B's counts don't fit: the aggregator adds A's
     * counts, releases the arrays and uses the original path for B and for C, without failing the request or tripping
     * the breaker. A control run without the lower limit counts all three segments and releases the counts after
     * collection.
     */
    public void testFallsBackWhenBreakerWouldTrip() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                // A: 4,000 terms of its own; B: 50,000 terms; C: 100 of B's terms. No segment has every term.
                for (int[] range : new int[][] { { 50_000, 54_000 }, { 0, 50_000 }, { 0, 100 } }) {
                    for (int i = range[0]; i < range[1]; i++) {
                        final Document doc = new Document();
                        doc.add(new SortedSetDocValuesField(FIELD, term(i)));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(3));
                final Map<String, Long> expected = bruteForce(reader, new MatchAllDocsQuery(), FIELD);

                final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                final HierarchyCircuitBreakerService breakers = breakers(clusterSettings, 100L << 20);
                final CircuitBreaker request = breakers.getBreaker(CircuitBreaker.REQUEST);
                final long[] usedBeforeLeaf = new long[3];
                final Run limited = runWithLeafHook(reader, new MatchAllDocsQuery(), termsBuilder(FIELD, 100_000), breakers, null, ctx -> {
                    usedBeforeLeaf[ctx.ord] = request.getUsed();
                    if (ctx.ord == 1) {
                        // Room for anything but B's segment counts (about 230 KB).
                        clusterSettings.applySettings(limitSettings(request.getUsed() + (64 << 10)));
                    }
                }, keyword(FIELD));
                assertThat(limited.counts(), equalTo(expected));
                assertThat(limited.countedSegments(), equalTo(1));
                assertThat(limited.debug().get("segment_ord_counting_disabled_by_breaker"), equalTo(true));
                assertThat(breakers.stats(CircuitBreaker.REQUEST).getTrippedCount(), equalTo(0L));
                // A's counts were released when B fell back.
                assertThat(usedBeforeLeaf[2], lessThan(usedBeforeLeaf[1]));

                final HierarchyCircuitBreakerService roomy = breakers(
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    100L << 20
                );
                final Run unlimited = run(reader, new MatchAllDocsQuery(), termsBuilder(FIELD, 100_000), roomy, null, keyword(FIELD));
                assertThat(unlimited.counts(), equalTo(expected));
                assertThat(unlimited.countedSegments(), equalTo(3));
                assertThat(unlimited.usedAfterPostCollection(), lessThan(unlimited.usedBeforePostCollection()));
            }
        }
    }

    /**
     * Lowers the request breaker before segment B to margins all around what B's counts need, with overhead 1 or 2: the
     * pre-check must never let BigArrays trip the breaker, and results are right on both sides of the boundary. Segment A
     * is smaller than a page (B replaces its arrays) or larger (B grows them in place).
     */
    public void testBreakerLimitAroundTheBoundaryNeverTrips() throws IOException {
        for (int aTerms : new int[] { 4_000, 10_000 }) {
            try (Directory directory = newDirectory()) {
                try (IndexWriter writer = new IndexWriter(directory, writerConfig())) {
                    for (int[] range : new int[][] { { 100_000, 100_000 + aTerms }, { 0, 50_000 }, { 0, 100 } }) {
                        for (int i = range[0]; i < range[1]; i++) {
                            final Document doc = new Document();
                            doc.add(new SortedSetDocValuesField(FIELD, term(i)));
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                }
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    final Map<String, Long> expected = bruteForce(reader, new MatchAllDocsQuery(), FIELD);
                    final double overhead = randomBoolean() ? 1.0 : 2.0;
                    boolean sawFallback = false;
                    boolean sawCounting = false;
                    for (long margin = 32 << 10; margin <= 320 << 10; margin += randomIntBetween(1, 8) << 10) {
                        final ClusterSettings clusterSettings = new ClusterSettings(
                            Settings.EMPTY,
                            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
                        );
                        final HierarchyCircuitBreakerService breakers = breakers(clusterSettings, 100L << 20, overhead);
                        final CircuitBreaker request = breakers.getBreaker(CircuitBreaker.REQUEST);
                        final long m = margin;
                        final Run run = runWithLeafHook(
                            reader,
                            new MatchAllDocsQuery(),
                            termsBuilder(FIELD, 100_000),
                            breakers,
                            null,
                            ctx -> {
                                if (ctx.ord == 1) {
                                    clusterSettings.applySettings(
                                        Settings.builder()
                                            .put(limitSettings((long) ((request.getUsed() + m) * overhead)))
                                            .put(REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), overhead)
                                            .build()
                                    );
                                }
                            },
                            keyword(FIELD)
                        );
                        assertThat(aTerms + " margin " + m, run.counts(), equalTo(expected));
                        assertThat(aTerms + " margin " + m, breakers.stats(CircuitBreaker.REQUEST).getTrippedCount(), equalTo(0L));
                        if ((boolean) run.debug().get("segment_ord_counting_disabled_by_breaker")) {
                            sawFallback = true;
                        } else {
                            sawCounting = true;
                        }
                    }
                    assertThat("the sweep should cross the boundary", sawFallback && sawCounting, equalTo(true));
                }
            }
        }
    }

    /** Result of one aggregation run on the terms aggregator. */
    private record Run(Map<String, Long> counts, long otherDocCount, Map<String, Object> debug, long usedBeforePostCollection,
        long usedAfterPostCollection) {

        int countedSegments() {
            return (Integer) debug.get("segments_with_segment_ord_counting");
        }
    }

    private Run run(
        IndexReader reader,
        Query query,
        AggregationBuilder builder,
        CircuitBreakerService breakers,
        MultiBucketConsumer consumer,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return runWithLeafHook(reader, query, builder, breakers, consumer, null, fieldTypes);
    }

    private Run runWithLeafHook(
        IndexReader reader,
        Query query,
        AggregationBuilder builder,
        CircuitBreakerService breakers,
        MultiBucketConsumer consumer,
        Consumer<LeafReaderContext> beforeLeaf,
        MappedFieldType... fieldTypes
    ) throws IOException {
        final LeafHookSearcher searcher = new LeafHookSearcher(reader, beforeLeaf);
        final CircuitBreakerService breakerService = breakers == null ? new NoneCircuitBreakerService() : breakers;
        final MultiBucketConsumer bucketConsumer = consumer != null
            ? consumer
            : new MultiBucketConsumer(MultiBucketConsumerService.DEFAULT_MAX_BUCKETS, new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        final SearchContext searchContext = createSearchContext(
            searcher,
            createIndexSettings(),
            query,
            bucketConsumer,
            breakerService,
            fieldTypes
        );
        final GlobalOrdinalsStringTermsAggregator aggregator = createAggregator(builder, searchContext);
        final CircuitBreaker request = breakerService.getBreaker(CircuitBreaker.REQUEST);
        aggregator.preCollection();
        searcher.search(query, aggregator);
        final long usedBefore = request.getUsed();
        aggregator.postCollection();
        final long usedAfter = request.getUsed();
        final InternalTerms<?, ?> result = (InternalTerms<?, ?>) aggregator.buildTopLevel();
        final Map<String, Object> debug = debug(aggregator);
        assertThat(debug.get("collection_strategy"), equalTo("dense"));
        return new Run(toMap(result), result.getSumOfOtherDocCounts(), debug, usedBefore, usedAfter);
    }

    private static Map<String, Object> debug(Aggregator aggregator) {
        final Map<String, Object> debug = new HashMap<>();
        aggregator.collectDebugInfo(debug::put);
        return debug;
    }

    private static TermsAggregationBuilder termsBuilder(String field, int size) {
        return new TermsAggregationBuilder("terms").field(field)
            .executionHint("global_ordinals")
            .size(size)
            .shardSize(size)
            .order(BucketOrder.key(true));
    }

    private static MappedFieldType keyword(String field) {
        return new KeywordFieldMapper.KeywordFieldType(field);
    }

    private static BytesRef term(int i) {
        return new BytesRef(String.format(Locale.ROOT, "t%06d", i));
    }

    private static Map<String, Long> toMap(Terms terms) {
        final Map<String, Long> out = new TreeMap<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            out.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        return out;
    }

    /** Leaves where the query matches at least one doc, or may (the count is unknown). */
    private static Predicate<LeafReaderContext> matches(IndexReader reader, Query query) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        return ctx -> {
            try {
                return weight.count(ctx) != 0;
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };
    }

    /**
     * Number of leaves the aggregator should count by segment ordinal, of those that reach collection ({@code visited}):
     * every leaf that lacks at least one term of the index, and a leaf with every term (its ordinals are the global
     * ordinals) only if the aggregator's query, {@code null} when it has none, is known to match at least a quarter as
     * many docs as the leaf has ordinals.
     */
    private static int expectedCountedSegments(
        IndexReader reader,
        String field,
        String missing,
        Query query,
        Predicate<LeafReaderContext> visited
    ) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Weight weight = query == null ? null : searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        final Set<String> globalTerms = new TreeSet<>();
        final List<Set<String>> segmentTerms = new ArrayList<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            final Set<String> terms = new TreeSet<>();
            final org.apache.lucene.index.Terms indexed = ctx.reader().terms(field);
            if (ctx.reader().getFieldInfos().fieldInfo(field) != null
                && ctx.reader().getFieldInfos().fieldInfo(field).getDocValuesType() == org.apache.lucene.index.DocValuesType.NONE
                && indexed != null) {
                // Text field data: the ordinals are the indexed terms.
                final org.apache.lucene.index.TermsEnum termsEnum = indexed.iterator();
                for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                    terms.add(term.utf8ToString());
                }
            } else {
                final SortedSetDocValues values = DocValues.getSortedSet(ctx.reader(), field);
                for (long ord = 0; ord < values.getValueCount(); ord++) {
                    terms.add(values.lookupOrd(ord).utf8ToString());
                }
            }
            if (missing != null) {
                terms.add(missing);
            }
            globalTerms.addAll(terms);
            segmentTerms.add(terms);
        }
        int counted = 0;
        for (LeafReaderContext ctx : reader.leaves()) {
            if (visited.test(ctx) == false) {
                continue;
            }
            final int segmentValueCount = segmentTerms.get(ctx.ord).size();
            if (segmentValueCount < globalTerms.size()) {
                counted++;
            } else if (weight != null && weight.count(ctx) >= 0 && weight.count(ctx) >= segmentValueCount / 4) {
                counted++;
            }
        }
        return counted;
    }

    /** Counts per term over live docs matching the query; a doc's {@code _doc_count} (default 1) is its weight. */
    private static Map<String, Long> bruteForce(IndexReader reader, Query query, String field) throws IOException {
        final Map<String, Long> out = new TreeMap<>();
        forEachMatch(reader, query, field, (values, doc, weightOfDoc) -> {
            for (int i = values.docValueCount(); i > 0; i--) {
                out.merge(values.lookupOrd(values.nextOrd()).utf8ToString(), weightOfDoc, Long::sum);
            }
        });
        return out;
    }

    /** Doc count and max of {@code NUM_FIELD} per term, over live docs matching the query. */
    private static Map<String, List<Long>> bruteForceWithMax(IndexReader reader, Query query, String field) throws IOException {
        final Map<String, long[]> acc = new TreeMap<>();
        final Map<LeafReaderContext, NumericDocValues> nums = new HashMap<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            nums.put(ctx, DocValues.getNumeric(ctx.reader(), NUM_FIELD));
        }
        forEachMatch(reader, query, field, new MatchConsumer() {
            @Override
            public void accept(SortedSetDocValues values, int doc, long weightOfDoc) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void accept(LeafReaderContext ctx, SortedSetDocValues values, int doc, long weightOfDoc) throws IOException {
                final NumericDocValues num = nums.get(ctx);
                final long value = num.advanceExact(doc) ? num.longValue() : Long.MIN_VALUE;
                for (int i = values.docValueCount(); i > 0; i--) {
                    final long[] entry = acc.computeIfAbsent(
                        values.lookupOrd(values.nextOrd()).utf8ToString(),
                        k -> new long[] { 0, Long.MIN_VALUE }
                    );
                    entry[0] += weightOfDoc;
                    entry[1] = Math.max(entry[1], value);
                }
            }
        });
        final Map<String, List<Long>> out = new TreeMap<>();
        acc.forEach((k, v) -> out.put(k, List.of(v[0], v[1])));
        return out;
    }

    private interface MatchConsumer {
        void accept(SortedSetDocValues values, int doc, long weightOfDoc) throws IOException;

        default void accept(LeafReaderContext ctx, SortedSetDocValues values, int doc, long weightOfDoc) throws IOException {
            accept(values, doc, weightOfDoc);
        }
    }

    /** Calls the consumer for every live doc that matches the query and has a value, positioned on that doc. */
    private static void forEachMatch(IndexReader reader, Query query, String field, MatchConsumer consumer) throws IOException {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
        for (LeafReaderContext ctx : reader.leaves()) {
            final Scorer scorer = weight.scorer(ctx);
            if (scorer == null) {
                continue;
            }
            final SortedSetDocValues values = DocValues.getSortedSet(ctx.reader(), field);
            final NumericDocValues docCounts = ctx.reader().getNumericDocValues(DocCountFieldMapper.NAME);
            final Bits live = ctx.reader().getLiveDocs();
            final DocIdSetIterator it = scorer.iterator();
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                if ((live == null || live.get(doc)) && values.advanceExact(doc)) {
                    final long weightOfDoc = docCounts != null && docCounts.advanceExact(doc) ? docCounts.longValue() : 1L;
                    consumer.accept(ctx, values, doc, weightOfDoc);
                }
            }
        }
    }

    /**
     * Random codec, but segments only where the test commits: with tiny random flush sizes and no merges an index can
     * get thousands of segments, and without a field data cache every global ordinals lookup rebuilds the ordinal map.
     */
    private static IndexWriterConfig writerConfig() {
        return newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(256);
    }

    private static Settings limitSettings(long requestLimitBytes) {
        return Settings.builder().put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestLimitBytes, ByteSizeUnit.BYTES).build();
    }

    private static HierarchyCircuitBreakerService breakers(ClusterSettings clusterSettings, long requestLimitBytes) {
        return breakers(clusterSettings, requestLimitBytes, 1.0);
    }

    private static HierarchyCircuitBreakerService breakers(ClusterSettings clusterSettings, long requestLimitBytes, double overhead) {
        return new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(limitSettings(requestLimitBytes))
                .put(REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), overhead)
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            Collections.emptyList(),
            clusterSettings
        );
    }

    /** Runs a callback before each leaf is searched. */
    private static final class LeafHookSearcher extends IndexSearcher {
        private final Consumer<LeafReaderContext> beforeLeaf;

        LeafHookSearcher(IndexReader reader, Consumer<LeafReaderContext> beforeLeaf) {
            super(reader);
            this.beforeLeaf = beforeLeaf;
        }

        @Override
        protected void searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector)
            throws IOException {
            if (beforeLeaf != null) {
                beforeLeaf.accept(ctx);
            }
            super.searchLeaf(ctx, minDocId, maxDocId, weight, collector);
        }
    }

    /** Searches every segment as two doc-id partitions, each with its own leaf collector. */
    private static final class PartitioningSearcher extends IndexSearcher {
        PartitioningSearcher(IndexReader reader) {
            super(reader);
        }

        void searchInTwoPartitionsPerSegment(Query query, Collector collector) throws IOException {
            final Weight weight = createWeight(rewrite(query), collector.scoreMode(), 1f);
            final List<LeafReaderContextPartition> partitions = new ArrayList<>();
            for (LeafReaderContext ctx : getIndexReader().leaves()) {
                final int mid = ctx.reader().maxDoc() / 2;
                partitions.add(LeafReaderContextPartition.createFromAndTo(ctx, 0, mid));
                partitions.add(LeafReaderContextPartition.createFromAndTo(ctx, mid, ctx.reader().maxDoc()));
            }
            search(partitions.toArray(new LeafReaderContextPartition[0]), weight, collector);
        }
    }
}
