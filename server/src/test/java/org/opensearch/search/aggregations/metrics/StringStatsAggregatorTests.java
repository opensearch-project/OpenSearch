/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.mapper.HllFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StringStatsAggregatorTests extends AggregatorTestCase {

    private static final String FIELD = "str";
    private static final String GROUP = "group";

    private static final String VALUE_SCRIPT = "_value";
    private static final String CONSTANT_SCRIPT = "constant";
    private static final String LIST_SCRIPT = "list";
    private static final String NUMBER_SCRIPT = "number";

    private static final String[] DRAFT_EXAMPLE = { "google.com", "x7k2qz9vh3.net", "mail.google.com" };

    private static final MappedFieldType KEYWORD = new KeywordFieldMapper.KeywordFieldType(FIELD);
    private static final MappedFieldType GROUP_KEYWORD = new KeywordFieldMapper.KeywordFieldType(GROUP);

    private static List<IndexableField> doc(String... values) {
        final List<IndexableField> fields = new ArrayList<>();
        for (String value : values) {
            fields.add(new SortedSetDocValuesField(FIELD, new BytesRef(value)));
        }
        return fields;
    }

    private static List<IndexableField> docInGroup(String group, String... values) {
        final List<IndexableField> fields = doc(values);
        fields.add(new StringField(GROUP, group, Field.Store.NO));
        fields.add(new SortedSetDocValuesField(GROUP, new BytesRef(group)));
        return fields;
    }

    private static StringStatsAggregationBuilder stringStats() {
        return new StringStatsAggregationBuilder("string_stats").field(FIELD);
    }

    private void testAggregation(
        StringStatsAggregationBuilder builder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalStringStats> verify
    ) throws IOException {
        testCase(builder, query, buildIndex, verify, KEYWORD);
    }

    public void testNoDocs() throws IOException {
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {}, stats -> {
            assertFalse(AggregationInspectionHelper.hasValue(stats));
            assertEmpty(stats);
        });
    }

    private static void assertEmpty(InternalStringStats stats) {
        assertEquals(0, stats.getCount());
        assertTrue(Double.isNaN(stats.getMinLength()));
        assertTrue(Double.isNaN(stats.getMaxLength()));
        assertTrue(Double.isNaN(stats.getAvgLength()));
        assertTrue(Double.isNaN(stats.getEntropy()));
        assertTrue(stats.getDistribution().isEmpty());
    }

    public void testDocumentsWithoutTheField() throws IOException {
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Collections.singleton(new SortedSetDocValuesField("other", new BytesRef("abc"))));
            writer.addDocument(Collections.singleton(new SortedSetDocValuesField("other", new BytesRef("de"))));
        }, stats -> {
            assertFalse(AggregationInspectionHelper.hasValue(stats));
            assertEmpty(stats);
        });
    }

    public void testSingleValuedField() throws IOException {
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {
            for (String value : DRAFT_EXAMPLE) {
                writer.addDocument(doc(value));
            }
        }, stats -> {
            assertTrue(AggregationInspectionHelper.hasValue(stats));
            assertEquals(3, stats.getCount());
            assertEquals(10.0, stats.getMinLength(), 0d);
            assertEquals(15.0, stats.getMaxLength(), 0d);
            assertEquals(13.0, stats.getAvgLength(), 0d);
            assertEquals(4.0604, stats.getEntropy(), 0.00005);
            // the distribution is only there when it is asked for
            assertTrue(stats.getDistribution().isEmpty());
        });
    }

    public void testShowDistribution() throws IOException {
        testAggregation(stringStats().showDistribution(true), new MatchAllDocsQuery(), writer -> {
            for (String value : DRAFT_EXAMPLE) {
                writer.addDocument(doc(value));
            }
        }, stats -> {
            assertEquals(21, stats.getDistribution().size());
            assertEquals(6.0 / 39, stats.getDistribution().get("o"), 1e-12);
            assertEquals(4.0 / 39, stats.getDistribution().get("g"), 1e-12);
            assertEquals(4.0 / 39, stats.getDistribution().get("."), 1e-12);
            // most frequent character first
            assertEquals("o", stats.getDistribution().keySet().iterator().next());
        });
    }

    public void testNoDistributionByDefaultAndWhenSwitchedOff() throws IOException {
        for (StringStatsAggregationBuilder builder : List.of(stringStats(), stringStats().showDistribution(false))) {
            testAggregation(builder, new MatchAllDocsQuery(), writer -> writer.addDocument(doc("abc")), stats -> {
                assertFalse(stats.showDistribution());
                assertTrue(stats.getDistribution().isEmpty());
            });
        }
    }

    public void testMultiValuedField() throws IOException {
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("ab", "cde"));
            writer.addDocument(doc("f"));
            writer.addDocument(doc("ghij", "klmnop", "q"));
        }, stats -> {
            // every value of a document counts, not every document
            assertEquals(6, stats.getCount());
            assertEquals(1.0, stats.getMinLength(), 0d);
            assertEquals(6.0, stats.getMaxLength(), 0d);
            assertEquals(17.0 / 6, stats.getAvgLength(), 1e-12);
            // all 17 letters are different, so each is as likely as the others
            assertEquals(Math.log(17) / Math.log(2), stats.getEntropy(), 1e-12);
        });
    }

    public void testMultiValuedFieldCountsEveryOccurrenceOfACharacter() throws IOException {
        testAggregation(stringStats().showDistribution(true), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("aab", "b"));
            writer.addDocument(doc("a"));
        }, stats -> {
            assertEquals(3, stats.getCount());
            assertEquals(3.0 / 5, stats.getDistribution().get("a"), 1e-12);
            assertEquals(2.0 / 5, stats.getDistribution().get("b"), 1e-12);
        });
    }

    public void testDuplicateValuesOfADocumentCountOnce() throws IOException {
        // doc values of a keyword field hold the distinct values of a document, so the same value twice is stored once
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> writer.addDocument(doc("same", "same")), stats -> {
            assertEquals(1, stats.getCount());
            assertEquals(4.0, stats.getMaxLength(), 0d);
        });
    }

    public void testEmptyStringsAreValuesWithoutCharacters() throws IOException {
        testAggregation(stringStats().showDistribution(true), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc(""));
            writer.addDocument(doc("abcd"));
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(0.0, stats.getMinLength(), 0d);
            assertEquals(4.0, stats.getMaxLength(), 0d);
            assertEquals(2.0, stats.getAvgLength(), 0d);
            assertEquals(2.0, stats.getEntropy(), 1e-12);
        });
    }

    public void testOnlyEmptyStrings() throws IOException {
        testAggregation(stringStats().showDistribution(true), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc(""));
            writer.addDocument(doc(""));
        }, stats -> {
            assertTrue(AggregationInspectionHelper.hasValue(stats));
            assertEquals(2, stats.getCount());
            assertEquals(0.0, stats.getAvgLength(), 0d);
            assertEquals(0.0, stats.getEntropy(), 0d);
            assertTrue(stats.getDistribution().isEmpty());
        });
    }

    public void testCharactersAreCodePointsNotUtf16Units() throws IOException {
        // the smiley takes four bytes in UTF-8 and two chars in Java, and the euro sign three bytes and one char
        testAggregation(stringStats().showDistribution(true), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("a😀b"));
            writer.addDocument(doc("€"));
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(1.0, stats.getMinLength(), 0d);
            assertEquals(3.0, stats.getMaxLength(), 0d);
            assertEquals(2.0, stats.getAvgLength(), 0d);
            assertEquals(Set.of("a", "😀", "b", "€"), stats.getDistribution().keySet());
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(stringStats(), new TermQuery(new Term(GROUP, "keep")), writer -> {
            writer.addDocument(docInGroup("keep", "abc"));
            writer.addDocument(docInGroup("keep", "de"));
            writer.addDocument(docInGroup("drop", "zzzzzzzzzzzz"));
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(2.0, stats.getMinLength(), 0d);
            assertEquals(3.0, stats.getMaxLength(), 0d);
            assertEquals(2.5, stats.getAvgLength(), 0d);
            // the z of the document that did not match does not count
            assertEquals(Math.log(5) / Math.log(2), stats.getEntropy(), 1e-12);
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(stringStats(), new TermQuery(new Term(GROUP, "none")), writer -> {
            writer.addDocument(docInGroup("keep", "abc"));
            writer.addDocument(docInGroup("keep", "de"));
        }, stats -> {
            assertFalse(AggregationInspectionHelper.hasValue(stats));
            assertEmpty(stats);
        });
    }

    public void testUnmapped() throws IOException {
        testAggregation(new StringStatsAggregationBuilder("string_stats").field("unknown"), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("abc"));
            writer.addDocument(doc("de"));
        }, stats -> {
            assertFalse(AggregationInspectionHelper.hasValue(stats));
            assertEmpty(stats);
        });
    }

    public void testUnmappedKeepsTheRequestForTheDistribution() throws IOException {
        testAggregation(
            new StringStatsAggregationBuilder("string_stats").field("unknown").showDistribution(true),
            new MatchAllDocsQuery(),
            writer -> writer.addDocument(doc("abc")),
            stats -> {
                assertTrue(stats.showDistribution());
                assertEmpty(stats);
            }
        );
    }

    public void testMissingForUnmappedField() throws IOException {
        testAggregation(
            new StringStatsAggregationBuilder("string_stats").field("unknown").missing("n/a").showDistribution(true),
            new MatchAllDocsQuery(),
            writer -> {
                writer.addDocument(doc("abc"));
                writer.addDocument(doc("de"));
                writer.addDocument(doc("f"));
            },
            stats -> {
                // all three documents have no value of that field, so each counts as the value that stands in for it
                assertTrue(AggregationInspectionHelper.hasValue(stats));
                assertEquals(3, stats.getCount());
                assertEquals(3.0, stats.getMinLength(), 0d);
                assertEquals(3.0, stats.getMaxLength(), 0d);
                assertEquals(Set.of("n", "/", "a"), stats.getDistribution().keySet());
                assertEquals(1.0 / 3, stats.getDistribution().get("/"), 1e-12);
            }
        );
    }

    public void testMissingForMappedField() throws IOException {
        testAggregation(stringStats().missing("zzzz"), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("ab"));
            writer.addDocument(Collections.singleton(new SortedSetDocValuesField("other", new BytesRef("unrelated"))));
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(2.0, stats.getMinLength(), 0d);
            assertEquals(4.0, stats.getMaxLength(), 0d);
        });
    }

    public void testMissingOnlyReplacesDocumentsWithoutValues() throws IOException {
        testAggregation(stringStats().missing("zzzz"), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("ab", "cd"));
            writer.addDocument(Collections.singleton(new SortedSetDocValuesField("other", new BytesRef("unrelated"))));
        }, stats -> {
            // two values of the first document, and the one that stands in for the second
            assertEquals(3, stats.getCount());
            assertEquals(2.0, stats.getMinLength(), 0d);
            assertEquals(4.0, stats.getMaxLength(), 0d);
        });
    }

    public void testMissingThatIsAnEmptyString() throws IOException {
        testAggregation(stringStats().missing(""), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("abcd"));
            writer.addDocument(Collections.singleton(new SortedSetDocValuesField("other", new BytesRef("unrelated"))));
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(0.0, stats.getMinLength(), 0d);
            assertEquals(2.0, stats.getAvgLength(), 0d);
        });
    }

    public void testMissingThatIsNotAString() throws IOException {
        // the number stands in as the characters it is written with
        testAggregation(stringStats().missing(12345).showDistribution(true), new MatchAllDocsQuery(), writer -> {
            writer.addDocument(Collections.singleton(new SortedSetDocValuesField("other", new BytesRef("unrelated"))));
        }, stats -> {
            assertEquals(1, stats.getCount());
            assertEquals(5.0, stats.getMaxLength(), 0d);
            assertEquals(5, stats.getDistribution().size());
        });
    }

    public void testScriptWithoutField() throws IOException {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder("string_stats").script(script(CONSTANT_SCRIPT));

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("ignored"));
            writer.addDocument(doc("ignored too"));
        }, stats -> {
            assertEquals(2, stats.getCount());
            assertEquals(2.0, stats.getMinLength(), 0d);
            assertEquals(2.0, stats.getMaxLength(), 0d);
            // all four characters are an a or a b, equally often
            assertEquals(1.0, stats.getEntropy(), 1e-12);
        });
    }

    public void testScriptThatReturnsSeveralValues() throws IOException {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder("string_stats").script(script(LIST_SCRIPT));

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("x"));
            writer.addDocument(doc("y"));
        }, stats -> {
            // a and bcd for each of the two documents
            assertEquals(4, stats.getCount());
            assertEquals(1.0, stats.getMinLength(), 0d);
            assertEquals(3.0, stats.getMaxLength(), 0d);
            assertEquals(2.0, stats.getAvgLength(), 0d);
        });
    }

    public void testScriptThatReturnsANumber() throws IOException {
        final StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder("string_stats").script(script(NUMBER_SCRIPT));

        testAggregation(builder, new MatchAllDocsQuery(), writer -> writer.addDocument(doc("x")), stats -> {
            assertEquals(1, stats.getCount());
            assertEquals(5.0, stats.getMaxLength(), 0d);
        });
    }

    public void testValueScript() throws IOException {
        final StringStatsAggregationBuilder builder = stringStats().script(script(VALUE_SCRIPT));

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(doc("ab"));
            writer.addDocument(doc("cde"));
        }, stats -> {
            // the script appends an exclamation mark to each value
            assertEquals(2, stats.getCount());
            assertEquals(3.0, stats.getMinLength(), 0d);
            assertEquals(4.0, stats.getMaxLength(), 0d);
        });
    }

    private static Script script(String name) {
        return new Script(ScriptType.INLINE, MockScriptEngine.NAME, name, Collections.emptyMap());
    }

    public void testMetrics() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (String value : DRAFT_EXAMPLE) {
                writer.addDocument(doc(value));
            }
            try (IndexReader reader = writer.getReader()) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final StringStatsAggregator aggregator = createAggregator(stringStats(), searcher, KEYWORD);
                aggregator.preCollection();
                searcher.search(new MatchAllDocsQuery(), aggregator);
                aggregator.postCollection();

                for (String metric : List.of("count", "min_length", "max_length", "avg_length", "entropy")) {
                    assertTrue("expected metric [" + metric + "]", aggregator.hasMetric(metric));
                }
                for (String unknown : List.of("distribution", "min", "Entropy", "", "value")) {
                    assertFalse("unexpected metric [" + unknown + "]", aggregator.hasMetric(unknown));
                }

                assertEquals(3.0, aggregator.metric("count", 0), 0d);
                assertEquals(10.0, aggregator.metric("min_length", 0), 0d);
                assertEquals(15.0, aggregator.metric("max_length", 0), 0d);
                assertEquals(13.0, aggregator.metric("avg_length", 0), 0d);
                assertEquals(4.0604, aggregator.metric("entropy", 0), 0.00005);

                // the metrics are those of the aggregation that is built for the bucket, to the last bit
                final InternalStringStats built = (InternalStringStats) aggregator.buildAggregation(0);
                for (String metric : List.of("count", "min_length", "max_length", "avg_length", "entropy")) {
                    assertEquals(built.value(metric), aggregator.metric(metric, 0), 0d);
                }

                // a bucket that never collected anything has no values, which is a count of zero and nothing else
                assertEquals(0.0, aggregator.metric("count", 17), 0d);
                for (String metric : List.of("min_length", "max_length", "avg_length", "entropy")) {
                    assertTrue(Double.isNaN(aggregator.metric(metric, 17)));
                }
                assertFalse(AggregationInspectionHelper.hasValue((InternalStringStats) aggregator.buildAggregation(17)));

                // ordering is only possible by a known metric
                assertNotNull(aggregator.bucketComparator("entropy", SortOrder.ASC));
                final IllegalArgumentException unknown = expectThrows(
                    IllegalArgumentException.class,
                    () -> aggregator.bucketComparator("variance", SortOrder.ASC)
                );
                assertThat(unknown.getMessage(), containsString("Unknown metric name [variance]"));
                final IllegalArgumentException noKey = expectThrows(
                    IllegalArgumentException.class,
                    () -> aggregator.bucketComparator(null, SortOrder.ASC)
                );
                assertThat(noKey.getMessage(), containsString("a metric name must be specified"));
                final IllegalArgumentException value = expectThrows(IllegalArgumentException.class, () -> aggregator.metric("variance", 0));
                assertThat(value.getMessage(), containsString("Unknown value [variance] in string_stats aggregation"));
            }
        }
    }

    public void testUnmappedMetrics() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(doc("abc"));
            try (IndexReader reader = writer.getReader()) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final StringStatsAggregator aggregator = createAggregator(
                    new StringStatsAggregationBuilder("string_stats").field("unknown"),
                    searcher
                );
                aggregator.preCollection();
                searcher.search(new MatchAllDocsQuery(), aggregator);
                aggregator.postCollection();

                assertTrue(aggregator.hasMetric("entropy"));
                assertEquals(0.0, aggregator.metric("count", 0), 0d);
                assertTrue(Double.isNaN(aggregator.metric("entropy", 0)));
                assertTrue(Double.isNaN(aggregator.metric("min_length", 0)));
            }
        }
    }

    public void testTermsOrderedByTheEntropyOfTheValues() throws IOException {
        final CheckedConsumer<RandomIndexWriter, IOException> buildIndex = writer -> {
            // low is a single character, mid is two characters that are equally frequent, and high is ten different ones
            writer.addDocument(docInGroup("low", "aaaaaaaa"));
            writer.addDocument(docInGroup("low", "aaaa"));
            writer.addDocument(docInGroup("mid", "abab"));
            writer.addDocument(docInGroup("high", "x7k2qz9vh3"));
        };
        for (boolean descending : new boolean[] { true, false }) {
            final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP)
                .order(BucketOrder.aggregation("shape.entropy", descending == false))
                .subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD));

            testCase(terms, new MatchAllDocsQuery(), buildIndex, (StringTerms result) -> {
                final List<String> keys = new ArrayList<>();
                for (Terms.Bucket bucket : result.getBuckets()) {
                    keys.add(bucket.getKeyAsString());
                }
                assertEquals(descending ? List.of("high", "mid", "low") : List.of("low", "mid", "high"), keys);
            }, GROUP_KEYWORD, KEYWORD);
        }
    }

    public void testTermsOrderedByTheAverageLengthOfTheValues() throws IOException {
        final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP)
            .order(BucketOrder.aggregation("shape.avg_length", false))
            .subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD));

        testCase(terms, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(docInGroup("short", "ab"));
            writer.addDocument(docInGroup("long", "abcdefghij"));
            writer.addDocument(docInGroup("middle", "abcde"));
        }, (StringTerms result) -> {
            final List<String> keys = new ArrayList<>();
            for (Terms.Bucket bucket : result.getBuckets()) {
                keys.add(bucket.getKeyAsString());
            }
            assertEquals(List.of("long", "middle", "short"), keys);
        }, GROUP_KEYWORD, KEYWORD);
    }

    public void testEachGroupHasItsOwnStatistics() throws IOException {
        final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP)
            .subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD).showDistribution(true));

        testCase(terms, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(docInGroup("a", "aaaa"));
            writer.addDocument(docInGroup("a", "aa"));
            writer.addDocument(docInGroup("b", "xy"));
            writer.addDocument(docInGroup("b", "xyzw"));
        }, (StringTerms result) -> {
            assertEquals(2, result.getBuckets().size());
            final InternalStringStats a = result.getBucketByKey("a").getAggregations().get("shape");
            final InternalStringStats b = result.getBucketByKey("b").getAggregations().get("shape");
            // nothing of a group leaks into the other
            assertEquals(2, a.getCount());
            assertEquals(3.0, a.getAvgLength(), 0d);
            assertEquals(0.0, a.getEntropy(), 0d);
            assertEquals(Collections.singletonMap("a", 1.0), a.getDistribution());
            assertEquals(2, b.getCount());
            assertEquals(3.0, b.getAvgLength(), 0d);
            // x and y make a third each of the characters, and z and w a sixth each
            final double expectedEntropy = -(2 * (1.0 / 3) * Math.log(1.0 / 3) + 2 * (1.0 / 6) * Math.log(1.0 / 6)) / Math.log(2);
            assertEquals(expectedEntropy, b.getEntropy(), 1e-12);
            assertEquals(4, b.getDistribution().size());
        }, GROUP_KEYWORD, KEYWORD);
    }

    public void testBucketsPathResolvesEveryStatistic() throws IOException {
        final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP)
            .subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD));

        testCase(terms, new MatchAllDocsQuery(), writer -> writer.addDocument(docInGroup("a", "abcd", "ab")), (StringTerms result) -> {
            final StringTerms.Bucket bucket = result.getBuckets().get(0);
            final InternalStringStats stats = bucket.getAggregations().get("shape");
            for (String metric : List.of("count", "min_length", "max_length", "avg_length", "entropy")) {
                assertEquals(stats.value(metric), (double) bucket.getProperty("by_group", List.of("shape", metric)), 0d);
            }
        }, GROUP_KEYWORD, KEYWORD);
    }

    public void testTermsOrderedByAnUnknownStatistic() {
        assertOrderIsRejected("shape.variance", "variance", false);
    }

    public void testTheDistributionIsNotAStatisticToOrderBy() {
        assertOrderIsRejected("shape.distribution", "distribution", true);
    }

    private void assertOrderIsRejected(String path, String unknown, boolean showDistribution) {
        final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP)
            .order(BucketOrder.aggregation(path, true))
            .subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD).showDistribution(showDistribution));

        final AggregationExecutionException e = expectThrows(
            AggregationExecutionException.class,
            () -> testCase(terms, new MatchAllDocsQuery(), writer -> writer.addDocument(docInGroup("a", "abc")), (StringTerms result) -> {
                fail("the request has to be rejected");
            }, GROUP_KEYWORD, KEYWORD)
        );

        // the framework reports an order path it can not resolve, with the reason that the aggregation gives for it
        assertThat(e.getMessage(), containsString("Invalid aggregation order path [" + path + "]"));
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("Unknown metric name [" + unknown + "] on multi-value metrics aggregation [shape]")
        );
    }

    public void testStatisticsOfRandomValues() throws IOException {
        final int numberOfDocs = randomIntBetween(1, 40);
        final List<List<String>> docs = new ArrayList<>();
        final Map<Integer, Long> frequencies = new TreeMap<>();
        long count = 0;
        long characters = 0;
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (int i = 0; i < numberOfDocs; i++) {
            // a field that has a value for only some of the documents, and several for others
            final int numberOfValues = randomIntBetween(0, 4);
            final List<String> values = new ArrayList<>();
            for (int v = 0; v < numberOfValues; v++) {
                final String value = randomBoolean()
                    ? randomAlphaOfLengthBetween(0, 12)
                    : randomRealisticUnicodeOfCodepointLengthBetween(0, 12);
                if (values.contains(value) == false) {
                    // the doc values of a document do not hold a value twice
                    values.add(value);
                }
            }
            docs.add(values);
            for (String value : values) {
                final int length = value.codePointCount(0, value.length());
                count++;
                characters += length;
                min = Math.min(min, length);
                max = Math.max(max, length);
                value.codePoints().forEach(codePoint -> frequencies.merge(codePoint, 1L, Long::sum));
            }
        }
        final long expectedCount = count;
        final long expectedCharacters = characters;
        final int expectedMin = min;
        final int expectedMax = max;
        double entropy = 0;
        for (long occurrences : frequencies.values()) {
            final double probability = (double) occurrences / expectedCharacters;
            entropy -= probability * Math.log(probability) / Math.log(2);
        }
        final double expectedEntropy = entropy;

        testAggregation(stringStats().showDistribution(true), new MatchAllDocsQuery(), writer -> {
            for (List<String> values : docs) {
                writer.addDocument(doc(values.toArray(new String[0])));
            }
        }, stats -> {
            assertEquals(expectedCount, stats.getCount());
            if (expectedCount == 0) {
                assertEmpty(stats);
                return;
            }
            assertEquals(expectedMin, stats.getMinLength(), 0d);
            assertEquals(expectedMax, stats.getMaxLength(), 0d);
            assertEquals((double) expectedCharacters / expectedCount, stats.getAvgLength(), 1e-9);
            assertEquals(expectedEntropy, stats.getEntropy(), 1e-9);
            assertEquals(frequencies.size(), stats.getDistribution().size());
            for (Map.Entry<Integer, Long> entry : frequencies.entrySet()) {
                final String character = new String(Character.toChars(entry.getKey()));
                assertEquals((double) entry.getValue() / expectedCharacters, stats.getDistribution().get(character), 1e-12);
            }
        });
    }

    public void testStatisticsDoNotDependOnHowTheValuesAreSpreadOverSegments() throws IOException {
        // the entropy of the whole is not the average of the entropy of the parts, whichever way the documents are cut into segments
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {
            for (int i = 0; i < 20; i++) {
                writer.addDocument(doc(i % 2 == 0 ? "aaaa" : "bbbb"));
                if (rarely()) {
                    writer.commit();
                }
            }
        }, stats -> {
            assertEquals(20, stats.getCount());
            assertEquals(1.0, stats.getEntropy(), 1e-12);
        });
    }

    public void testBucketsWithManyDistinctCharactersAreAllowedUpToTheLimit() throws IOException {
        testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {
            // 64 values of 1024 distinct characters each, which together are as many distinct characters as are allowed
            for (int doc = 0; doc < InternalStringStats.MAX_DISTINCT_CHARACTERS / 1024; doc++) {
                writer.addDocument(doc(distinctCharacters(0x20000 + doc * 1024, 1024)));
            }
        }, stats -> {
            assertEquals(InternalStringStats.MAX_DISTINCT_CHARACTERS / 1024, stats.getCount());
            assertEquals(InternalStringStats.MAX_DISTINCT_CHARACTERS, stats.codePoints().length);
        });
    }

    public void testBucketsWithMoreDistinctCharactersThanTheLimitAreRejected() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            testAggregation(stringStats(), new MatchAllDocsQuery(), writer -> {
                for (int doc = 0; doc <= InternalStringStats.MAX_DISTINCT_CHARACTERS / 1024; doc++) {
                    writer.addDocument(doc(distinctCharacters(0x20000 + doc * 1024, 1024)));
                }
            }, stats -> fail("the aggregation has to fail"));
        });

        assertThat(e.getMessage(), containsString("[string_stats] aggregation [string_stats]"));
        assertThat(e.getMessage(), containsString("more than [" + InternalStringStats.MAX_DISTINCT_CHARACTERS + "] distinct characters"));
    }

    public void testTheLimitIsEnforcedWhileCollectingAndNotOnlyWhenReducing() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int doc = 0; doc <= InternalStringStats.MAX_DISTINCT_CHARACTERS / 1024; doc++) {
                writer.addDocument(doc(distinctCharacters(0x20000 + doc * 1024, 1024)));
            }
            try (IndexReader reader = writer.getReader()) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final StringStatsAggregator aggregator = createAggregator(stringStats(), searcher, KEYWORD);
                aggregator.preCollection();

                // a shard has to stop at the limit, with no results to reduce yet, as its memory is what the limit protects
                final IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> searcher.search(new MatchAllDocsQuery(), aggregator)
                );

                assertThat(
                    e.getMessage(),
                    containsString("more than [" + InternalStringStats.MAX_DISTINCT_CHARACTERS + "] distinct characters")
                );
            }
        }
    }

    public void testTheLimitOfDistinctCharactersIsPerBucket() throws IOException {
        final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP)
            .subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD));

        testCase(terms, new MatchAllDocsQuery(), writer -> {
            // together the groups have twice as many distinct characters as one bucket may have
            for (int doc = 0; doc < InternalStringStats.MAX_DISTINCT_CHARACTERS / 1024; doc++) {
                writer.addDocument(docInGroup("a", distinctCharacters(0x20000 + doc * 1024, 1024)));
                writer.addDocument(docInGroup("b", distinctCharacters(0x40000 + doc * 1024, 1024)));
            }
        }, (StringTerms result) -> {
            assertEquals(2, result.getBuckets().size());
            for (Terms.Bucket bucket : result.getBuckets()) {
                final InternalStringStats stats = bucket.getAggregations().get("shape");
                assertEquals(InternalStringStats.MAX_DISTINCT_CHARACTERS, stats.codePoints().length);
            }
        }, GROUP_KEYWORD, KEYWORD);
    }

    private static String distinctCharacters(int first, int number) {
        final int[] codePoints = new int[number];
        for (int i = 0; i < number; i++) {
            codePoints[i] = first + i;
        }
        return new String(codePoints, 0, number);
    }

    public void testTheMemoryOfTheCharactersIsChargedToTheCircuitBreaker() throws IOException {
        final LimitedBreaker breaker = new LimitedBreaker(Long.MAX_VALUE);
        final long chargedForNothing;
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(doc("a"));
            try (IndexReader reader = writer.getReader()) {
                chargedForNothing = collectWithBreaker(reader, breaker);
            }
        }

        final LimitedBreaker breakerForManyCharacters = new LimitedBreaker(Long.MAX_VALUE);
        final long chargedForManyCharacters;
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(doc(distinctCharacters(0x20000, 4000)));
            try (IndexReader reader = writer.getReader()) {
                chargedForManyCharacters = collectWithBreaker(reader, breakerForManyCharacters);
            }
        }

        // 4000 distinct characters need an int and a long each, and the table has room to spare
        assertThat(chargedForManyCharacters - chargedForNothing, greaterThan(4000L * (Integer.BYTES + Long.BYTES)));
    }

    public void testManyDistinctCharactersCanTripTheCircuitBreaker() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(doc(distinctCharacters(0x20000, 4000)));
            try (IndexReader reader = writer.getReader()) {
                // enough room to start, but not for the characters of the value
                final LimitedBreaker breaker = new LimitedBreaker(20_000);

                expectThrows(CircuitBreakingException.class, () -> collectWithBreaker(reader, breaker));
            }
        }
    }

    public void testManyBucketsAreChargedToTheCircuitBreaker() throws IOException {
        final LimitedBreaker withoutStats = new LimitedBreaker(Long.MAX_VALUE);
        final LimitedBreaker withStats = new LimitedBreaker(Long.MAX_VALUE);
        final long chargedWithoutStats;
        final long chargedWithStats;
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < 200; i++) {
                writer.addDocument(docInGroup("group-" + i, "abc"));
            }
            try (IndexReader reader = writer.getReader()) {
                chargedWithoutStats = collectGroupsWithBreaker(reader, withoutStats, false);
                chargedWithStats = collectGroupsWithBreaker(reader, withStats, true);
            }
        }

        // every one of the 200 buckets has a table of counts of its own, which is more than a hundred bytes
        assertThat(chargedWithStats - chargedWithoutStats, greaterThan(200L * 100));
    }

    /**
     * Collects the values of the documents with an aggregator that has to ask the breaker for its memory, and returns the most
     * that the breaker was ever asked for.
     */
    private long collectWithBreaker(IndexReader reader, LimitedBreaker breaker) throws IOException {
        final IndexSearcher searcher = newIndexSearcher(reader);
        final SearchContext context = createSearchContext(
            searcher,
            createIndexSettings(),
            new MatchAllDocsQuery(),
            new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, breaker),
            breakerService(breaker),
            KEYWORD
        );
        final StringStatsAggregator aggregator = createAggregator(stringStats(), context);
        try {
            aggregator.preCollection();
            searcher.search(new MatchAllDocsQuery(), aggregator);
            aggregator.postCollection();
            aggregator.buildTopLevel();
            return breaker.getUsed();
        } finally {
            // the aggregator is closed when the test is over, and then all that it was charged must have been given back
            breakers.add(breaker);
        }
    }

    private long collectGroupsWithBreaker(IndexReader reader, LimitedBreaker breaker, boolean withStringStats) throws IOException {
        final IndexSearcher searcher = newIndexSearcher(reader);
        final TermsAggregationBuilder terms = new TermsAggregationBuilder("by_group").field(GROUP).size(200);
        if (withStringStats) {
            terms.subAggregation(new StringStatsAggregationBuilder("shape").field(FIELD));
        }
        final SearchContext context = createSearchContext(
            searcher,
            createIndexSettings(),
            new MatchAllDocsQuery(),
            new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, breaker),
            breakerService(breaker),
            GROUP_KEYWORD,
            KEYWORD
        );
        final Aggregator aggregator = createAggregator(terms, context);
        try {
            aggregator.preCollection();
            searcher.search(new MatchAllDocsQuery(), aggregator);
            aggregator.postCollection();
            aggregator.buildTopLevel();
            return breaker.getUsed();
        } finally {
            breakers.add(breaker);
        }
    }

    private final List<LimitedBreaker> breakers = new ArrayList<>();

    @Override
    protected void afterClose() {
        for (LimitedBreaker breaker : breakers) {
            assertThat("the memory of an aggregator is released with it", breaker.getUsed(), equalTo(0L));
        }
        breakers.clear();
    }

    private static CircuitBreakerService breakerService(CircuitBreaker breaker) {
        final CircuitBreakerService service = mock(CircuitBreakerService.class);
        when(service.getBreaker(anyString())).thenAnswer(
            invocation -> CircuitBreaker.REQUEST.equals(invocation.getArgument(0))
                ? breaker
                : new NoopCircuitBreaker(invocation.getArgument(0))
        );
        return service;
    }

    /**
     * A breaker that counts the bytes it is asked for, and breaks when they would go above its limit.
     */
    private static final class LimitedBreaker extends NoopCircuitBreaker {
        private final long limit;
        private long used;

        LimitedBreaker(long limit) {
            super(CircuitBreaker.REQUEST);
            this.limit = limit;
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (used + bytes > limit) {
                throw new CircuitBreakingException(
                    "[" + label + "] would use [" + (used + bytes) + "] bytes",
                    bytes,
                    limit,
                    Durability.TRANSIENT
                );
            }
            used += bytes;
            return used;
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            used += bytes;
            return used;
        }

        @Override
        public long getUsed() {
            return used;
        }

        @Override
        public long getLimit() {
            return limit;
        }
    }

    @Override
    protected List<String> unsupportedMappedFieldTypes() {
        // the doc values of an hll field are sketches, which are bytes and not strings, and its field data refuses to hand them out
        return List.of(HllFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return singletonList(CoreValuesSourceType.BYTES);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new StringStatsAggregationBuilder("foo").field(fieldName);
    }

    public void testNumericFieldIsRejected() {
        final MappedFieldType number = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(
                new StringStatsAggregationBuilder("string_stats").field("number"),
                new MatchAllDocsQuery(),
                writer -> writer.addDocument(Collections.emptyList()),
                (InternalStringStats stats) -> fail("a number is not a string"),
                number
            )
        );

        assertThat(e.getMessage(), containsString("Field [number] of type [long] is not supported for aggregation [string_stats]"));
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put(VALUE_SCRIPT, vars -> vars.get("_value") + "!");
        scripts.put(CONSTANT_SCRIPT, vars -> "ab");
        scripts.put(LIST_SCRIPT, vars -> List.of("a", "bcd"));
        scripts.put(NUMBER_SCRIPT, vars -> 12345);

        final MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        final Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }
}
