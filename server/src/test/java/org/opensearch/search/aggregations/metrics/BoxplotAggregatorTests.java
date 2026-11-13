/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;

public class BoxplotAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "number";

    /** Script to return the {@code _value} provided by aggs framework, plus one. */
    private static final String VALUE_SCRIPT = "_value";
    private static final String SINGLE_SCRIPT = "single";

    private static final double EXACT = 0d;

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {}, agg -> {
            assertFalse(AggregationInspectionHelper.hasValue(agg));
            assertEquals(Double.NaN, agg.getQ2(), EXACT);
        });
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {
            writer.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
            writer.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 2)));
        }, agg -> {
            assertFalse(AggregationInspectionHelper.hasValue(agg));
            assertEquals(Double.NaN, agg.getQ2(), EXACT);
        });
    }

    public void testSingleValuedField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {
            for (int value = 1; value <= 5; value++) {
                writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, value)));
            }
        }, agg -> {
            assertTrue(AggregationInspectionHelper.hasValue(agg));
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(5.0, agg.getMax(), EXACT);
            assertEquals(3.0, agg.getQ2(), 0.5);
            assertEquals(1.0, agg.getLower(), EXACT);
            assertEquals(5.0, agg.getUpper(), EXACT);
        });
    }

    public void testNumericDocValues() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {
            for (int value = 1; value <= 5; value++) {
                writer.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, value)));
            }
        }, agg -> {
            assertTrue(AggregationInspectionHelper.hasValue(agg));
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(5.0, agg.getMax(), EXACT);
            assertEquals(3.0, agg.getQ2(), 0.5);
        });
    }

    public void testMultiValuedField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {
            writer.addDocument(
                Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 1), new SortedNumericDocValuesField(FIELD_NAME, 2))
            );
            writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 3)));
        }, agg -> {
            assertEquals(3, agg.getState().size());
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(3.0, agg.getMax(), EXACT);
        });
    }

    public void testOutlierSetsWhiskers() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {
            for (int value = 1; value <= 100; value++) {
                writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, value)));
            }
            writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1000)));
        }, agg -> {
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(1000.0, agg.getMax(), EXACT);
            assertEquals(100.0, agg.getUpper(), 1.0);
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery(FIELD_NAME, 1, 10), writer -> {
            for (int value = 1; value <= 100; value++) {
                writer.addDocument(Arrays.asList(new IntPoint(FIELD_NAME, value), new SortedNumericDocValuesField(FIELD_NAME, value)));
            }
        }, agg -> {
            assertTrue(AggregationInspectionHelper.hasValue(agg));
            assertEquals(10, agg.getState().size());
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(10.0, agg.getMax(), EXACT);
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery(FIELD_NAME, -1, 0), writer -> {
            writer.addDocument(Arrays.asList(new IntPoint(FIELD_NAME, 1), new SortedNumericDocValuesField(FIELD_NAME, 1)));
            writer.addDocument(Arrays.asList(new IntPoint(FIELD_NAME, 2), new SortedNumericDocValuesField(FIELD_NAME, 2)));
        }, agg -> {
            assertFalse(AggregationInspectionHelper.hasValue(agg));
            assertEquals(Double.NaN, agg.getQ2(), EXACT);
        });
    }

    public void testUnmapped() throws IOException {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("boxplot").field("unknown");

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 7)));
            writer.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
        }, agg -> {
            assertFalse(AggregationInspectionHelper.hasValue(agg));
            assertEquals(Double.NaN, agg.getMin(), EXACT);
            assertEquals(Double.NaN, agg.getMax(), EXACT);
        });
    }

    public void testUnmappedMissing() throws IOException {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("boxplot").field("unknown").missing(7);

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(singleton(new NumericDocValuesField("unrelatedField", 1)));
            writer.addDocument(singleton(new NumericDocValuesField("unrelatedField", 2)));
            writer.addDocument(singleton(new NumericDocValuesField("unrelatedField", 3)));
        }, agg -> {
            assertTrue(AggregationInspectionHelper.hasValue(agg));
            assertEquals(3, agg.getState().size());
            assertEquals(7.0, agg.getMin(), EXACT);
            assertEquals(7.0, agg.getMax(), EXACT);
            assertEquals(7.0, agg.getQ1(), EXACT);
            assertEquals(7.0, agg.getQ2(), EXACT);
            assertEquals(7.0, agg.getQ3(), EXACT);
            assertEquals(7.0, agg.getLower(), EXACT);
            assertEquals(7.0, agg.getUpper(), EXACT);
        });
    }

    public void testMissingForMappedField() throws IOException {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("boxplot").field(FIELD_NAME).missing(7);

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1)));
            writer.addDocument(singleton(new SortedNumericDocValuesField("unrelatedField", 2)));
        }, agg -> {
            assertEquals(2, agg.getState().size());
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(7.0, agg.getMax(), EXACT);
        });
    }

    public void testValueScript() throws IOException {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("boxplot").field(FIELD_NAME)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            for (int value = 1; value <= 5; value++) {
                writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, value)));
            }
        }, agg -> {
            assertTrue(AggregationInspectionHelper.hasValue(agg));
            assertEquals(2.0, agg.getMin(), EXACT);
            assertEquals(6.0, agg.getMax(), EXACT);
        });
    }

    public void testScript() throws IOException {
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("boxplot").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SINGLE_SCRIPT, Collections.emptyMap())
        );

        testAggregation(builder, new MatchAllDocsQuery(), writer -> {
            for (int value = 1; value <= 10; value++) {
                writer.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, value)));
            }
        }, agg -> {
            assertTrue(AggregationInspectionHelper.hasValue(agg));
            assertEquals(10, agg.getState().size());
            assertEquals(1.0, agg.getMin(), EXACT);
            assertEquals(1.0, agg.getQ1(), EXACT);
            assertEquals(1.0, agg.getQ2(), EXACT);
            assertEquals(1.0, agg.getQ3(), EXACT);
            assertEquals(1.0, agg.getMax(), EXACT);
        });
    }

    public void testMetrics() throws IOException {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int value = 1; value <= 5; value++) {
                writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, value)));
            }
            try (IndexReader reader = writer.getReader()) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final BoxplotAggregator aggregator = createAggregator(
                    new BoxplotAggregationBuilder("boxplot").field(FIELD_NAME),
                    searcher,
                    fieldType
                );
                aggregator.preCollection();
                searcher.search(new MatchAllDocsQuery(), aggregator);
                aggregator.postCollection();

                for (String metric : List.of("min", "max", "q1", "q2", "q3", "lower", "upper")) {
                    assertTrue("expected metric [" + metric + "]", aggregator.hasMetric(metric));
                }
                assertFalse(aggregator.hasMetric("median"));
                assertFalse(aggregator.hasMetric("Min"));
                assertFalse(aggregator.hasMetric(""));

                assertEquals(1.0, aggregator.metric("min", 0), EXACT);
                assertEquals(5.0, aggregator.metric("max", 0), EXACT);
                assertEquals(3.0, aggregator.metric("q2", 0), 0.5);
                assertEquals(1.0, aggregator.metric("lower", 0), EXACT);
                assertEquals(5.0, aggregator.metric("upper", 0), EXACT);

                // the metrics agree with the aggregation that is built for the bucket
                final InternalBoxplot built = (InternalBoxplot) aggregator.buildAggregation(0);
                for (String metric : List.of("min", "max", "q1", "q2", "q3", "lower", "upper")) {
                    assertEquals(built.value(metric), aggregator.metric(metric, 0), EXACT);
                }

                // a bucket that never collected anything has no statistics
                for (String metric : List.of("min", "max", "q1", "q2", "q3", "lower", "upper")) {
                    assertEquals(Double.NaN, aggregator.metric(metric, 17), EXACT);
                }
                assertFalse(AggregationInspectionHelper.hasValue((InternalBoxplot) aggregator.buildAggregation(17)));

                // ordering is only possible by a known metric
                assertNotNull(aggregator.bucketComparator("q2", SortOrder.ASC));
                IllegalArgumentException unknown = expectThrows(
                    IllegalArgumentException.class,
                    () -> aggregator.bucketComparator("median", SortOrder.ASC)
                );
                assertThat(unknown.getMessage(), containsString("Unknown metric name [median]"));
                IllegalArgumentException noKey = expectThrows(
                    IllegalArgumentException.class,
                    () -> aggregator.bucketComparator(null, SortOrder.ASC)
                );
                assertThat(noKey.getMessage(), containsString("a metric name must be specified"));
            }
        }
    }

    public void testUnmappedMetrics() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            writer.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1)));
            try (IndexReader reader = writer.getReader()) {
                final IndexSearcher searcher = newIndexSearcher(reader);
                final BoxplotAggregator aggregator = createAggregator(new BoxplotAggregationBuilder("boxplot").field("unknown"), searcher);
                aggregator.preCollection();
                searcher.search(new MatchAllDocsQuery(), aggregator);
                aggregator.postCollection();

                assertTrue(aggregator.hasMetric("q1"));
                assertEquals(Double.NaN, aggregator.metric("q1", 0), EXACT);
                assertEquals(Double.NaN, aggregator.metric("max", 0), EXACT);
            }
        }
    }

    private void testAggregation(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalBoxplot> verify)
        throws IOException {
        final double compression = randomDoubleBetween(20, 1000, true);
        final BoxplotAggregationBuilder builder = new BoxplotAggregationBuilder("boxplot").field(FIELD_NAME).compression(compression);

        testAggregation(builder, query, buildIndex, agg -> {
            // the requested compression reaches the digest of every shard and survives the reduce
            assertEquals(compression, agg.getState().compression(), EXACT);
            verify.accept(agg);
        });
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalBoxplot> verify
    ) throws IOException {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return singletonList(CoreValuesSourceType.NUMERIC);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new BoxplotAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(VALUE_SCRIPT, vars -> ((Number) vars.get("_value")).doubleValue() + 1);
        scripts.put(SINGLE_SCRIPT, vars -> 1);

        final MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        final Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }
}
