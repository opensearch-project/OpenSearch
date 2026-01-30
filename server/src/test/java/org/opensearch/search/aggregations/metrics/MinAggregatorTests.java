/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.InternalGlobal;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

public class MinAggregatorTests extends AggregatorTestCase {

    private final String SCRIPT_NAME = "script_name";
    private QueryShardContext queryShardContext;
    private final long SCRIPT_VALUE = 19L;

    /** Script to take a field name in params and sum the values of the field. */
    private static final String SUM_FIELD_PARAMS_SCRIPT = "sum_field_params";

    /** Script to sum the values of a field named {@code values}. */
    private static final String SUM_VALUES_FIELD_SCRIPT = "sum_values_field";

    /** Script to return the value of a field named {@code value}. */
    private static final String VALUE_FIELD_SCRIPT = "value_field";

    /** Script to return the {@code _value} provided by aggs framework. */
    private static final String VALUE_SCRIPT = "_value";

    private static final String INVERT_SCRIPT = "invert";

    private static final String RANDOM_SCRIPT = "random";

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        Function<Map<String, Object>, Integer> getInc = vars -> {
            if (vars == null || vars.containsKey("inc") == false) {
                return 0;
            } else {
                return ((Number) vars.get("inc")).intValue();
            }
        };

        BiFunction<Map<String, Object>, String, Object> sum = (vars, fieldname) -> {
            int inc = getInc.apply(vars);
            LeafDocLookup docLookup = (LeafDocLookup) vars.get("doc");
            List<Long> values = new ArrayList<>();
            for (Object v : docLookup.get(fieldname)) {
                values.add(((Number) v).longValue() + inc);
            }
            return values;
        };

        scripts.put(SCRIPT_NAME, script -> SCRIPT_VALUE);
        scripts.put(SUM_FIELD_PARAMS_SCRIPT, vars -> {
            String fieldname = (String) vars.get("field");
            return sum.apply(vars, fieldname);
        });
        scripts.put(SUM_VALUES_FIELD_SCRIPT, vars -> sum.apply(vars, "values"));
        scripts.put(VALUE_FIELD_SCRIPT, vars -> sum.apply(vars, "value"));
        scripts.put(VALUE_SCRIPT, vars -> {
            int inc = getInc.apply(vars);
            return ((Number) vars.get("_value")).doubleValue() + inc;
        });
        scripts.put(INVERT_SCRIPT, vars -> -((Number) vars.get("_value")).doubleValue());

        Map<String, Function<Map<String, Object>, Object>> nonDeterministicScripts = new HashMap<>();
        nonDeterministicScripts.put(RANDOM_SCRIPT, vars -> AggregatorTestCase.randomDouble());

        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            scripts,
            nonDeterministicScripts,
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    @Override
    protected QueryShardContext queryShardContextMock(
        IndexSearcher searcher,
        MapperService mapperService,
        IndexSettings indexSettings,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays
    ) {
        this.queryShardContext = super.queryShardContextMock(searcher, mapperService, indexSettings, circuitBreakerService, bigArrays);
        return queryShardContext;
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 3)));
        }, min -> {
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(2, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(2, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(3, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(3, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(IntPoint.newRangeQuery("number", 0, 3), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 3), new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(1, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testCase(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 3), new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testIpField() throws IOException {
        final String fieldName = "IP_field";
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field(fieldName);

        MappedFieldType fieldType = new IpFieldMapper.IpFieldType(fieldName);
        boolean v4 = randomBoolean();
        expectThrows(IllegalArgumentException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedSetDocValuesField(fieldName, new BytesRef(InetAddressPoint.encode(randomIp(v4))))));
            iw.addDocument(singleton(new SortedSetDocValuesField(fieldName, new BytesRef(InetAddressPoint.encode(randomIp(v4))))));
        }, min -> fail("expected an exception"), fieldType));
    }

    public void testUnmappedWithMissingField() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("does_not_exist").missing(0L);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> {
            assertEquals(0.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testUnsupportedType() {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("not_a_number");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
                iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo"))));
            }, (Consumer<InternalMin>) min -> { fail("Should have thrown exception"); }, fieldType)
        );
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [min]", e.getMessage());
    }

    public void testBadMissingField() {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number").missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> { fail("Should have thrown exception"); }, fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("does_not_exist").missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> { fail("Should have thrown exception"); }, fieldType));
    }

    public void testEmptyBucket() throws IOException {
        HistogramAggregationBuilder histogram = new HistogramAggregationBuilder("histo").field("number")
            .interval(1)
            .minDocCount(0)
            .subAggregation(new MinAggregationBuilder("min").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(histogram, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, (Consumer<InternalHistogram>) histo -> {
            assertThat(histo.getBuckets().size(), equalTo(3));

            assertNotNull(histo.getBuckets().get(0).getAggregations().asMap().get("min"));
            InternalMin min = (InternalMin) histo.getBuckets().get(0).getAggregations().asMap().get("min");
            assertEquals(1.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));

            assertNotNull(histo.getBuckets().get(1).getAggregations().asMap().get("min"));
            min = (InternalMin) histo.getBuckets().get(1).getAggregations().asMap().get("min");
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(min));

            assertNotNull(histo.getBuckets().get(2).getAggregations().asMap().get("min"));
            min = (InternalMin) histo.getBuckets().get(2).getAggregations().asMap().get("min");
            assertEquals(3.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));

        }, fieldType);
    }

    public void testFormatter() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number").format("0000.0");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> {
            assertEquals(1.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
            assertEquals("0001.0", min.getValueAsString());
        }, fieldType);
    }

    public void testGetProperty() throws IOException {
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
            new MinAggregationBuilder("min").field("number")
        );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(globalBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalGlobal>) global -> {
            assertEquals(2, global.getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(global));
            assertNotNull(global.getAggregations().asMap().get("min"));

            InternalMin min = (InternalMin) global.getAggregations().asMap().get("min");
            assertEquals(1.0, min.getValue(), 0);
            assertThat(global.getProperty("min"), equalTo(min));
            assertThat(global.getProperty("min.value"), equalTo(1.0));
            assertThat(min.getProperty("value"), equalTo(1.0));
        }, fieldType);
    }

    public void testSingleValuedFieldPartiallyUnmapped() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

        try (Directory directory = newDirectory(); Directory unmappedDirectory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
            unmappedIndexWriter.close();

            try (
                IndexReader indexReader = DirectoryReader.open(directory);
                IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory)
            ) {

                MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
                IndexSearcher indexSearcher = newSearcher(multiReader, true, true);

                InternalMin min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(2.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(min));
            }
        }
    }

    public void testSingleValuedFieldPartiallyUnmappedWithMissing() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number").missing(-19L);

        try (Directory directory = newDirectory(); Directory unmappedDirectory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
            unmappedIndexWriter.addDocument(singleton(new NumericDocValuesField("unrelated", 100)));
            unmappedIndexWriter.close();

            try (
                IndexReader indexReader = DirectoryReader.open(directory);
                IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory)
            ) {

                MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
                IndexSearcher indexSearcher = newSearcher(multiReader, true, true);

                InternalMin min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(-19.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(min));
            }
        }
    }

    public void testSingleValuedFieldWithValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(-10.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScriptAndMissing() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .missing(-100L)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
            iw.addDocument(singleton(new NumericDocValuesField("unrelated", 1)));
        }, (Consumer<InternalMin>) min -> {
            assertEquals(-100.0, min.getValue(), 0); // Note: this comes straight from missing, and is not inverted from script
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScriptAndParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.singletonMap("inc", 5)));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(6.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SCRIPT_NAME, Collections.emptyMap())
        );

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(19.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testMultiValuedField() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(2.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testMultiValuedFieldWithScript() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(-12.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testMultiValuedFieldWithScriptParams() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.singletonMap("inc", 5)));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(7.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testOrderByEmptyAggregation() throws IOException {
        AggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("number")
            .order(BucketOrder.compound(BucketOrder.aggregation("filter>min", true)))
            .subAggregation(
                new FilterAggregationBuilder("filter", termQuery("number", 100)).subAggregation(
                    new MinAggregationBuilder("min").field("number")
                )
            );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        int numDocs = 10;
        testCase(termsBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalTerms<?, LongTerms.Bucket>>) terms -> {
            for (int i = 0; i < numDocs; i++) {
                List<LongTerms.Bucket> buckets = terms.getBuckets();
                Terms.Bucket bucket = buckets.get(i);
                assertNotNull(bucket);
                assertEquals((long) i + 1, bucket.getKeyAsNumber());
                assertEquals(1L, bucket.getDocCount());

                Filter filter = bucket.getAggregations().get("filter");
                assertNotNull(filter);
                assertEquals(0L, filter.getDocCount());

                InternalMin min = filter.getAggregations().get("min");
                assertNotNull(min);
                assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(min));
            }
        }, fieldType);
    }

    public void testCaching() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                InternalMin min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(2.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(min));

                assertTrue(queryShardContext.isCacheable());
            }
        }
    }

    public void testScriptCaching() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        MinAggregationBuilder nonDeterministicAggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, RANDOM_SCRIPT, Collections.emptyMap()));

        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                InternalMin min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), nonDeterministicAggregationBuilder, fieldType);
                assertTrue(min.getValue() >= 0.0 && min.getValue() <= 1.0);
                assertTrue(AggregationInspectionHelper.hasValue(min));

                assertFalse(queryShardContext.isCacheable());

                indexSearcher = newSearcher(indexReader, true, true);

                min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(-7.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(min));

                assertTrue(queryShardContext.isCacheable());
            }
        }
    }

    public void testMinShortcutRandom() throws Exception {
        testMinShortcutCase(
            () -> randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE),
            (n) -> new LongPoint("number", n.longValue()),
            (v) -> LongPoint.decodeDimension(v, 0)
        );

        testMinShortcutCase(() -> randomInt(), (n) -> new IntPoint("number", n.intValue()), (v) -> IntPoint.decodeDimension(v, 0));

        testMinShortcutCase(() -> randomFloat(), (n) -> new FloatPoint("number", n.floatValue()), (v) -> FloatPoint.decodeDimension(v, 0));

        testMinShortcutCase(
            () -> randomDouble(),
            (n) -> new DoublePoint("number", n.doubleValue()),
            (v) -> DoublePoint.decodeDimension(v, 0)
        );
    }

    private void testMinShortcutCase(
        Supplier<Number> randomNumber,
        Function<Number, Field> pointFieldFunc,
        Function<byte[], Number> pointConvertFunc
    ) throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter indexWriter = new IndexWriter(directory, config);
        List<Document> documents = new ArrayList<>();
        List<Tuple<Integer, Number>> values = new ArrayList<>();
        int numValues = atLeast(50);
        int docID = 0;
        for (int i = 0; i < numValues; i++) {
            int numDup = randomIntBetween(1, 3);
            for (int j = 0; j < numDup; j++) {
                Document document = new Document();
                Number nextValue = randomNumber.get();
                values.add(new Tuple<>(docID, nextValue));
                document.add(new StringField("id", Integer.toString(docID), Field.Store.NO));
                document.add(pointFieldFunc.apply(nextValue));
                document.add(pointFieldFunc.apply(nextValue));
                documents.add(document);
                docID++;
            }
        }
        // insert some documents without a value for the metric field.
        for (int i = 0; i < 3; i++) {
            Document document = new Document();
            documents.add(document);
        }
        indexWriter.addDocuments(documents);
        Collections.sort(values, Comparator.comparingDouble(t -> t.v2().doubleValue()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(values.get(0).v2()));
        }
        for (int i = 1; i < values.size(); i++) {
            indexWriter.deleteDocuments(new Term("id", values.get(i - 1).v1().toString()));
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
                assertThat(res, equalTo(values.get(i).v2()));
            }
        }
        indexWriter.deleteDocuments(new Term("id", values.get(values.size() - 1).v1().toString()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(null));
        }
        indexWriter.close();
        directory.close();
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalMin> verify)
        throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Arrays.asList(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MinAggregationBuilder("foo").field(fieldName);
    }

    public void testDoReset() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.addDocument(singleton(new NumericDocValuesField("value", 5)));
        indexWriter.addDocument(singleton(new NumericDocValuesField("value", 10)));
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("value");

        MinAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);

        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();

        InternalMin result1 = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(5.0, result1.getValue(), 0);

        aggregator.doReset();

        InternalMin result2 = (InternalMin) aggregator.buildAggregation(0L);
        assertEquals(Double.POSITIVE_INFINITY, result2.getValue(), 0);

        indexReader.close();
        directory.close();
    }

    /**
     * Property test for result equivalence between skiplist and standard collectors.
     * Feature: min-aggregator-skiplist, Property 19: Result equivalence
     * Validates: Requirements 6.2, 6.3, 6.4
     *
     * For any set of documents and bucket configuration, the minimum values computed by the
     * skiplist collector should equal the minimum values computed by the standard collector.
     */
    public void testSkiplistResultEquivalence() throws IOException {
        // Run the property test multiple times with different random data
        int iterations = 100;
        for (int iter = 0; iter < iterations; iter++) {
            // Generate random test parameters
            int numDocs = randomIntBetween(10, 20);
            long actualMin = Long.MAX_VALUE;
            boolean includeNegatives = randomBoolean();
            boolean includeMissingValues = randomBoolean();
            double missingValueProbability = includeMissingValues ? randomDoubleBetween(0.1, 0.3, true) : 0.0;

            // Generate random document values with filter field
            List<Long> docValues = new ArrayList<>();
            List<String> filterValues = new ArrayList<>();
            for (int i = 0; i < numDocs; i++) {
                if (random().nextDouble() < missingValueProbability) {
                    // Skip this document (no value)
                    docValues.add(null);
                    filterValues.add(randomBoolean() ? "a" : "b");
                } else {
                    long value;
                    if (includeNegatives) {
                        value = randomLongBetween(-10000, 10000);
                    } else {
                        value = randomLongBetween(0, 10000);
                    }
                    docValues.add(value);
                    // Randomly assign filter value "a" or "b"
                    if (randomBoolean()) {
                        filterValues.add("a");
                        actualMin = Math.min(actualMin, value);
                    } else {
                        filterValues.add("b");
                    }
                }
            }

            // Test with skiplist-enabled field (single-valued with doc values and points)
            try (Directory skiplistDir = newDirectory()) {
                IndexWriterConfig skiplistConfig = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
                try (IndexWriter skiplistWriter = new IndexWriter(skiplistDir, skiplistConfig)) {
                    for (int i = 0; i < docValues.size(); i++) {
                        Long value = docValues.get(i);
                        String filterValue = filterValues.get(i);
                        Document doc = new Document();
                        if (value != null) {
                            doc.add(SortedNumericDocValuesField.indexedField("number", value));
                        }
                        // Add filter field
                        doc.add(new StringField("filterField", filterValue, Field.Store.NO));
                        skiplistWriter.addDocument(doc);
                    }
                }

                // Test with standard field (multi-valued, no points - forces standard collector)
                try (Directory standardDir = newDirectory()) {
                    IndexWriterConfig standardConfig = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
                    try (IndexWriter standardWriter = new IndexWriter(standardDir, standardConfig)) {
                        for (int i = 0; i < docValues.size(); i++) {
                            Long value = docValues.get(i);
                            String filterValue = filterValues.get(i);
                            Document doc = new Document();
                            if (value != null) {
                                // Use SortedNumericDocValuesField to force multi-valued collector
                                doc.add(new SortedNumericDocValuesField("number", value));
                            }
                            // Add filter field
                            doc.add(new StringField("filterField", filterValue, Field.Store.NO));
                            standardWriter.addDocument(doc);
                        }
                    }

                    try (
                        IndexReader skiplistReader = DirectoryReader.open(skiplistDir);
                        IndexReader standardReader = DirectoryReader.open(standardDir)
                    ) {
                        IndexSearcher skiplistSearcher = newSearcher(skiplistReader, true, true);
                        IndexSearcher standardSearcher = newSearcher(standardReader, true, true);

                        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                            "number",
                            NumberFieldMapper.NumberType.LONG
                        );
                        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

                        // Create filter query to only include documents with filterField:a
                        Query filterQuery = new org.apache.lucene.search.TermQuery(new Term("filterField", "a"));

                        // Execute aggregation with skiplist optimization
                        InternalMin skiplistResult = searchAndReduce(skiplistSearcher, filterQuery, aggregationBuilder, fieldType);

                        // Execute aggregation with standard collector
                        InternalMin standardResult = searchAndReduce(standardSearcher, filterQuery, aggregationBuilder, fieldType);

                        // Verify results are equivalent
                        assertEquals(
                            "Iteration " + iter + ": Skiplist and standard collectors should produce the same minimum value: " + actualMin,
                            standardResult.getValue(),
                            skiplistResult.getValue(),
                            0.0
                        );

                        // Verify both have the same "has value" status
                        assertEquals(
                            "Iteration " + iter + ": Skiplist and standard collectors should have the same 'has value' status",
                            AggregationInspectionHelper.hasValue(standardResult),
                            AggregationInspectionHelper.hasValue(skiplistResult)
                        );
                    }
                }
            }
        }
    }

    /**
     * Test skiplist optimization with terms aggregation and min sub-aggregation.
     */
    public void testSkiplistWithTermsAggregation() throws IOException {
        // Create terms aggregation with min sub-aggregation
        String categoryField = "category";
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("categories").field(categoryField)
            .subAggregation(new MinAggregationBuilder("min_value").field("value"));

        MappedFieldType categoryFieldType = new KeywordFieldMapper.KeywordFieldType(categoryField);
        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.LONG);

        testCase(termsAgg, new MatchAllDocsQuery(), iw -> {
            // Category "a": values 10, 15, 20 -> min should be 10
            for (int i = 0; i < 4096 * 2; i++) {
                Document doc1 = new Document();
                doc1.add(new StringField(categoryField, "a", Field.Store.NO));
                doc1.add(new SortedSetDocValuesField(categoryField, new BytesRef("a")));
                doc1.add(SortedNumericDocValuesField.indexedField("value", 10));
                iw.addDocument(doc1);

                Document doc2 = new Document();
                doc2.add(new StringField(categoryField, "a", Field.Store.NO));
                doc2.add(new SortedSetDocValuesField(categoryField, new BytesRef("a")));
                doc2.add(SortedNumericDocValuesField.indexedField("value", 15));
                iw.addDocument(doc2);

                Document doc3 = new Document();
                doc3.add(new StringField(categoryField, "a", Field.Store.NO));
                doc3.add(new SortedSetDocValuesField(categoryField, new BytesRef("a")));
                doc3.add(SortedNumericDocValuesField.indexedField("value", 20));
                iw.addDocument(doc3);
            }
            // Category "b": values 5, 25, 30 -> min should be 5
            for (int i = 0; i < 4096 * 2; i++) {
                Document doc4 = new Document();
                doc4.add(new StringField(categoryField, "b", Field.Store.NO));
                doc4.add(new SortedSetDocValuesField(categoryField, new BytesRef("b")));
                doc4.add(SortedNumericDocValuesField.indexedField("value", 5));
                iw.addDocument(doc4);

                Document doc5 = new Document();
                doc5.add(new StringField(categoryField, "b", Field.Store.NO));
                doc5.add(new SortedSetDocValuesField(categoryField, new BytesRef("b")));
                doc5.add(SortedNumericDocValuesField.indexedField("value", 25));
                iw.addDocument(doc5);

                Document doc6 = new Document();
                doc6.add(new StringField(categoryField, "b", Field.Store.NO));
                doc6.add(new SortedSetDocValuesField(categoryField, new BytesRef("b")));
                doc6.add(SortedNumericDocValuesField.indexedField("value", 30));
                iw.addDocument(doc6);
            }
            // Category "c": values 100, 200 -> min should be 100
                for (int i = 0; i < 4096 * 2; i++) {
                    Document doc7 = new Document();
                    doc7.add(new StringField(categoryField, "c", Field.Store.NO));
                    doc7.add(new SortedSetDocValuesField(categoryField, new BytesRef("c")));
                    doc7.add(SortedNumericDocValuesField.indexedField("value", 100));
                    iw.addDocument(doc7);

                    Document doc8 = new Document();
                    doc8.add(new StringField(categoryField, "c", Field.Store.NO));
                    doc8.add(new SortedSetDocValuesField(categoryField, new BytesRef("c")));
                    doc8.add(SortedNumericDocValuesField.indexedField("value", 200));
                    iw.addDocument(doc8);
                }
        }, (Consumer<InternalTerms<?, ?>>) result -> {
            // Verify we have 3 buckets
            assertEquals(3, result.getBuckets().size());

            // Find and verify each category bucket
            Map<String, InternalMin> minByCategory = new HashMap<>();
            for (Object bucketObj : result.getBuckets()) {
                Terms.Bucket bucket = (Terms.Bucket) bucketObj;
                String category = bucket.getKeyAsString();
                InternalMin min = bucket.getAggregations().get("min_value");
                minByCategory.put(category, min);
            }

            // Assert category "a" has min value of 10
            assertTrue("Category 'a' should exist", minByCategory.containsKey("a"));
            assertEquals("Category 'a' min should be 10", 10.0, minByCategory.get("a").getValue(), 0.0);
            assertTrue("Category 'a' min should have value", AggregationInspectionHelper.hasValue(minByCategory.get("a")));

            // Assert category "b" has min value of 5
            assertTrue("Category 'b' should exist", minByCategory.containsKey("b"));
            assertEquals("Category 'b' min should be 5", 5.0, minByCategory.get("b").getValue(), 0.0);
            assertTrue("Category 'b' min should have value", AggregationInspectionHelper.hasValue(minByCategory.get("b")));

            // Assert category "c" has min value of 100
            assertTrue("Category 'c' should exist", minByCategory.containsKey("c"));
            assertEquals("Category 'c' min should be 100", 100.0, minByCategory.get("c").getValue(), 0.0);
            assertTrue("Category 'c' min should have value", AggregationInspectionHelper.hasValue(minByCategory.get("c")));
        }, categoryFieldType, valueFieldType);
    }
}
