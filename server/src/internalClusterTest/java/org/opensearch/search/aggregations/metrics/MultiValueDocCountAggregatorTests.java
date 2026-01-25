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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.HllFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
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
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.singleton;

public class MultiValueDocCountAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    private static final String STRING_VALUE_SCRIPT = "string_value";
    private static final String NUMBER_VALUE_SCRIPT = "number_value";
    private static final String SINGLE_SCRIPT = "single";
    private static final String MULTI_SCRIPT = "multi";

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MultiValueDocCountAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Arrays.asList(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.BYTES,
            CoreValuesSourceType.IP,
            CoreValuesSourceType.GEOPOINT,
            CoreValuesSourceType.RANGE
        );
    }

    @Override
    protected List<String> unsupportedMappedFieldTypes() {
        return List.of(HllFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(STRING_VALUE_SCRIPT, vars -> (Double.valueOf((String) vars.get("_value")) + 1));
        scripts.put(NUMBER_VALUE_SCRIPT, vars -> (((Number) vars.get("_value")).doubleValue() + 1));
        scripts.put(SINGLE_SCRIPT, vars -> 1);
        scripts.put(MULTI_SCRIPT, vars -> Arrays.asList(1, 2));

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testKeywordsField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.STRING, iw -> {
            Document document1 = new Document();
            document1.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Electronics")));
            document1.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Computers")));
            iw.addDocument(document1);

            Document document2 = new Document();
            document2.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Books")));
            iw.addDocument(document2);

            Document document3 = new Document();
            document3.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Electronics")));
            document3.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Phones")));
            document3.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Accessories")));
            iw.addDocument(document3);

            Document document4 = new Document();
            document4.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("Clothing")));
            iw.addDocument(document4);
        }, count -> {
            assertEquals(2, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testDoubleField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.DOUBLE, iw -> {
            Document doc1 = new Document();
            doc1.add(new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(23D)));
            iw.addDocument(doc1);

            for (int i = 0; i < 3; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(23D + i)));
                document.add(new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(2.3D + i)));
                document.add(new SortedNumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(14D + i)));
                iw.addDocument(document);
            }
        }, count -> { assertEquals(3L, count.getValue()); });
    }

    public void testKeywordField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.STRING, iw -> {
            // document1 has single value.
            Document doc1 = new Document();
            doc1.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("single1-1")));
            iw.addDocument(doc1);

            // document2 and 3 have multiple values.
            Document doc2 = new Document();
            doc2.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("multi2-1")));
            doc2.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("multi2-2")));
            iw.addDocument(doc2);

            Document doc3 = new Document();
            doc3.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("multi2-1")));
            doc3.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("multi2-2")));
            doc3.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("multi2-3")));
            iw.addDocument(doc3);
        }, count -> {
            assertEquals(2L, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testNoDocs() throws IOException {
        for (ValueType valueType : ValueType.values()) {
            testAggregation(new MatchAllDocsQuery(), valueType, iw -> {

            }, count -> {
                assertEquals(0L, count.getValue());
                assertFalse(AggregationInspectionHelper.hasValue(count));
            });
        }
    }

    public void testSomeMatchesSortNumericDocValues() throws IOException {
        testAggregation(new FieldExistsQuery(FIELD_NAME), ValueType.NUMBER, iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 1)));
        }, count -> {
            assertEquals(0L, count.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("level", 0, 5), ValueType.STRING, iw -> {
            for (int i = 0; i < 7; i++) {
                Document document = new Document();
                document.add(new IntPoint("level", i));
                document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("sample1")));
                document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("sample2")));
                iw.addDocument(document);
            }
            for (int i = 7; i < 10; i++) {
                Document document = new Document();
                document.add(new IntPoint("level", i));
                document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("sample1")));
                iw.addDocument(document);
            }
        }, count -> {
            assertEquals(6L, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("level", -1, 0), ValueType.STRING, iw -> {
            Document document = new Document();
            document.add(new IntPoint("level", 3));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("foo")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("foo1")));
            iw.addDocument(document);
        }, count -> {
            assertEquals(0L, count.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testUnmappedMissingString() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").field("number")
            .missing(List.of("unmapped1", "unmapped2"));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < 3; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("unrelatedField", 7));
                document.add(new SortedNumericDocValuesField("unrelatedField", 8));
                document.add(new SortedNumericDocValuesField("unrelatedField", 9));
                iw.addDocument(document);
            }
        }, valueCount -> {
            assertEquals(0, valueCount.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(valueCount));
        }, null);
    }

    public void testUnmappedMissingNumber() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").field("number")
            .missing(1234);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < 3; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("unrelatedField", 7));
                document.add(new SortedNumericDocValuesField("unrelatedField", 8));
                document.add(new SortedNumericDocValuesField("unrelatedField", 9));
                iw.addDocument(document);
            }
        }, valueCount -> {
            assertEquals(0, valueCount.getValue());
            assertFalse((AggregationInspectionHelper.hasValue(valueCount)));
        }, null);
    }

    public void testUnmappedMissingGeoPoint() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").field("number")
            .missing(new GeoPoint(42.39561, -71.13051));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, valueCount -> {
            assertEquals(0, valueCount.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(valueCount));
        }, null);
    }

    public void testRangeFieldValues() throws IOException {
        RangeType rangeType = RangeType.DOUBLE;
        final RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(rangeType, 6.0D, 10.0D, true, true);
        final String fieldName = "rangeField";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);
        final MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("_name").field(fieldName);
        Set<RangeFieldMapper.Range> multiRecord = new HashSet<>(2);
        multiRecord.add(range1);
        multiRecord.add(range2);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            Document document = new Document();
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range1)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(singleton(range2)))));
            iw.addDocument(singleton(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(multiRecord))));
        }, count -> {
            assertEquals(0, count.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(count));
        }, fieldType);
    }

    public void testNoMultiValuesDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.STRING, iw -> {
            for (int i = 0; i < 10; i++) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("stringValue" + i)));
                iw.addDocument(document);
            }
        }, count -> assertEquals(0L, count.getValue()));
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), ValueType.LONG, iw -> {
            Document document = new Document();
            document.add(new SortedNumericDocValuesField("wrong_number", 7));
            document.add(new SortedNumericDocValuesField("wrong_number", 10));
            iw.addDocument(document);
        }, count -> {
            assertEquals(0L, count.getValue());
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testValueScriptNumber() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").field(FIELD_NAME)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, NUMBER_VALUE_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.NUMERIC);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < 3; i++) {
                Document document1 = new Document();
                document1.add(new SortedNumericDocValuesField(FIELD_NAME, i * 10 + i));
                document1.add(new SortedNumericDocValuesField(FIELD_NAME, i * 10 + i));
                document1.add(new SortedNumericDocValuesField(FIELD_NAME, i * 10 + i));
                iw.addDocument(document1);
            }
        }, valueCount -> {
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    public void testValueScriptString() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").field(FIELD_NAME)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, STRING_VALUE_SCRIPT, Collections.emptyMap()));

        MappedFieldType mappedFieldType = createMappedFieldType(FIELD_NAME, ValueType.STRING);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < 3; i++) {
                Document document1 = new Document();
                document1.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("1" + i)));
                document1.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("2" + i)));
                document1.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("3" + i)));
                iw.addDocument(document1);
            }
        }, count -> {
            assertEquals(3, count.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(count));
        }, mappedFieldType);
    }

    public void testSingleScriptString() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SINGLE_SCRIPT, Collections.emptyMap())
        );

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.STRING);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            Document document = new Document();
            // Note: unlike numerics, lucene de-dupes strings so we increment here
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("1")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("2")));
            iw.addDocument(document);

            document = new Document();
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("3")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("4")));
            iw.addDocument(document);

            document = new Document();
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("5")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("6")));
            iw.addDocument(document);
        }, valueCount -> {
            // Note: The field values won't be taken into account. The script will only be called
            // Every document return 1. So, multivalue_count is zero
            assertEquals(0, valueCount.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    public void testMultiScriptString() throws IOException {
        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("name").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, MULTI_SCRIPT, Collections.emptyMap())
        );

        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, ValueType.STRING);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            Document document = new Document();
            // Note: unlike numerics, lucene de-dupes strings so we increment here
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("1")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("2")));
            iw.addDocument(document);

            document = new Document();
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("3")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("4")));
            iw.addDocument(document);

            document = new Document();
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("5")));
            document.add(new SortedSetDocValuesField(FIELD_NAME, new BytesRef("6")));
            iw.addDocument(document);
        }, valueCount -> {
            // Note: The field values won't be taken into account. The script will only be called
            // Every document return 1. So, multivalue_count is zero
            assertEquals(3, valueCount.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(valueCount));
        }, fieldType);
    }

    private void testAggregation(
        Query query,
        ValueType valueType,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        Consumer<InternalValueCount> verify
    ) throws IOException {
        testAggregation(query, valueType, indexer, verify, true);
        testAggregation(query, valueType, indexer, verify, false);
    }

    private void testAggregation(
        Query query,
        ValueType valueType,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        Consumer<InternalValueCount> verify,
        boolean testWithHint
    ) throws IOException {
        MappedFieldType fieldType = createMappedFieldType(FIELD_NAME, valueType);

        MultiValueDocCountAggregationBuilder aggregationBuilder = new MultiValueDocCountAggregationBuilder("_name");
        if (valueType != null && testWithHint) {
            aggregationBuilder.userValueTypeHint(valueType);
        }
        aggregationBuilder.field(FIELD_NAME);

        testAggregation(aggregationBuilder, query, indexer, verify, fieldType);
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalValueCount> verify,
        MappedFieldType fieldType
    ) throws IOException {
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private static MappedFieldType createMappedFieldType(String name, ValueType valueType) {
        switch (valueType) {
            case BOOLEAN:
                return new BooleanFieldMapper.BooleanFieldType(name);
            case STRING:
                return new KeywordFieldMapper.KeywordFieldType(name);
            case DOUBLE:
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.DOUBLE);
            case NUMBER:
            case NUMERIC:
            case LONG:
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
            case UNSIGNED_LONG:
                return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.UNSIGNED_LONG);
            case DATE:
                return new DateFieldMapper.DateFieldType(name);
            case IP:
                return new IpFieldMapper.IpFieldType(name);
            case GEOPOINT:
                return new GeoPointFieldMapper.GeoPointFieldType(name);
            case RANGE:
                return new RangeFieldMapper.RangeFieldType(name, RangeType.DOUBLE);
            default:
                throw new IllegalArgumentException("Test does not support value type [" + valueType + "]");
        }
    }
}
