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

package org.opensearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.nested;
import static org.opensearch.search.aggregations.AggregationBuilders.reverseNested;
import static org.hamcrest.Matchers.equalTo;

public class ReverseNestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String REVERSE_AGG_NAME = "reverseNestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";

    /**
     * For each provided field type, we also register an alias with name {@code <field>-alias}.
     */
    @Override
    protected Map<String, MappedFieldType> getFieldAliases(MappedFieldType... fieldTypes) {
        return Arrays.stream(fieldTypes).collect(Collectors.toMap(ft -> ft.name() + "-alias", Function.identity()));
    }

    /**
     * Nested aggregations need the {@linkplain DirectoryReader} wrapped.
     */
    @Override
    protected IndexReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return wrapInMockESDirectoryReader(reader);
    }

    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                ReverseNestedAggregationBuilder reverseNestedBuilder = new ReverseNestedAggregationBuilder(REVERSE_AGG_NAME);
                nestedBuilder.subAggregation(reverseNestedBuilder);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                reverseNestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                Nested nested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), nestedBuilder, fieldType);
                ReverseNested reverseNested = (ReverseNested) ((InternalAggregation) nested).getProperty(REVERSE_AGG_NAME);
                assertEquals(REVERSE_AGG_NAME, reverseNested.getName());
                assertEquals(0, reverseNested.getDocCount());

                InternalMax max = (InternalMax) ((InternalAggregation) reverseNested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testMaxFromParentDocs() throws IOException {
        int numParentDocs = randomIntBetween(1, 20);
        int expectedParentDocs = 0;
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numParentDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    for (int nested = 0; nested < numNestedDocs; nested++) {
                        Document document = new Document();
                        document.add(
                            new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.NESTED_FIELD_TYPE)
                        );
                        document.add(new Field(NestedPathFieldMapper.NAME, NESTED_OBJECT, NestedPathFieldMapper.Defaults.FIELD_TYPE));
                        documents.add(document);
                        expectedNestedDocs++;
                    }
                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    long value = randomNonNegativeLong() % 10000;
                    document.add(SortedNumericDocValuesField.indexedField(VALUE_FIELD_NAME, value));
                    document.add(SeqNoFieldMapper.SequenceIDFields.emptySeqID().primaryTerm);
                    if (numNestedDocs > 0) {
                        expectedMaxValue = Math.max(expectedMaxValue, value);
                        expectedParentDocs++;
                    }
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                ReverseNestedAggregationBuilder reverseNestedBuilder = new ReverseNestedAggregationBuilder(REVERSE_AGG_NAME);
                nestedBuilder.subAggregation(reverseNestedBuilder);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                reverseNestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                Nested nested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), nestedBuilder, fieldType);
                assertEquals(expectedNestedDocs, nested.getDocCount());

                ReverseNested reverseNested = (ReverseNested) ((InternalAggregation) nested).getProperty(REVERSE_AGG_NAME);
                assertEquals(REVERSE_AGG_NAME, reverseNested.getName());
                assertEquals(expectedParentDocs, reverseNested.getDocCount());

                InternalMax max = (InternalMax) ((InternalAggregation) reverseNested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testFieldAlias() throws IOException {
        int numParentDocs = randomIntBetween(1, 20);
        int expectedParentDocs = 0;

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numParentDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    if (numNestedDocs > 0) {
                        expectedParentDocs++;
                    }

                    for (int nested = 0; nested < numNestedDocs; nested++) {
                        Document document = new Document();
                        document.add(
                            new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.NESTED_FIELD_TYPE)
                        );
                        document.add(new Field(NestedPathFieldMapper.NAME, NESTED_OBJECT, NestedPathFieldMapper.Defaults.FIELD_TYPE));
                        documents.add(document);
                    }
                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));

                    long value = randomNonNegativeLong() % 10000;
                    document.add(SortedNumericDocValuesField.indexedField(VALUE_FIELD_NAME, value));
                    document.add(SeqNoFieldMapper.SequenceIDFields.emptySeqID().primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }

            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                MaxAggregationBuilder maxAgg = max(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                MaxAggregationBuilder aliasMaxAgg = max(MAX_AGG_NAME).field(VALUE_FIELD_NAME + "-alias");

                NestedAggregationBuilder agg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    reverseNested(REVERSE_AGG_NAME).subAggregation(maxAgg)
                );
                NestedAggregationBuilder aliasAgg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    reverseNested(REVERSE_AGG_NAME).subAggregation(aliasMaxAgg)
                );

                Nested nested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), agg, fieldType);
                Nested aliasNested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), aliasAgg, fieldType);

                ReverseNested reverseNested = nested.getAggregations().get(REVERSE_AGG_NAME);
                ReverseNested aliasReverseNested = aliasNested.getAggregations().get(REVERSE_AGG_NAME);

                assertEquals(reverseNested, aliasReverseNested);
                assertEquals(expectedParentDocs, reverseNested.getDocCount());
            }
        }
    }

    public void testNestedUnderTerms() throws IOException {
        int numProducts = scaledRandomIntBetween(1, 100);
        int numResellers = scaledRandomIntBetween(1, 100);

        AggregationBuilder b = new NestedAggregationBuilder("nested", "nested_reseller").subAggregation(
            new TermsAggregationBuilder("resellers").field("reseller_id")
                .size(numResellers)
                .subAggregation(
                    new ReverseNestedAggregationBuilder("reverse_nested").subAggregation(
                        new TermsAggregationBuilder("products").field("product_id").size(numProducts)
                    )
                )
        );
        testCase(b, new MatchAllDocsQuery(), NestedAggregatorTests.buildResellerData(numProducts, numResellers), result -> {
            InternalNested nested = (InternalNested) result;
            assertThat(nested.getDocCount(), equalTo((long) numProducts * numResellers));
            LongTerms resellers = nested.getAggregations().get("resellers");
            assertThat(
                resellers.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                equalTo(LongStream.range(0, numResellers).mapToObj(Long::valueOf).collect(toList()))
            );
            for (int r = 0; r < numResellers; r++) {
                LongTerms.Bucket bucket = resellers.getBucketByKey(Integer.toString(r));
                assertThat(bucket.getDocCount(), equalTo((long) numProducts));
                InternalReverseNested reverseNested = bucket.getAggregations().get("reverse_nested");
                assertThat(reverseNested.getDocCount(), equalTo((long) numProducts));
                LongTerms products = reverseNested.getAggregations().get("products");
                assertThat(
                    products.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                    equalTo(LongStream.range(0, numProducts).mapToObj(Long::valueOf).collect(toList()))
                );
            }
        }, NestedAggregatorTests.resellersMappedFields());
    }

}
