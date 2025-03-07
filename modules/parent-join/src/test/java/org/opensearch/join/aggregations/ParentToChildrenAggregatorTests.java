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

package org.opensearch.join.aggregations;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.join.ParentJoinModulePlugin;
import org.opensearch.join.mapper.MetaJoinFieldMapper;
import org.opensearch.join.mapper.ParentJoinFieldMapper;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParentToChildrenAggregatorTests extends AggregatorTestCase {

    private static final String CHILD_TYPE = "child_type";
    private static final String PARENT_TYPE = "parent_type";

    public void testNoDocs() throws IOException {
        Directory directory = newDirectory();

        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        // intentionally not writing any docs
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);

        testCase(new MatchAllDocsQuery(), newSearcher(indexReader, false, true), parentToChild -> {
            assertEquals(0L, parentToChild.getDocCount());
            assertEquals(
                Double.POSITIVE_INFINITY,
                ((InternalMin) parentToChild.getAggregations().get("in_child")).getValue(),
                Double.MIN_VALUE
            );
        });
        indexReader.close();
        directory.close();
    }

    public void testParentChild() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
        indexWriter.close();

        IndexReader indexReader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(new Index("foo", "_na_"), 1));
        // TODO set "maybeWrap" to true for IndexSearcher once #23338 is resolved
        IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

        testCase(new MatchAllDocsQuery(), indexSearcher, child -> {
            int expectedTotalChildren = 0;
            int expectedMinValue = Integer.MAX_VALUE;
            for (Tuple<Integer, Integer> expectedValues : expectedParentChildRelations.values()) {
                expectedTotalChildren += expectedValues.v1();
                expectedMinValue = Math.min(expectedMinValue, expectedValues.v2());
            }
            assertEquals(expectedTotalChildren, child.getDocCount());
            assertTrue(JoinAggregationInspectionHelper.hasValue(child));
            assertEquals(expectedMinValue, ((InternalMin) child.getAggregations().get("in_child")).getValue(), Double.MIN_VALUE);
        });

        for (String parent : expectedParentChildRelations.keySet()) {
            testCase(new TermInSetQuery(IdFieldMapper.NAME, Collections.singleton(Uid.encodeId(parent))), indexSearcher, child -> {
                assertEquals((long) expectedParentChildRelations.get(parent).v1(), child.getDocCount());
                assertEquals(
                    expectedParentChildRelations.get(parent).v2(),
                    ((InternalMin) child.getAggregations().get("in_child")).getValue(),
                    Double.MIN_VALUE
                );
            });
        }
        indexReader.close();
        directory.close();
    }

    public void testParentChildAsSubAgg() throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

            final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
            indexWriter.close();

            try (
                IndexReader indexReader = OpenSearchDirectoryReader.wrap(
                    DirectoryReader.open(directory),
                    new ShardId(new Index("foo", "_na_"), 1)
                )
            ) {
                IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

                AggregationBuilder request = new TermsAggregationBuilder("t").field("kwd")
                    .subAggregation(
                        new ChildrenAggregationBuilder("children", CHILD_TYPE).subAggregation(
                            new MinAggregationBuilder("min").field("number")
                        )
                    );

                long expectedEvenChildCount = 0;
                double expectedEvenMin = Double.MAX_VALUE;
                long expectedOddChildCount = 0;
                double expectedOddMin = Double.MAX_VALUE;
                for (Map.Entry<String, Tuple<Integer, Integer>> e : expectedParentChildRelations.entrySet()) {
                    if (Integer.valueOf(e.getKey().substring("parent".length())) % 2 == 0) {
                        expectedEvenChildCount += e.getValue().v1();
                        expectedEvenMin = Math.min(expectedEvenMin, e.getValue().v2());
                    } else {
                        expectedOddChildCount += e.getValue().v1();
                        expectedOddMin = Math.min(expectedOddMin, e.getValue().v2());
                    }
                }
                StringTerms result = searchAndReduce(
                    indexSearcher,
                    new MatchAllDocsQuery(),
                    request,
                    longField("number"),
                    keywordField("kwd")
                );

                StringTerms.Bucket evenBucket = result.getBucketByKey("even");
                InternalChildren evenChildren = evenBucket.getAggregations().get("children");
                InternalMin evenMin = evenChildren.getAggregations().get("min");
                assertThat(evenChildren.getDocCount(), equalTo(expectedEvenChildCount));
                assertThat(evenMin.getValue(), equalTo(expectedEvenMin));

                if (expectedOddChildCount > 0) {
                    StringTerms.Bucket oddBucket = result.getBucketByKey("odd");
                    InternalChildren oddChildren = oddBucket.getAggregations().get("children");
                    InternalMin oddMin = oddChildren.getAggregations().get("min");
                    assertThat(oddChildren.getDocCount(), equalTo(expectedOddChildCount));
                    assertThat(oddMin.getValue(), equalTo(expectedOddMin));
                } else {
                    assertNull(result.getBucketByKey("odd"));
                }
            }
        }
    }

    private static Map<String, Tuple<Integer, Integer>> setupIndex(RandomIndexWriter iw) throws IOException {
        Map<String, Tuple<Integer, Integer>> expectedValues = new HashMap<>();
        int numParents = randomIntBetween(1, 10);
        for (int i = 0; i < numParents; i++) {
            String parent = "parent" + i;
            iw.addDocument(createParentDocument(parent, i % 2 == 0 ? "even" : "odd"));
            int numChildren = randomIntBetween(1, 10);
            int minValue = Integer.MAX_VALUE;
            for (int c = 0; c < numChildren; c++) {
                int randomValue = randomIntBetween(0, 100);
                minValue = Math.min(minValue, randomValue);
                iw.addDocument(createChildDocument("child" + c + "_" + parent, parent, randomValue));
            }
            expectedValues.put(parent, new Tuple<>(numChildren, minValue));
        }
        return expectedValues;
    }

    private static List<Field> createParentDocument(String id, String kwd) {
        return Arrays.asList(
            new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO),
            new SortedSetDocValuesField("kwd", new BytesRef(kwd)),
            new StringField("join_field", PARENT_TYPE, Field.Store.NO),
            createJoinField(PARENT_TYPE, id)
        );
    }

    private static List<Field> createChildDocument(String childId, String parentId, int value) {
        return Arrays.asList(
            new StringField(IdFieldMapper.NAME, Uid.encodeId(childId), Field.Store.NO),
            new StringField("join_field", CHILD_TYPE, Field.Store.NO),
            createJoinField(PARENT_TYPE, parentId),
            new SortedNumericDocValuesField("number", value)
        );
    }

    private static SortedDocValuesField createJoinField(String parentType, String id) {
        return new SortedDocValuesField("join_field#" + parentType, new BytesRef(id));
    }

    @Override
    protected MapperService mapperServiceMock() {
        ParentJoinFieldMapper joinFieldMapper = createJoinFieldMapper();
        MapperService mapperService = mock(MapperService.class);
        MetaJoinFieldMapper.MetaJoinFieldType metaJoinFieldType = mock(MetaJoinFieldMapper.MetaJoinFieldType.class);
        when(metaJoinFieldType.getJoinField()).thenReturn("join_field");
        when(mapperService.fieldType("_parent_join")).thenReturn(metaJoinFieldType);
        MappingLookup fieldMappers = new MappingLookup(
            Collections.singleton(joinFieldMapper),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        DocumentMapper mockMapper = mock(DocumentMapper.class);
        when(mockMapper.mappers()).thenReturn(fieldMappers);
        when(mapperService.documentMapper()).thenReturn(mockMapper);
        return mapperService;
    }

    private static ParentJoinFieldMapper createJoinFieldMapper() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return new ParentJoinFieldMapper.Builder("join_field").addParent(PARENT_TYPE, Collections.singleton(CHILD_TYPE))
            .build(new Mapper.BuilderContext(settings, new ContentPath(0)));
    }

    private void testCase(Query query, IndexSearcher indexSearcher, Consumer<InternalChildren> verify) throws IOException {

        ChildrenAggregationBuilder aggregationBuilder = new ChildrenAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_child").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        InternalChildren result = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
        verify.accept(result);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new ParentJoinModulePlugin());
    }
}
