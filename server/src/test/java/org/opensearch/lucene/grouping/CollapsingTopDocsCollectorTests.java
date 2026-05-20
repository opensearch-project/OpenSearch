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

package org.opensearch.lucene.grouping;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MockFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CollapsingTopDocsCollectorTests extends OpenSearchTestCase {
    private static class SegmentSearcher extends IndexSearcher {
        private final List<LeafReaderContext> ctx;

        SegmentSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = Collections.singletonList(ctx);
        }

        public void search(Weight weight, Collector collector) throws IOException {
            LeafReaderContextPartition[] partitions = new LeafReaderContextPartition[ctx.size()];
            for (int i = 0; i < partitions.length; i++) {
                partitions[i] = LeafReaderContextPartition.createForEntireSegment(ctx.get(i));
            }
            search(partitions, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx.get(0) + ")";
        }
    }

    interface CollapsingDocValuesProducer<T extends Comparable<?>> {
        T randomGroup(int maxGroup);

        void add(Document doc, T value, boolean multivalued);

        SortField sortField(boolean multivalued);
    }

    <T extends Comparable<T>> void assertSearchCollapse(CollapsingDocValuesProducer<T> dvProducers, boolean numeric) throws IOException {
        assertSearchCollapse(dvProducers, numeric, true);
        assertSearchCollapse(dvProducers, numeric, false);
    }

    private <T extends Comparable<T>> void assertSearchCollapse(
        CollapsingDocValuesProducer<T> dvProducers,
        boolean numeric,
        boolean multivalued
    ) throws IOException {
        final int numDocs = randomIntBetween(1000, 2000);
        int maxGroup = randomIntBetween(2, 500);
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Set<T> values = new HashSet<>();
        int totalHits = 0;
        for (int i = 0; i < numDocs; i++) {
            final T value = dvProducers.randomGroup(maxGroup);
            values.add(value);
            Document doc = new Document();
            dvProducers.add(doc, value, multivalued);
            doc.add(new NumericDocValuesField("sort1", randomIntBetween(0, 10)));
            doc.add(new NumericDocValuesField("sort2", randomLong()));
            w.addDocument(doc);
            totalHits++;
        }

        List<T> valueList = new ArrayList<>(values);
        Collections.sort(valueList);
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);
        final SortField collapseField = dvProducers.sortField(multivalued);
        final SortField sort1 = new SortField("sort1", SortField.Type.INT);
        final SortField sort2 = new SortField("sort2", SortField.Type.LONG);
        Sort sort = new Sort(sort1, sort2, collapseField);

        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType(collapseField.getField());

        int expectedNumGroups = values.size();

        final CollapsingTopDocsCollector<?> collapsingCollector;
        if (numeric) {
            collapsingCollector = CollapsingTopDocsCollector.createNumeric(collapseField.getField(), fieldType, sort, expectedNumGroups);
        } else {
            collapsingCollector = CollapsingTopDocsCollector.createKeyword(collapseField.getField(), fieldType, sort, expectedNumGroups);
        }

        TopFieldCollector topFieldCollector = new TopFieldCollectorManager(sort, totalHits, null, Integer.MAX_VALUE, false).newCollector();
        Query query = new MatchAllDocsQuery();
        searcher.search(query, collapsingCollector);
        searcher.search(query, topFieldCollector);
        CollapseTopFieldDocs collapseTopFieldDocs = collapsingCollector.getTopDocs();
        TopFieldDocs topDocs = topFieldCollector.topDocs();
        assertEquals(collapseField.getField(), collapseTopFieldDocs.field);
        assertEquals(expectedNumGroups, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(totalHits, collapseTopFieldDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, collapseTopFieldDocs.totalHits.relation());
        assertEquals(totalHits, topDocs.scoreDocs.length);
        assertEquals(totalHits, topDocs.totalHits.value());

        Set<Object> seen = new HashSet<>();
        // collapse field is the last sort
        int collapseIndex = sort.getSort().length - 1;
        int topDocsIndex = 0;
        for (int i = 0; i < expectedNumGroups; i++) {
            FieldDoc fieldDoc = null;
            for (; topDocsIndex < totalHits; topDocsIndex++) {
                fieldDoc = (FieldDoc) topDocs.scoreDocs[topDocsIndex];
                if (seen.contains(fieldDoc.fields[collapseIndex]) == false) {
                    break;
                }
            }
            FieldDoc collapseFieldDoc = (FieldDoc) collapseTopFieldDocs.scoreDocs[i];
            assertNotNull(fieldDoc);
            assertEquals(collapseFieldDoc.doc, fieldDoc.doc);
            assertArrayEquals(collapseFieldDoc.fields, fieldDoc.fields);
            seen.add(fieldDoc.fields[fieldDoc.fields.length - 1]);
        }
        for (; topDocsIndex < totalHits; topDocsIndex++) {
            FieldDoc fieldDoc = (FieldDoc) topDocs.scoreDocs[topDocsIndex];
            assertTrue(seen.contains(fieldDoc.fields[collapseIndex]));
        }

        // check merge
        final IndexReaderContext ctx = searcher.getTopReaderContext();
        final SegmentSearcher[] subSearchers;
        final int[] docStarts;

        if (ctx instanceof LeafReaderContext) {
            subSearchers = new SegmentSearcher[1];
            docStarts = new int[1];
            subSearchers[0] = new SegmentSearcher((LeafReaderContext) ctx, ctx);
            docStarts[0] = 0;
        } else {
            final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
            final int size = compCTX.leaves().size();
            subSearchers = new SegmentSearcher[size];
            docStarts = new int[size];
            int docBase = 0;
            for (int searcherIDX = 0; searcherIDX < subSearchers.length; searcherIDX++) {
                final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
                subSearchers[searcherIDX] = new SegmentSearcher(leave, compCTX);
                docStarts[searcherIDX] = docBase;
                docBase += leave.reader().maxDoc();
            }
        }

        final CollapseTopFieldDocs[] shardHits = new CollapseTopFieldDocs[subSearchers.length];
        final Weight weight = searcher.createWeight(searcher.rewrite(new MatchAllDocsQuery()), ScoreMode.COMPLETE, 1f);
        for (int shardIDX = 0; shardIDX < subSearchers.length; shardIDX++) {
            final SegmentSearcher subSearcher = subSearchers[shardIDX];
            final CollapsingTopDocsCollector<?> c;
            if (numeric) {
                c = CollapsingTopDocsCollector.createNumeric(collapseField.getField(), fieldType, sort, expectedNumGroups);
            } else {
                c = CollapsingTopDocsCollector.createKeyword(collapseField.getField(), fieldType, sort, expectedNumGroups);
            }
            subSearcher.search(weight, c);
            shardHits[shardIDX] = c.getTopDocs();
        }
        CollapseTopFieldDocs mergedFieldDocs = CollapseTopFieldDocs.merge(sort, 0, expectedNumGroups, shardHits);
        assertTopDocsEquals(query, mergedFieldDocs, collapseTopFieldDocs);
        w.close();
        reader.close();
        dir.close();
    }

    private static void assertTopDocsEquals(Query query, CollapseTopFieldDocs topDocs1, CollapseTopFieldDocs topDocs2) {
        CheckHits.checkEqual(query, topDocs1.scoreDocs, topDocs2.scoreDocs);
        assertArrayEquals(topDocs1.collapseValues, topDocs2.collapseValues);
    }

    public void testCollapseLong() throws Exception {
        CollapsingDocValuesProducer<Long> producer = new CollapsingDocValuesProducer<Long>() {
            @Override
            public Long randomGroup(int maxGroup) {
                return randomNonNegativeLong() % maxGroup;
            }

            @Override
            public void add(Document doc, Long value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", value));
                } else {
                    doc.add(new NumericDocValuesField("field", value));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.LONG);
                } else {
                    return new SortField("field", SortField.Type.LONG);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseInt() throws Exception {
        CollapsingDocValuesProducer<Integer> producer = new CollapsingDocValuesProducer<Integer>() {
            @Override
            public Integer randomGroup(int maxGroup) {
                return randomIntBetween(0, maxGroup - 1);
            }

            @Override
            public void add(Document doc, Integer value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", value));
                } else {
                    doc.add(new NumericDocValuesField("field", value));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.INT);
                } else {
                    return new SortField("field", SortField.Type.INT);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseFloat() throws Exception {
        CollapsingDocValuesProducer<Float> producer = new CollapsingDocValuesProducer<Float>() {
            @Override
            public Float randomGroup(int maxGroup) {
                return Float.valueOf(randomIntBetween(0, maxGroup - 1));
            }

            @Override
            public void add(Document doc, Float value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", NumericUtils.floatToSortableInt(value)));
                } else {
                    doc.add(new NumericDocValuesField("field", Float.floatToIntBits(value)));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.FLOAT);
                } else {
                    return new SortField("field", SortField.Type.FLOAT);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseDouble() throws Exception {
        CollapsingDocValuesProducer<Double> producer = new CollapsingDocValuesProducer<Double>() {
            @Override
            public Double randomGroup(int maxGroup) {
                return Double.valueOf(randomIntBetween(0, maxGroup - 1));
            }

            @Override
            public void add(Document doc, Double value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                } else {
                    doc.add(new NumericDocValuesField("field", Double.doubleToLongBits(value)));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedNumericSortField("field", SortField.Type.DOUBLE);
                } else {
                    return new SortField("field", SortField.Type.DOUBLE);
                }
            }
        };
        assertSearchCollapse(producer, true);
    }

    public void testCollapseString() throws Exception {
        CollapsingDocValuesProducer<BytesRef> producer = new CollapsingDocValuesProducer<BytesRef>() {
            @Override
            public BytesRef randomGroup(int maxGroup) {
                return new BytesRef(Integer.toString(randomIntBetween(0, maxGroup - 1)));
            }

            @Override
            public void add(Document doc, BytesRef value, boolean multivalued) {
                if (multivalued) {
                    doc.add(new SortedSetDocValuesField("field", value));
                } else {
                    doc.add(new SortedDocValuesField("field", value));
                }
            }

            @Override
            public SortField sortField(boolean multivalued) {
                if (multivalued) {
                    return new SortedSetSortField("field", false);
                } else {
                    return new SortField("field", SortField.Type.STRING);
                }
            }
        };
        assertSearchCollapse(producer, false);
    }

    public void testEmptyNumericSegment() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new NumericDocValuesField("group", 0));
        w.addDocument(doc);
        doc.clear();
        doc.add(new NumericDocValuesField("group", 1));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new NumericDocValuesField("group", 10));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new NumericDocValuesField("category", 0));
        w.addDocument(doc);
        w.commit();
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");

        SortField sortField = new SortField("group", SortField.Type.LONG);
        sortField.setMissingValue(Long.MAX_VALUE);
        Sort sort = new Sort(sortField);

        final CollapsingTopDocsCollector<?> collapsingCollector = CollapsingTopDocsCollector.createNumeric("group", fieldType, sort, 10);
        searcher.search(new MatchAllDocsQuery(), collapsingCollector);
        CollapseTopFieldDocs collapseTopFieldDocs = collapsingCollector.getTopDocs();
        assertEquals(4, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(4, collapseTopFieldDocs.collapseValues.length);
        assertEquals(0L, collapseTopFieldDocs.collapseValues[0]);
        assertEquals(1L, collapseTopFieldDocs.collapseValues[1]);
        assertEquals(10L, collapseTopFieldDocs.collapseValues[2]);
        assertNull(collapseTopFieldDocs.collapseValues[3]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testEmptySortedSegment() throws Exception {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        Document doc = new Document();
        doc.add(new SortedDocValuesField("group", new BytesRef("0")));
        w.addDocument(doc);
        doc.clear();
        doc.add(new SortedDocValuesField("group", new BytesRef("1")));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new SortedDocValuesField("group", new BytesRef("10")));
        w.addDocument(doc);
        w.commit();
        doc.clear();
        doc.add(new NumericDocValuesField("category", 0));
        w.addDocument(doc);
        w.commit();
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");

        Sort sort = new Sort(new SortField("group", SortField.Type.STRING));

        final CollapsingTopDocsCollector<?> collapsingCollector = CollapsingTopDocsCollector.createKeyword("group", fieldType, sort, 10);
        searcher.search(new MatchAllDocsQuery(), collapsingCollector);
        CollapseTopFieldDocs collapseTopFieldDocs = collapsingCollector.getTopDocs();
        assertEquals(4, collapseTopFieldDocs.scoreDocs.length);
        assertEquals(4, collapseTopFieldDocs.collapseValues.length);
        assertNull(collapseTopFieldDocs.collapseValues[0]);
        assertEquals(new BytesRef("0"), collapseTopFieldDocs.collapseValues[1]);
        assertEquals(new BytesRef("1"), collapseTopFieldDocs.collapseValues[2]);
        assertEquals(new BytesRef("10"), collapseTopFieldDocs.collapseValues[3]);
        w.close();
        reader.close();
        dir.close();
    }

    public void testInconsistentShardIndicesException() {
        Sort sort = Sort.RELEVANCE;

        // Create TopDocs with mixed shardIndex values - some set, some -1
        ScoreDoc[] shard1Docs = {
            new FieldDoc(1, 9.0f, new Object[] { 9.0f }, 0), // shardIndex = 0
            new FieldDoc(2, 8.0f, new Object[] { 8.0f }, 0)  // shardIndex = 0
        };

        ScoreDoc[] shard2Docs = {
            new FieldDoc(3, 7.0f, new Object[] { 7.0f }, -1), // shardIndex = -1 (inconsistent!)
            new FieldDoc(4, 6.0f, new Object[] { 6.0f }, -1)  // shardIndex = -1
        };

        CollapseTopFieldDocs[] shardHits = {
            new CollapseTopFieldDocs(
                "field",
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                shard1Docs,
                sort.getSort(),
                new Object[] { "val1", "val2" }
            ),
            new CollapseTopFieldDocs(
                "field",
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                shard2Docs,
                sort.getSort(),
                new Object[] { "val3", "val4" }
            ) };

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            CollapseTopFieldDocs.merge(sort, 0, 10, shardHits);
        });

        assertEquals("Inconsistent order of shard indices", exception.getMessage());
    }

    public void testSearchAfterValidation() {
        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("category");

        // Test multiple sort fields - should fail
        Sort multiSort = new Sort(new SortField("category", SortField.Type.INT), new SortField("score", SortField.Type.FLOAT));
        FieldDoc multiAfter = new FieldDoc(0, Float.NaN, new Object[] { 1, 1.0f });
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            CollapsingTopDocsCollector.createNumeric("category", fieldType, multiSort, 10, multiAfter);
        });
        assertEquals("The after parameter can only be used when the sort is based on the collapse field", exception.getMessage());

        // Test wrong sort field - should fail
        Sort wrongSort = new Sort(new SortField("different_field", SortField.Type.INT));
        FieldDoc wrongAfter = new FieldDoc(0, Float.NaN, new Object[] { 1 });
        exception = expectThrows(IllegalArgumentException.class, () -> {
            CollapsingTopDocsCollector.createNumeric("category", fieldType, wrongSort, 10, wrongAfter);
        });
        assertEquals("The after parameter can only be used when the sort is based on the collapse field", exception.getMessage());

        // Test correct sort field - should succeed
        Sort correctSort = new Sort(new SortField("category", SortField.Type.INT));
        FieldDoc correctAfter = new FieldDoc(0, Float.NaN, new Object[] { 1 });
        CollapsingTopDocsCollector<?> collector = CollapsingTopDocsCollector.createNumeric(
            "category",
            fieldType,
            correctSort,
            10,
            correctAfter
        );
        assertNotNull(collector);

        // Test keyword field with multiple sorts - should fail
        MappedFieldType keywordFieldType = new MockFieldMapper.FakeFieldType("tag");
        Sort keywordMultiSort = new Sort(new SortField("tag", SortField.Type.STRING), new SortField("score", SortField.Type.FLOAT));
        FieldDoc keywordAfter = new FieldDoc(0, Float.NaN, new Object[] { "A", 1.0f });
        exception = expectThrows(IllegalArgumentException.class, () -> {
            CollapsingTopDocsCollector.createKeyword("tag", keywordFieldType, keywordMultiSort, 10, keywordAfter);
        });
        assertEquals("The after parameter can only be used when the sort is based on the collapse field", exception.getMessage());
    }

    public void testSearchAfterWithNumericCollapse() throws IOException {
        testSearchAfterCollapse(new NumericDVProducer(), true);
    }

    public void testSearchAfterWithKeywordCollapse() throws IOException {
        testSearchAfterCollapse(new KeywordDVProducer(), false);
    }

    private <T extends Comparable<T>> void testSearchAfterCollapse(CollapsingDocValuesProducer<T> dvProducer, boolean numeric)
        throws IOException {
        final int numDocs = 100;
        final int maxGroup = 10;
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            T groupValue = dvProducer.randomGroup(maxGroup);
            dvProducer.add(doc, groupValue, false);
            w.addDocument(doc);
        }

        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        SortField collapseField = dvProducer.sortField(false);
        // Use collapse field as sort field to satisfy validation
        Sort sort = new Sort(collapseField);
        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType(collapseField.getField());

        // First search without search_after
        CollapsingTopDocsCollector<?> collector1 = numeric
            ? CollapsingTopDocsCollector.createNumeric(collapseField.getField(), fieldType, sort, 5, null)
            : CollapsingTopDocsCollector.createKeyword(collapseField.getField(), fieldType, sort, 5, null);

        searcher.search(new MatchAllDocsQuery(), collector1);
        CollapseTopFieldDocs results1 = collector1.getTopDocs();

        assertTrue("Should have results", results1.scoreDocs.length > 0);

        // Use the last result as search_after
        FieldDoc after = (FieldDoc) results1.scoreDocs[results1.scoreDocs.length - 1];

        // Second search with search_after
        CollapsingTopDocsCollector<?> collector2 = numeric
            ? CollapsingTopDocsCollector.createNumeric(collapseField.getField(), fieldType, sort, 5, after)
            : CollapsingTopDocsCollector.createKeyword(collapseField.getField(), fieldType, sort, 5, after);

        searcher.search(new MatchAllDocsQuery(), collector2);
        CollapseTopFieldDocs results2 = collector2.getTopDocs();

        // Verify no overlap between pages
        Set<Integer> firstPageDocs = new HashSet<>();
        for (ScoreDoc doc : results1.scoreDocs) {
            firstPageDocs.add(doc.doc);
        }

        for (ScoreDoc doc : results2.scoreDocs) {
            assertFalse("No document should appear in both pages", firstPageDocs.contains(doc.doc));
        }

        w.close();
        reader.close();
        dir.close();
    }

    public void testSearchAfterWithEmptyResults() throws IOException {
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

        final int numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("group", i));
            w.addDocument(doc);
        }

        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        // Use collapse field as sort field to satisfy validation
        Sort sort = new Sort(new SortField("group", SortField.Type.INT));
        MappedFieldType fieldType = new MockFieldMapper.FakeFieldType("group");

        // Create search_after that's beyond all documents
        FieldDoc after = new FieldDoc(0, Float.NaN, new Object[] { 200 });

        CollapsingTopDocsCollector<?> collector = CollapsingTopDocsCollector.createNumeric("group", fieldType, sort, 10, after);

        searcher.search(new MatchAllDocsQuery(), collector);
        CollapseTopFieldDocs results = collector.getTopDocs();

        assertEquals("Should have no results after last document", 0, results.scoreDocs.length);
        assertEquals("Total hits should reflect all documents processed with search_after", numDocs, results.totalHits.value());

        w.close();
        reader.close();
        dir.close();
    }

    // Helper classes for test data
    private static class NumericDVProducer implements CollapsingDocValuesProducer<Long> {
        @Override
        public Long randomGroup(int maxGroup) {
            return (long) randomIntBetween(0, maxGroup - 1);
        }

        @Override
        public void add(Document doc, Long value, boolean multivalued) {
            doc.add(new NumericDocValuesField("group", value));
        }

        @Override
        public SortField sortField(boolean multivalued) {
            return new SortField("group", SortField.Type.LONG);
        }
    }

    private static class KeywordDVProducer implements CollapsingDocValuesProducer<BytesRef> {
        @Override
        public BytesRef randomGroup(int maxGroup) {
            return new BytesRef("group" + randomIntBetween(0, maxGroup - 1));
        }

        @Override
        public void add(Document doc, BytesRef value, boolean multivalued) {
            doc.add(new SortedDocValuesField("group", value));
        }

        @Override
        public SortField sortField(boolean multivalued) {
            return new SortField("group", SortField.Type.STRING);
        }
    }

}
