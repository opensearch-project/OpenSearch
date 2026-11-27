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

package org.opensearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SearchAfterSortedDocQueryTests extends OpenSearchTestCase {

    public void testBasics() {
        Sort sort1 = new Sort(new SortedNumericSortField("field1", SortField.Type.INT), new SortedSetSortField("field2", false));
        Sort sort2 = new Sort(new SortedNumericSortField("field1", SortField.Type.INT), new SortedSetSortField("field3", false));
        FieldDoc fieldDoc1 = new FieldDoc(0, 0f, new Object[] { 5, new BytesRef("foo") });
        FieldDoc fieldDoc2 = new FieldDoc(0, 0f, new Object[] { 5, new BytesRef("foo") });

        SearchAfterSortedDocQuery query1 = new SearchAfterSortedDocQuery(sort1, fieldDoc1);
        SearchAfterSortedDocQuery query2 = new SearchAfterSortedDocQuery(sort1, fieldDoc2);
        SearchAfterSortedDocQuery query3 = new SearchAfterSortedDocQuery(sort2, fieldDoc2);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);
    }

    public void testInvalidSort() {
        Sort sort = new Sort(new SortedNumericSortField("field1", SortField.Type.INT));
        FieldDoc fieldDoc = new FieldDoc(0, 0f, new Object[] { 4, 5 });
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new SearchAfterSortedDocQuery(sort, fieldDoc));
        assertThat(ex.getMessage(), equalTo("after doc  has 2 value(s) but sort has 1."));
    }

    public void testRandom() throws IOException {
        final int numDocs = randomIntBetween(100, 200);
        final Document doc = new Document();
        final Directory dir = newDirectory();
        Sort sort = new Sort(
            new SortedNumericSortField("number1", SortField.Type.INT, randomBoolean()),
            new SortField("string", SortField.Type.STRING, randomBoolean())
        );
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setIndexSort(sort);
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir, config);
        for (int i = 0; i < numDocs; ++i) {
            int rand = randomIntBetween(0, 10);
            doc.add(new SortedNumericDocValuesField("number1", rand));
            doc.add(new SortedDocValuesField("string", new BytesRef(randomAlphaOfLength(randomIntBetween(5, 50)))));
            w.addDocument(doc);
            doc.clear();
            if (rarely()) {
                w.commit();
            }
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = new IndexSearcher(reader);

        int step = randomIntBetween(1, 10);
        FixedBitSet bitSet = new FixedBitSet(numDocs);
        TopDocs topDocs = null;
        for (int i = 0; i < numDocs;) {
            if (topDocs != null) {
                FieldDoc after = (FieldDoc) topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                topDocs = searcher.search(new SearchAfterSortedDocQuery(sort, after), step, sort);
            } else {
                topDocs = searcher.search(new MatchAllDocsQuery(), step, sort);
            }
            i += step;
            for (ScoreDoc topDoc : topDocs.scoreDocs) {
                int readerIndex = ReaderUtil.subIndex(topDoc.doc, reader.leaves());
                final LeafReaderContext leafReaderContext = reader.leaves().get(readerIndex);
                int docRebase = topDoc.doc - leafReaderContext.docBase;
                if (leafReaderContext.reader().hasDeletions()) {
                    assertTrue(leafReaderContext.reader().getLiveDocs().get(docRebase));
                }
                assertFalse(bitSet.get(topDoc.doc));
                bitSet.set(topDoc.doc);
            }
        }
        assertThat(bitSet.cardinality(), equalTo(reader.numDocs()));
        w.close();
        reader.close();
        dir.close();
    }

    public void testFilteredPagination() throws IOException {
        for (boolean reverse : new boolean[] { false, true }) {
            for (int segments : new int[] { 1, 4 }) {
                for (boolean deleteDocs : new boolean[] { false, true }) {
                    assertFilteredPagination(reverse, segments, deleteDocs);
                }
            }
        }
    }

    private void assertFilteredPagination(boolean reverse, int segments, boolean deleteDocs) throws IOException {
        Sort sort = new Sort(new SortField("number", SortField.Type.LONG, reverse));
        IndexWriterConfig config = new IndexWriterConfig().setIndexSort(sort).setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 256; i++) {
                Document doc = new Document();
                doc.add(new NumericDocValuesField("number", i / 3));
                doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                doc.add(new StringField("tag", i % 2 == 0 ? "even" : "odd", Field.Store.NO));
                writer.addDocument(doc);
                if ((i + 1) % (256 / segments) == 0) {
                    writer.commit();
                }
            }
            if (deleteDocs) {
                for (int i = 0; i < 256; i += 7) {
                    writer.deleteDocuments(new Term("id", Integer.toString(i)));
                }
            }
            try (IndexReader reader = org.apache.lucene.index.DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                Query filter = new TermQuery(new Term("tag", "even"));
                for (int pageSize : new int[] { 1, 10, 100 }) {
                    FieldDoc after = null;
                    int collected = 0;
                    while (true) {
                        TopDocs expected = searcher.searchAfter(after, filter, pageSize, sort);
                        Query query = after == null
                            ? filter
                            : new BooleanQuery.Builder().add(filter, BooleanClause.Occur.MUST)
                                .add(new SearchAfterSortedDocQuery(sort, after), BooleanClause.Occur.FILTER)
                                .build();
                        TopDocs actual = searcher.search(query, pageSize, sort);
                        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
                        if (actual.scoreDocs.length == 0) {
                            break;
                        }
                        for (int i = 0; i < actual.scoreDocs.length; i++) {
                            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
                            assertArrayEquals(((FieldDoc) expected.scoreDocs[i]).fields, ((FieldDoc) actual.scoreDocs[i]).fields);
                        }
                        collected += actual.scoreDocs.length;
                        after = (FieldDoc) actual.scoreDocs[actual.scoreDocs.length - 1];
                    }
                    assertEquals(searcher.count(filter), collected);
                }
                FieldDoc beforeAll = new FieldDoc(-1, Float.NaN, new Object[] { reverse ? Long.MAX_VALUE : Long.MIN_VALUE });
                Weight weight = searcher.createWeight(new SearchAfterSortedDocQuery(sort, beforeAll), ScoreMode.COMPLETE_NO_SCORES, 1f);
                for (LeafReaderContext leaf : reader.leaves()) {
                    DocIdSetIterator iterator = weight.scorer(leaf).iterator();
                    assertEquals(0, iterator.nextDoc());
                    assertEquals(leaf.reader().maxDoc(), iterator.docIDRunEnd());
                    FixedBitSet bits = new FixedBitSet(leaf.reader().maxDoc());
                    iterator.intoBitSet(leaf.reader().maxDoc(), bits, 0);
                    assertEquals(leaf.reader().maxDoc(), bits.cardinality());
                }
            }
        }
    }
}
