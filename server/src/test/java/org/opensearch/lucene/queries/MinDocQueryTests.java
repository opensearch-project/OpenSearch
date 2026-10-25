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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class MinDocQueryTests extends OpenSearchTestCase {

    public void testBasics() {
        MinDocQuery query1 = new MinDocQuery(42);
        MinDocQuery query2 = new MinDocQuery(42);
        MinDocQuery query3 = new MinDocQuery(43);
        QueryUtils.check(query1);
        QueryUtils.checkEqual(query1, query2);
        QueryUtils.checkUnequal(query1, query3);

        MinDocQuery query4 = new MinDocQuery(42, new Object());
        MinDocQuery query5 = new MinDocQuery(42, new Object());
        QueryUtils.checkUnequal(query4, query5);
    }

    public void testRewrite() throws Exception {
        IndexReader reader = new MultiReader();
        IndexSearcher searcher = new IndexSearcher(reader);
        MinDocQuery query = new MinDocQuery(42);
        Query rewritten = query.rewrite(searcher);
        QueryUtils.checkUnequal(query, rewritten);
        Query rewritten2 = rewritten.rewrite(searcher);
        assertSame(rewritten, rewritten2);
    }

    public void testRandom() throws IOException {
        final int numDocs = randomIntBetween(10, 200);
        final Document doc = new Document();
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(doc);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i <= numDocs; ++i) {
            assertEquals(numDocs - i, searcher.count(new MinDocQuery(i)));
        }
        w.close();
        reader.close();
        dir.close();
    }

    public void testRangeIterator() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < 128; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                for (int minDoc : new int[] { -1, 0, 31, 127 }) {
                    Weight weight = searcher.createWeight(searcher.rewrite(new MinDocQuery(minDoc)), ScoreMode.COMPLETE_NO_SCORES, 1f);
                    DocIdSetIterator iterator = weight.scorer(reader.leaves().get(0)).iterator();
                    int start = Math.max(0, minDoc);
                    assertEquals(-1, iterator.docID());
                    assertEquals(128 - start, iterator.cost());
                    assertEquals(start, iterator.nextDoc());
                    assertEquals(128, iterator.docIDRunEnd());
                    FixedBitSet bits = new FixedBitSet(128);
                    iterator.intoBitSet(128, bits, 0);
                    assertEquals(128 - start, bits.cardinality());
                    assertEquals(start, bits.nextSetBit(0));
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.docID());
                }
                for (int minDoc : new int[] { 128, 129 }) {
                    Weight weight = searcher.createWeight(searcher.rewrite(new MinDocQuery(minDoc)), ScoreMode.COMPLETE_NO_SCORES, 1f);
                    assertNull(weight.scorerSupplier(reader.leaves().get(0)));
                }
            }
        }
    }

}
