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

package org.opensearch.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.Before;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchCancellationTests extends OpenSearchTestCase {

    private static final String STRING_FIELD_NAME = "foo";
    private static final String POINT_FIELD_NAME = "point";

    private static Directory dir;
    private static IndexReader reader;
    private SearchContext searchContext;

    @BeforeClass
    public static void setup() throws IOException {
        dir = newDirectory();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        // we need at least 2 segments - so no merges should be allowed
        w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        w.setDoRandomForceMerge(false);
        indexRandomDocuments(w, TestUtil.nextInt(random(), 2, 20));
        w.flush();
        indexRandomDocuments(w, TestUtil.nextInt(random(), 1, 20));
        reader = w.getReader();
        w.close();
    }

    private static void indexRandomDocuments(RandomIndexWriter w, int numDocs) throws IOException {
        for (int i = 1; i <= numDocs; ++i) {
            Document doc = new Document();
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < i; j++) {
                sb.append('a');
            }
            doc.add(new StringField(STRING_FIELD_NAME, sb.toString(), Field.Store.NO));
            doc.add(new IntPoint(POINT_FIELD_NAME, i, i + 1));
            w.addDocument(doc);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException {
        IOUtils.close(reader, dir);
        dir = null;
        reader = null;
    }

    @Before
    public void testSetup() {
        searchContext = mock(SearchContext.class);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
    }

    public void testAddingCancellationActions() throws IOException {
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );
        NullPointerException npe = expectThrows(NullPointerException.class, () -> searcher.addQueryCancellation(null));
        assertEquals("cancellation runnable should not be null", npe.getMessage());

        Runnable r = () -> {};
        searcher.addQueryCancellation(r);
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> searcher.addQueryCancellation(r));
        assertEquals("Cancellation runnable already added", iae.getMessage());
    }

    public void testCancellableCollector() throws IOException {
        TotalHitCountCollector collector1 = new TotalHitCountCollector();
        Runnable cancellation = () -> { throw new TaskCancelledException("cancelled"); };
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        searcher.search(new MatchAllDocsQuery(), collector1);
        assertThat(collector1.getTotalHits(), equalTo(reader.numDocs()));

        searcher.addQueryCancellation(cancellation);
        expectThrows(TaskCancelledException.class, () -> searcher.search(new MatchAllDocsQuery(), collector1));

        searcher.removeQueryCancellation(cancellation);
        TotalHitCountCollector collector2 = new TotalHitCountCollector();
        searcher.search(new MatchAllDocsQuery(), collector2);
        assertThat(collector2.getTotalHits(), equalTo(reader.numDocs()));
    }

    public void testExitableDirectoryReader() throws IOException {
        AtomicBoolean cancelled = new AtomicBoolean(true);
        Runnable cancellation = () -> {
            if (cancelled.get()) {
                throw new TaskCancelledException("cancelled");
            }
        };
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );
        searcher.addQueryCancellation(cancellation);
        CompiledAutomaton automaton = new CompiledAutomaton(new RegExp("a.*").toAutomaton());

        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().terms(STRING_FIELD_NAME).iterator()
        );
        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().terms(STRING_FIELD_NAME).intersect(automaton, null)
        );
        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME)
        );
        expectThrows(
            TaskCancelledException.class,
            () -> searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME)
        );

        cancelled.set(false); // Avoid exception during construction of the wrapper objects
        Terms terms = searcher.getIndexReader().leaves().get(0).reader().terms(STRING_FIELD_NAME);
        TermsEnum termsIterator = terms.iterator();
        TermsEnum termsIntersect = terms.intersect(automaton, null);
        PointValues pointValues1 = searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME);
        cancelled.set(true);
        expectThrows(TaskCancelledException.class, termsIterator::next);
        expectThrows(TaskCancelledException.class, termsIntersect::next);
        expectThrows(TaskCancelledException.class, pointValues1::getDocCount);
        expectThrows(TaskCancelledException.class, pointValues1::getNumIndexDimensions);
        expectThrows(TaskCancelledException.class, () -> pointValues1.intersect(new PointValuesIntersectVisitor()));

        cancelled.set(false); // Avoid exception during construction of the wrapper objects
        // Re-initialize objects so that we reset the `calls` counter used to avoid cancellation check
        // on every iteration and assure that cancellation would normally happen if we hadn't removed the
        // cancellation runnable.
        termsIterator = terms.iterator();
        termsIntersect = terms.intersect(automaton, null);
        PointValues pointValues2 = searcher.getIndexReader().leaves().get(0).reader().getPointValues(POINT_FIELD_NAME);
        cancelled.set(true);
        searcher.removeQueryCancellation(cancellation);
        termsIterator.next();
        termsIntersect.next();
        pointValues2.getDocCount();
        pointValues2.getNumIndexDimensions();
        pointValues2.intersect(new PointValuesIntersectVisitor());
    }

    private static class PointValuesIntersectVisitor implements PointValues.IntersectVisitor {
        @Override
        public void visit(int docID) {}

        @Override
        public void visit(int docID, byte[] packedValue) {}

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
    }
}
