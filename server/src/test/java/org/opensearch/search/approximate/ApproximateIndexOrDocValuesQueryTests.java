/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.apache.lucene.document.LongPoint.pack;

public class ApproximateIndexOrDocValuesQueryTests extends OpenSearchTestCase {
    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig());
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testApproximateIndexOrDocValuesQueryWeight() throws IOException {

        long l = Long.MIN_VALUE;
        long u = Long.MAX_VALUE;
        Query indexQuery = LongPoint.newRangeQuery("test-index", l, u);

        ApproximateQuery approximateIndexQuery = new ApproximatePointRangeQuery(
            "test-index",
            pack(new long[] { l }).bytes,
            pack(new long[] { u }).bytes,
            new long[] { l }.length
        ) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };

        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery("test-index", l, u);

        ApproximateIndexOrDocValuesQuery approximateIndexOrDocValuesQuery = new ApproximateIndexOrDocValuesQuery(
            indexQuery,
            approximateIndexQuery,
            dvQuery
        );

        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        approximateIndexOrDocValuesQuery.resolvedQuery = indexQuery;

        Weight weight = approximateIndexOrDocValuesQuery.rewrite(searcher).createWeight(searcher, ScoreMode.COMPLETE, 1f);

        assertTrue(weight instanceof ConstantScoreWeight);

        ApproximateQuery approximateIndexQueryCanApproximate = new ApproximatePointRangeQuery(
            "test-index",
            pack(new long[] { l }).bytes,
            pack(new long[] { u }).bytes,
            new long[] { l }.length
        ) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }

            public boolean canApproximate(SearchContext context) {
                return true;
            }
        };

        ApproximateIndexOrDocValuesQuery approximateIndexOrDocValuesQueryCanApproximate = new ApproximateIndexOrDocValuesQuery(
            indexQuery,
            approximateIndexQueryCanApproximate,
            dvQuery
        );

        approximateIndexOrDocValuesQueryCanApproximate.resolvedQuery = approximateIndexQueryCanApproximate;

        Weight approximateIndexOrDocValuesQueryCanApproximateWeight = approximateIndexOrDocValuesQueryCanApproximate.rewrite(searcher)
            .createWeight(searcher, ScoreMode.COMPLETE, 1f);

        // we get ConstantScoreWeight since we're expecting to call ApproximatePointRangeQuery
        assertTrue(approximateIndexOrDocValuesQueryCanApproximateWeight instanceof ConstantScoreWeight);

    }
}
