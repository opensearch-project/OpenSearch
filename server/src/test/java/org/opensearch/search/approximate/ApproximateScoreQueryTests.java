/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.apache.lucene.document.LongPoint.pack;

public class ApproximateScoreQueryTests extends OpenSearchTestCase {

    public void testApproximationScoreSupplier() throws IOException {
        long l = Long.MIN_VALUE;
        long u = Long.MAX_VALUE;
        Query originalQuery = new PointRangeQuery(
            "test-index",
            pack(new long[] { l }).bytes,
            pack(new long[] { u }).bytes,
            new long[] { l }.length
        ) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };

        ApproximateQuery approximateQuery = new ApproximatePointRangeQuery(
            "test-index",
            pack(new long[] { l }).bytes,
            pack(new long[] { u }).bytes,
            new long[] { l }.length
        ) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };

        ApproximateScoreQuery query = new ApproximateScoreQuery(originalQuery, approximateQuery);
        query.resolvedQuery = approximateQuery;

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                Document document = new Document();
                document.add(new LongPoint("testPoint", Long.MIN_VALUE));
                iw.addDocument(document);
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        IndexSearcher searcher = new IndexSearcher(reader);
                        searcher.search(query, 10);
                        Weight weight = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F);
                        Scorer scorer = weight.scorer(reader.leaves().get(0));
                        assertEquals(
                            scorer,
                            originalQuery.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0F).scorer(searcher.getLeafContexts().get(0))
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }
}
