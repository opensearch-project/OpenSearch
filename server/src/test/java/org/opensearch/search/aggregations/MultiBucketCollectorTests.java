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

package org.opensearch.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiBucketCollectorTests extends OpenSearchTestCase {
    private static class ScoreAndDoc extends Scorable {
        float score;

        @Override
        public float score() {
            return score;
        }
    }

    private static class TerminateAfterBucketCollector extends BucketCollector {

        private int count = 0;
        private final int terminateAfter;
        private final BucketCollector in;

        TerminateAfterBucketCollector(BucketCollector in, int terminateAfter) {
            this.in = in;
            this.terminateAfter = terminateAfter;
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
            if (count >= terminateAfter) {
                throw new CollectionTerminatedException();
            }
            final LeafBucketCollector leafCollector = in.getLeafCollector(context);
            return new LeafBucketCollectorBase(leafCollector, null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (count >= terminateAfter) {
                        throw new CollectionTerminatedException();
                    }
                    super.collect(doc, bucket);
                    count++;
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}
    }

    private static class TotalHitCountBucketCollector extends BucketCollector {

        private int count = 0;

        TotalHitCountBucketCollector() {}

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext context) {
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    count++;
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}

        int getTotalHits() {
            return count;
        }
    }

    private static class SetScorerBucketCollector extends BucketCollector {
        private final BucketCollector in;
        private final AtomicBoolean setScorerCalled;

        SetScorerBucketCollector(BucketCollector in, AtomicBoolean setScorerCalled) {
            this.in = in;
            this.setScorerCalled = setScorerCalled;
        }

        @Override
        public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
            final LeafBucketCollector leafCollector = in.getLeafCollector(context);
            return new LeafBucketCollectorBase(leafCollector, null) {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    setScorerCalled.set(true);
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}
    }

    public void testCollectionTerminatedExceptionHandling() throws IOException {
        final int iters = atLeast(3);
        for (int iter = 0; iter < iters; ++iter) {
            Directory dir = newDirectory();
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            final int numDocs = randomIntBetween(100, 1000);
            final Document doc = new Document();
            for (int i = 0; i < numDocs; ++i) {
                w.addDocument(doc);
            }
            final IndexReader reader = w.getReader();
            w.close();
            final IndexSearcher searcher = newSearcher(reader);
            Map<TotalHitCountBucketCollector, Integer> expectedCounts = new HashMap<>();
            List<BucketCollector> collectors = new ArrayList<>();
            final int numCollectors = randomIntBetween(1, 5);
            for (int i = 0; i < numCollectors; ++i) {
                final int terminateAfter = random().nextInt(numDocs + 10);
                final int expectedCount = terminateAfter > numDocs ? numDocs : terminateAfter;
                TotalHitCountBucketCollector collector = new TotalHitCountBucketCollector();
                expectedCounts.put(collector, expectedCount);
                collectors.add(new TerminateAfterBucketCollector(collector, terminateAfter));
            }
            searcher.search(new MatchAllDocsQuery(), MultiBucketCollector.wrap(collectors));
            for (Map.Entry<TotalHitCountBucketCollector, Integer> expectedCount : expectedCounts.entrySet()) {
                assertEquals(expectedCount.getValue().intValue(), expectedCount.getKey().getTotalHits());
            }
            reader.close();
            dir.close();
        }
    }

    public void testSetScorerAfterCollectionTerminated() throws IOException {
        BucketCollector collector1 = new TotalHitCountBucketCollector();
        BucketCollector collector2 = new TotalHitCountBucketCollector();

        AtomicBoolean setScorerCalled1 = new AtomicBoolean();
        collector1 = new SetScorerBucketCollector(collector1, setScorerCalled1);

        AtomicBoolean setScorerCalled2 = new AtomicBoolean();
        collector2 = new SetScorerBucketCollector(collector2, setScorerCalled2);

        collector1 = new TerminateAfterBucketCollector(collector1, 1);
        collector2 = new TerminateAfterBucketCollector(collector2, 2);

        Scorable scorer = new ScoreAndDoc();

        List<BucketCollector> collectors = Arrays.asList(collector1, collector2);
        Collections.shuffle(collectors, random());
        BucketCollector collector = MultiBucketCollector.wrap(collectors);

        LeafBucketCollector leafCollector = collector.getLeafCollector(null);
        leafCollector.setScorer(scorer);
        assertTrue(setScorerCalled1.get());
        assertTrue(setScorerCalled2.get());

        leafCollector.collect(0);
        leafCollector.collect(1);

        setScorerCalled1.set(false);
        setScorerCalled2.set(false);
        leafCollector.setScorer(scorer);
        assertFalse(setScorerCalled1.get());
        assertTrue(setScorerCalled2.get());

        expectThrows(CollectionTerminatedException.class, () -> { leafCollector.collect(1); });

        setScorerCalled1.set(false);
        setScorerCalled2.set(false);
        leafCollector.setScorer(scorer);
        assertFalse(setScorerCalled1.get());
        assertFalse(setScorerCalled2.get());
    }
}
