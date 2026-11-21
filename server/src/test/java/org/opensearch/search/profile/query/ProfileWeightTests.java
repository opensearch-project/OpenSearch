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
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.search.profile.ProfileMetricUtil;
import org.opensearch.search.profile.Timer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ProfileWeightTests extends OpenSearchTestCase {

    private static final class RecordingWeight extends Weight {
        private final RecordingScorerSupplier scorerSupplier = new RecordingScorerSupplier();

        private RecordingWeight(Query query) {
            super(query);
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) {
            return scorerSupplier;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCacheable(LeafReaderContext context) {
            return false;
        }
    }

    private static final class RecordingScorerSupplier extends ScorerSupplier {
        private boolean topLevelScoringClause;

        @Override
        public Scorer get(long loadCost) throws IOException {
            throw new IOException("expected test exception");
        }

        @Override
        public long cost() {
            return 1L;
        }

        @Override
        public void setTopLevelScoringClause() throws IOException {
            topLevelScoringClause = true;
            throw new IOException("expected test exception");
        }
    }

    public void testSetTopLevelScoringClauseDelegatesAndBuildScorerTimerStopsOnException() throws IOException {
        Query query = new MatchAllDocsQuery();
        RecordingWeight delegate = new RecordingWeight(query);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileWeight profileWeight = new ProfileWeight(query, delegate, profile);

        ScorerSupplier scorerSupplier = profileWeight.scorerSupplier(null);
        expectThrows(IOException.class, scorerSupplier::setTopLevelScoringClause);
        assertTrue(delegate.scorerSupplier.topLevelScoringClause);
        expectThrows(IOException.class, () -> scorerSupplier.get(1L));
        expectThrows(IOException.class, scorerSupplier::bulkScorer);

        Timer buildScorerTimer = profile.getTimer(QueryTimingType.BUILD_SCORER);
        assertEquals(4L, buildScorerTimer.getCount());
        buildScorerTimer.getApproximateTiming();
    }

    public void testFlaggedBooleanScorerAndProfiledTopDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 128; i++) {
                    switch (i % 4) {
                        case 0 -> addDocument(writer, "one two three");
                        case 1 -> addDocument(writer, "one two");
                        case 2 -> addDocument(writer, "one");
                        default -> addDocument(writer, "none");
                    }
                }
                writer.forceMerge(1);
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Query query = new BooleanQuery.Builder().add(new TermQuery(new Term("field", "one")), BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term("field", "two")), BooleanClause.Occur.SHOULD)
                        .add(new TermQuery(new Term("field", "three")), BooleanClause.Occur.SHOULD)
                        .build();
                    Query rewrittenQuery = searcher.rewrite(query);
                    Weight delegateWeight = searcher.createWeight(rewrittenQuery, ScoreMode.TOP_SCORES, 1f);
                    LeafReaderContext leaf = reader.leaves().get(0);

                    Scorer unflaggedScorer = delegateWeight.scorerSupplier(leaf).get(Long.MAX_VALUE);
                    ScorerSupplier flaggedSupplier = delegateWeight.scorerSupplier(leaf);
                    flaggedSupplier.setTopLevelScoringClause();
                    Scorer flaggedScorer = flaggedSupplier.get(Long.MAX_VALUE);
                    assertNotEquals(unflaggedScorer.getClass(), flaggedScorer.getClass());

                    QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
                    ProfileWeight profileWeight = new ProfileWeight(query, delegateWeight, profile);
                    ScorerSupplier profileSupplier = profileWeight.scorerSupplier(leaf);
                    profileSupplier.setTopLevelScoringClause();
                    Scorer profileScorer = profileSupplier.get(Long.MAX_VALUE);
                    assertEquals(flaggedScorer.getClass(), ((ProfileScorer) profileScorer).getDelegate().getClass());

                    TopScoreDocCollectorManager collectorManager = new TopScoreDocCollectorManager(3, 0);
                    TopDocs expected = searcher.search(query, collectorManager);
                    TopScoreDocCollectorManager profiledCollectorManager = new TopScoreDocCollectorManager(3, 0);
                    var collector = profiledCollectorManager.newCollector();
                    ScorerSupplier profiledSearchSupplier = profileWeight.scorerSupplier(leaf);
                    profiledSearchSupplier.setTopLevelScoringClause();
                    BulkScorer bulkScorer = profiledSearchSupplier.bulkScorer();
                    assertEquals(delegateWeight.bulkScorer(leaf).getClass(), ((ProfileBulkScorer) bulkScorer).getDelegate().getClass());
                    assertEquals("MaxScoreBulkScorer", ((ProfileBulkScorer) bulkScorer).getDelegate().getClass().getSimpleName());
                    bulkScorer.score(collector.getLeafCollector(leaf), null, 0, reader.maxDoc());
                    TopDocs actual = collector.topDocs();
                    assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
                    assertEquals(3, actual.scoreDocs.length);
                    for (int i = 0; i < expected.scoreDocs.length; i++) {
                        assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
                        assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0f);
                    }

                    Weight completeDelegateWeight = searcher.createWeight(rewrittenQuery, ScoreMode.COMPLETE, 1f);
                    QueryProfileBreakdown completeProfile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
                    ProfileWeight completeProfileWeight = new ProfileWeight(query, completeDelegateWeight, completeProfile);
                    TopScoreDocCollectorManager exactCountCollectorManager = new TopScoreDocCollectorManager(3, Integer.MAX_VALUE);
                    var exactCountCollector = exactCountCollectorManager.newCollector();
                    ScorerSupplier exactCountSupplier = completeProfileWeight.scorerSupplier(leaf);
                    exactCountSupplier.setTopLevelScoringClause();
                    exactCountSupplier.bulkScorer().score(exactCountCollector.getLeafCollector(leaf), null, 0, reader.maxDoc());
                    assertEquals(searcher.count(query), exactCountCollector.topDocs().totalHits.value());

                    assertEquals(1L, profile.getTimer(QueryTimingType.BULK_SCORE).getCount());
                    assertTrue(profile.getTimer(QueryTimingType.BULK_SCORE).getApproximateTiming() > 0L);
                    assertEquals(0L, profile.getTimer(QueryTimingType.SCORE).getCount());
                    assertEquals(0L, profile.getTimer(QueryTimingType.NEXT_DOC).getCount());
                }
            }
        }
    }

    private static void addDocument(RandomIndexWriter writer, String value) throws IOException {
        Document document = new Document();
        for (String term : value.split(" ")) {
            document.add(new StringField("field", term, Field.Store.NO));
        }
        writer.addDocument(document);
    }
}
