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

package org.opensearch.search.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.opensearch.Version;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.lucene.search.function.ScriptScoreQuery;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptScoreQueryTests extends OpenSearchTestCase {

    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private LeafReaderContext leafReaderContext;

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig(new StandardAnalyzer()));
        Document d = new Document();
        d.add(new TextField("field", "some text in a field", Field.Store.YES));
        d.add(new TextField("_uid", "1", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);
        leafReaderContext = reader.leaves().get(0);
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testExplain() throws IOException {
        Script script = new Script("script using explain");
        ScoreScript.LeafFactory factory = newFactory(script, true, explanation -> {
            assertNotNull(explanation);
            explanation.set("this explains the score");
            return 1.0;
        });

        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        assertThat(explanation.getDescription(), equalTo("this explains the score"));
        assertThat(explanation.getValue(), equalTo(1.0));
    }

    public void testExplainWithName() throws IOException {
        Script script = new Script("script using explain");
        ScoreScript.LeafFactory factory = newFactory(script, true, explanation -> {
            assertNotNull(explanation);
            explanation.set("this explains the score");
            return 1.0;
        });

        ScriptScoreQuery query = new ScriptScoreQuery(
            Queries.newMatchAllQuery(),
            "query1",
            script,
            factory,
            null,
            "index",
            0,
            Version.CURRENT
        );
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        assertThat(explanation.getDescription(), equalTo("this explains the score"));
        assertThat(explanation.getValue(), equalTo(1.0));

        assertThat(explanation.getDetails(), arrayWithSize(1));
        assertThat(explanation.getDetails()[0].getDescription(), equalTo("*:* (_name: query1)"));
    }

    public void testExplainDefault() throws IOException {
        Script script = new Script("script without setting explanation");
        ScoreScript.LeafFactory factory = newFactory(script, true, explanation -> 1.5);

        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        String description = explanation.getDescription();
        assertThat(description, containsString("script score function, computed with script:"));
        assertThat(description, containsString("script without setting explanation"));
        assertThat(explanation.getDetails(), arrayWithSize(1));
        assertThat(explanation.getDetails()[0].getDescription(), containsString("_score"));
        assertThat(explanation.getValue(), equalTo(1.5f));
    }

    public void testExplainDefaultNoScore() throws IOException {
        Script script = new Script("script without setting explanation and no score");
        ScoreScript.LeafFactory factory = newFactory(script, false, explanation -> 2.0);

        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        String description = explanation.getDescription();
        assertThat(description, containsString("script score function, computed with script:"));
        assertThat(description, containsString("script without setting explanation and no score"));
        assertThat(explanation.getDetails(), arrayWithSize(0));
        assertThat(explanation.getValue(), equalTo(2.0f));
    }

    public void testScriptScoreErrorOnNegativeScore() {
        Script script = new Script("script that returns a negative score");
        ScoreScript.LeafFactory factory = newFactory(script, false, explanation -> -1000.0);
        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> searcher.search(query, 1));
        assertTrue(e.getMessage().contains("Must be a non-negative score!"));
    }

    public void testTwoPhaseIteratorDelegation() throws IOException {
        Map<String, Object> params = new HashMap<>();
        String scriptSource = "doc['field'].value != null ? 2.0 : 0.0"; // Adjust based on actual field and logic
        Script script = new Script(ScriptType.INLINE, "painless", scriptSource, params);
        float minScore = 1.0f; // This should be below the score produced by the script for all docs
        ScoreScript.LeafFactory factory = newFactory(script, false, explanation -> 2.0);

        Query subQuery = new MatchAllDocsQuery();
        ScriptScoreQuery scriptScoreQuery = new ScriptScoreQuery(subQuery, script, factory, minScore, "index", 0, Version.CURRENT);

        Weight weight = searcher.createWeight(searcher.rewrite(scriptScoreQuery), ScoreMode.COMPLETE, 1f);

        boolean foundMatchingDoc = false;
        for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
            Scorer scorer = weight.scorer(leafContext);
            if (scorer != null) {
                TwoPhaseIterator twoPhaseIterator = scorer.twoPhaseIterator();
                assertNotNull("TwoPhaseIterator should not be null", twoPhaseIterator);
                DocIdSetIterator docIdSetIterator = twoPhaseIterator.approximation();
                int docId;
                while ((docId = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (twoPhaseIterator.matches()) {
                        foundMatchingDoc = true;
                        break;
                    }
                }
            }
        }
        assertTrue("Expected to find at least one matching document", foundMatchingDoc);
    }

    public void testNullScorerSupplier() throws IOException {
        Script script = new Script("script using explain");
        ScoreScript.LeafFactory factory = newFactory(script, true, explanation -> {
            assertNotNull(explanation);
            explanation.set("this explains the score");
            return 1.0;
        });

        ScriptScoreQuery query = new ScriptScoreQuery(new NullScorerSupplierQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(null);
        assertNull(scorerSupplier);
    }

    private static class NullScorerSupplierQuery extends Query {

        @Override
        public String toString(String field) {
            return getClass().getSimpleName();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new Weight(this) {

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return null;
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }
    }

    public void testBulkScoringLeafFactoryIsUsed() throws IOException {
        Script script = new Script("bulk scoring script");
        List<Integer> collectedDocs = new ArrayList<>();

        ScoreScript.BulkScoringLeafFactory factory = newBulkFactory(
            script,
            false,
            explanation -> 2.0,
            (context, subQueryBulkScorer, subQueryScoreMode, boost) -> {
                return new BulkScorer() {
                    @Override
                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                        Scorable scorable = new Scorable() {
                            float score = 5.0f;

                            @Override
                            public float score() {
                                return score;
                            }
                        };
                        collector.setScorer(scorable);
                        collector.collect(0);
                        collectedDocs.add(0);
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 1;
                    }
                };
            }
        );

        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        BulkScorer bulkScorer = scorerSupplier.bulkScorer();

        List<Integer> searchCollectedDocs = new ArrayList<>();
        LeafCollector collector = new LeafCollector() {
            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) {
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                searchCollectedDocs.add(doc);
            }
        };
        bulkScorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);

        assertThat(collectedDocs.size(), equalTo(1));
        assertThat(searchCollectedDocs.size(), equalTo(1));
        assertThat(searchCollectedDocs.get(0), equalTo(0));
    }

    public void testBulkScoringLeafFactoryReturnsNullFallsBackToDefault() throws IOException {
        Script script = new Script("bulk scoring returns null");

        ScoreScript.BulkScoringLeafFactory factory = newBulkFactory(
            script,
            false,
            explanation -> 3.0,
            (context, subQueryBulkScorer, subQueryScoreMode, boost) -> null
        );

        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        BulkScorer bulkScorer = scorerSupplier.bulkScorer();

        List<Float> scores = new ArrayList<>();
        LeafCollector collector = new LeafCollector() {
            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) {
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                scores.add(scorer.score());
            }
        };
        bulkScorer.score(collector, null, 0, DocIdSetIterator.NO_MORE_DOCS);

        assertThat(scores.size(), equalTo(1));
        assertThat(scores.get(0), equalTo(3.0f));
    }

    public void testBulkScoringLeafFactorySkippedWhenMinScoreSet() throws IOException {
        Script script = new Script("bulk scoring with minScore");
        boolean[] bulkScorerCalled = new boolean[] { false };

        ScoreScript.BulkScoringLeafFactory factory = newBulkFactory(
            script,
            false,
            explanation -> 5.0,
            (context, subQueryBulkScorer, subQueryScoreMode, boost) -> {
                bulkScorerCalled[0] = true;
                return new BulkScorer() {
                    @Override
                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }
                };
            }
        );

        float minScore = 1.0f;
        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, minScore, "index", 0, Version.CURRENT);

        searcher.search(query, 10);
        assertFalse("BulkScoringLeafFactory.bulkScorer() should not be called when minScore is set", bulkScorerCalled[0]);
    }

    public void testBulkScoringLeafFactoryWithBoost() throws IOException {
        Script script = new Script("bulk scoring with boost");
        float queryBoost = 2.5f;
        float[] receivedBoost = new float[] { 0f };

        ScoreScript.BulkScoringLeafFactory factory = newBulkFactory(
            script,
            false,
            explanation -> 1.0,
            (context, subQueryBulkScorer, subQueryScoreMode, boost) -> {
                receivedBoost[0] = boost;
                return null;
            }
        );

        ScriptScoreQuery query = new ScriptScoreQuery(Queries.newMatchAllQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, queryBoost);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        scorerSupplier.bulkScorer();

        assertThat(receivedBoost[0], equalTo(queryBoost));
    }

    public void testBulkScoringReturnsNullWhenSubQueryBulkScorerIsNull() throws IOException {
        Script script = new Script("bulk scoring with null sub-query bulk scorer");
        boolean[] bulkScorerCalled = new boolean[] { false };

        ScoreScript.BulkScoringLeafFactory factory = newBulkFactory(
            script,
            false,
            explanation -> 1.0,
            (context, subQueryBulkScorer, subQueryScoreMode, boost) -> {
                bulkScorerCalled[0] = true;
                return new BulkScorer() {
                    @Override
                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }
                };
            }
        );

        ScriptScoreQuery query = new ScriptScoreQuery(new NullBulkScorerQuery(), script, factory, null, "index", 0, Version.CURRENT);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        ScorerSupplier scorerSupplier = weight.scorerSupplier(leafReaderContext);
        BulkScorer bulkScorer = scorerSupplier.bulkScorer();

        assertNull("BulkScorer should be null when sub-query's bulkScorer() returns null", bulkScorer);
        assertFalse("BulkScoringLeafFactory.bulkScorer() should not be called when sub-query bulk scorer is null", bulkScorerCalled[0]);
    }

    private static class NullBulkScorerQuery extends Query {

        @Override
        public String toString(String field) {
            return getClass().getSimpleName();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new Weight(this) {

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            return null;
                        }

                        @Override
                        public BulkScorer bulkScorer() throws IOException {
                            return null;
                        }

                        @Override
                        public long cost() {
                            return 0;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        }
    }

    @FunctionalInterface
    interface BulkScorerProvider {
        BulkScorer provide(LeafReaderContext context, BulkScorer subQueryBulkScorer, ScoreMode subQueryScoreMode, float boost)
            throws IOException;
    }

    private ScoreScript.BulkScoringLeafFactory newBulkFactory(
        Script script,
        boolean needsScore,
        Function<ScoreScript.ExplanationHolder, Double> function,
        BulkScorerProvider bulkScorerProvider
    ) {
        SearchLookup lookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        IndexSearcher indexSearcher = mock(IndexSearcher.class);
        when(lookup.getLeafSearchLookup(any())).thenReturn(leafLookup);
        return new ScoreScript.BulkScoringLeafFactory() {
            @Override
            public BulkScorer bulkScorer(LeafReaderContext context, BulkScorer subQueryBulkScorer, ScoreMode subQueryScoreMode, float boost)
                throws IOException {
                return bulkScorerProvider.provide(context, subQueryBulkScorer, subQueryScoreMode, boost);
            }

            @Override
            public boolean needs_score() {
                return needsScore;
            }

            @Override
            public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
                return new ScoreScript(script.getParams(), lookup, indexSearcher, leafReaderContext) {
                    @Override
                    public double execute(ExplanationHolder explanation) {
                        return function.apply(explanation);
                    }
                };
            }
        };
    }

    private ScoreScript.LeafFactory newFactory(
        Script script,
        boolean needsScore,
        Function<ScoreScript.ExplanationHolder, Double> function
    ) {
        SearchLookup lookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        IndexSearcher indexSearcher = mock(IndexSearcher.class);
        when(lookup.getLeafSearchLookup(any())).thenReturn(leafLookup);
        return new ScoreScript.LeafFactory() {
            @Override
            public boolean needs_score() {
                return needsScore;
            }

            @Override
            public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
                return new ScoreScript(script.getParams(), lookup, indexSearcher, leafReaderContext) {
                    @Override
                    public double execute(ExplanationHolder explanation) {
                        return function.apply(explanation);
                    }
                };
            }
        };
    }
}
