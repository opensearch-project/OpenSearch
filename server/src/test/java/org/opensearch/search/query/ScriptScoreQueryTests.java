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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
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
import java.util.HashMap;
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
