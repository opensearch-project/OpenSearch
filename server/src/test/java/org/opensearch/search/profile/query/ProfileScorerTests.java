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

package org.opensearch.search.profile.query;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.opensearch.search.profile.ProfileMetricUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collection;

public class ProfileScorerTests extends OpenSearchTestCase {

    private static class FakeScorer extends Scorer {

        public float maxScore, minCompetitiveScore;

        protected FakeScorer(Weight weight) {
            super();
        }

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return maxScore;
        }

        @Override
        public float score() throws IOException {
            return 1f;
        }

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMinCompetitiveScore(float minScore) {
            this.minCompetitiveScore = minScore;
        }
    }

    public void testPropagateMinCompetitiveScore() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);
        profileScorer.setMinCompetitiveScore(0.42f);
        assertEquals(0.42f, fakeScorer.minCompetitiveScore, 0f);
    }

    public void testPropagateMaxScore() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);
        profileScorer.setMinCompetitiveScore(0.42f);
        fakeScorer.maxScore = 42f;
        assertEquals(42f, profileScorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0f);
    }

    public void testGetWrappedScorer() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);

        // Verify getWrappedScorer returns the original scorer
        assertSame(fakeScorer, profileScorer.getWrappedScorer());
    }

    public void testGetChildren_exposesWrappedScorerAsChild() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);

        // getChildren() should expose the wrapped scorer as a child,
        // making the scorer tree traversable through profiling wrappers.
        // This enables plugins to discover custom scorers (e.g. HybridQueryScorer)
        // through ProfileScorer via standard Lucene getChildren() traversal.
        Collection<Scorer.ChildScorable> children = profileScorer.getChildren();
        assertEquals("ProfileScorer should have exactly 1 child (the wrapped scorer)", 1, children.size());

        Scorer.ChildScorable child = children.iterator().next();
        assertSame("Child should be the wrapped scorer", fakeScorer, child.child());
        assertEquals("Child relationship should be PROFILED", "PROFILED", child.relationship());
    }
}
