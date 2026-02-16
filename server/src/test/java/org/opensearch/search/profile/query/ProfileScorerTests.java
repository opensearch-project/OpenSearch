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
import org.apache.lucene.search.Scorable;
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

    public void testImplementsWrappedScorerAccessor() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);

        // ProfileScorer implements WrappedScorerAccessor, allowing plugins to detect and unwrap
        // profiling wrappers using instanceof without reflection
        assertTrue("ProfileScorer should implement WrappedScorerAccessor", profileScorer instanceof WrappedScorerAccessor);
        WrappedScorerAccessor accessor = (WrappedScorerAccessor) profileScorer;
        assertSame("WrappedScorerAccessor.getWrappedScorer() should return the original scorer", fakeScorer, accessor.getWrappedScorer());
    }

    public void testUnwrapFromScorableReference() throws IOException {
        // Simulate how a plugin collector receives a generic Scorable reference
        // (e.g., via LeafCollector.setScorer()) and uses WrappedScorerAccessor
        // to unwrap the profiling wrapper and access the original scorer
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());

        // Plugin only sees a Scorable reference, not ProfileScorer
        Scorable scorable = new ProfileScorer(fakeScorer, profile);

        // Plugin uses instanceof to detect the wrapper and unwrap
        assertTrue("Scorable should be detected as WrappedScorerAccessor", scorable instanceof WrappedScorerAccessor);
        Scorer unwrapped = ((WrappedScorerAccessor) scorable).getWrappedScorer();
        assertSame("Unwrapped scorer should be the original FakeScorer", fakeScorer, unwrapped);
    }

    public void testGetChildren_delegatesToWrappedScorer() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);

        // getChildren() should delegate to the wrapped scorer's getChildren()
        Collection<Scorer.ChildScorable> children = profileScorer.getChildren();
        assertEquals("FakeScorer has no children, so ProfileScorer should return empty collection", 0, children.size());
    }
}
