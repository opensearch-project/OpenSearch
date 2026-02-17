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
import org.opensearch.search.profile.ProfilingWrapper;
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

    public void testGetDelegate() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);

        // Verify getDelegate returns the original scorer
        assertSame(fakeScorer, profileScorer.getDelegate());
    }

    @SuppressWarnings("unchecked")
    public void testImplementsProfilingWrapper() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);

        // ProfileScorer implements ProfilingWrapper<Scorer>, allowing plugins to detect and unwrap
        // profiling wrappers using instanceof without reflection
        assertTrue("ProfileScorer should implement ProfilingWrapper", profileScorer instanceof ProfilingWrapper);
        ProfilingWrapper<Scorer> wrapper = (ProfilingWrapper<Scorer>) profileScorer;
        assertSame("ProfilingWrapper.getDelegate() should return the original scorer", fakeScorer, wrapper.getDelegate());
    }

    @SuppressWarnings("unchecked")
    public void testUnwrapNestedProfilingScorerFromScorableReference() throws IOException {
        // Simulate nested profiling: a scorer wrapped in two ProfileScorer layers.
        // This tests that a plugin can iteratively unwrap through multiple profiling
        // layers via a generic Scorable reference (as received in LeafCollector.setScorer())
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown innerProfile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        QueryProfileBreakdown outerProfile = new QueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());

        // Create nested profiling: FakeScorer -> innerProfileScorer -> outerProfileScorer
        ProfileScorer innerProfileScorer = new ProfileScorer(fakeScorer, innerProfile);
        ProfileScorer outerProfileScorer = new ProfileScorer(innerProfileScorer, outerProfile);

        // Plugin only sees a Scorable reference
        Scorable scorable = outerProfileScorer;

        // Unwrap first layer
        assertTrue("Outer Scorable should be detected as ProfilingWrapper", scorable instanceof ProfilingWrapper);
        Scorer firstUnwrap = ((ProfilingWrapper<Scorer>) scorable).getDelegate();
        assertNotSame("First unwrap should not be the original scorer", fakeScorer, firstUnwrap);
        assertTrue("First unwrap should still be a ProfilingWrapper (inner ProfileScorer)", firstUnwrap instanceof ProfilingWrapper);

        // Unwrap second layer to reach the original scorer
        Scorer secondUnwrap = ((ProfilingWrapper<Scorer>) firstUnwrap).getDelegate();
        assertSame("Second unwrap should be the original FakeScorer", fakeScorer, secondUnwrap);
        assertFalse("Original scorer should not implement ProfilingWrapper", secondUnwrap instanceof ProfilingWrapper);
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
