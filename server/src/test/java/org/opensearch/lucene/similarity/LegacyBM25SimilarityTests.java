/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.lucene.similarity;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.tests.search.similarities.BaseSimilarityTestCase;

import java.util.Random;

@Deprecated
public class LegacyBM25SimilarityTests extends BaseSimilarityTestCase {

    public void testIllegalK1() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            new LegacyBM25Similarity(Float.POSITIVE_INFINITY, 0.75f);
        });
        assertTrue(expected.getMessage().contains("illegal k1 value"));

        expected = expectThrows(IllegalArgumentException.class, () -> { new LegacyBM25Similarity(-1, 0.75f); });
        assertTrue(expected.getMessage().contains("illegal k1 value"));

        expected = expectThrows(IllegalArgumentException.class, () -> { new LegacyBM25Similarity(Float.NaN, 0.75f); });
        assertTrue(expected.getMessage().contains("illegal k1 value"));
    }

    public void testIllegalB() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> { new LegacyBM25Similarity(1.2f, 2f); });
        assertTrue(expected.getMessage().contains("illegal b value"));

        expected = expectThrows(IllegalArgumentException.class, () -> { new LegacyBM25Similarity(1.2f, -1f); });
        assertTrue(expected.getMessage().contains("illegal b value"));

        expected = expectThrows(IllegalArgumentException.class, () -> { new LegacyBM25Similarity(1.2f, Float.POSITIVE_INFINITY); });
        assertTrue(expected.getMessage().contains("illegal b value"));

        expected = expectThrows(IllegalArgumentException.class, () -> { new LegacyBM25Similarity(1.2f, Float.NaN); });
        assertTrue(expected.getMessage().contains("illegal b value"));
    }

    public void testDefaults() {
        LegacyBM25Similarity legacyBM25Similarity = new LegacyBM25Similarity();
        BM25Similarity bm25Similarity = new BM25Similarity();
        assertEquals(bm25Similarity.getB(), legacyBM25Similarity.getB(), 0f);
        assertEquals(bm25Similarity.getK1(), legacyBM25Similarity.getK1(), 0f);
    }

    public void testToString() {
        LegacyBM25Similarity legacyBM25Similarity = new LegacyBM25Similarity();
        BM25Similarity bm25Similarity = new BM25Similarity();
        assertEquals(bm25Similarity.toString(), legacyBM25Similarity.toString());
    }

    @Override
    protected Similarity getSimilarity(Random random) {
        return new LegacyBM25Similarity(randomK1(random), randomB(random));
    }

    private static float randomK1(Random random) {
        // term frequency normalization parameter k1
        switch (random.nextInt(4)) {
            case 0:
                // minimum value
                return 0;
            case 1:
                // tiny value
                return Float.MIN_VALUE;
            case 2:
                // maximum value
                // upper bounds on individual term's score is 43.262806 * (k1 + 1) * boost
                // we just limit the test to "reasonable" k1 values but don't enforce this anywhere.
                return Integer.MAX_VALUE;
            default:
                // random value
                return Integer.MAX_VALUE * random.nextFloat();
        }
    }

    private static float randomB(Random random) {
        // length normalization parameter b [0 .. 1]
        switch (random.nextInt(4)) {
            case 0:
                // minimum value
                return 0;
            case 1:
                // tiny value
                return Float.MIN_VALUE;
            case 2:
                // maximum value
                return 1;
            default:
                // random value
                return random.nextFloat();
        }
    }
}
