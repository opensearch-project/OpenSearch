/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless.functions;

import java.util.List;
import java.util.Map;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public final class PainlessVectorFunctions {

    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    private PainlessVectorFunctions() {}

    /**
     * Calculates the late interaction score between query vectors and document vectors.
     * For each query vector, finds the maximum dot product with any document vector and sums these maxima.
     * This implements a ColBERT-style late interaction pattern for token-level matching.
     *
     * @param queryVectors List of query vectors, each a list of doubles
     * @param docFieldName Name of the field in the document containing vectors
     * @param doc Document source as a map
     * @return Sum of maximum similarity scores
     */
    @SuppressWarnings("unchecked")
    public static double lateInteractionScore(List<List<Double>> queryVectors, String docFieldName, Map<String, Object> doc) {
        if (queryVectors == null || queryVectors.isEmpty()) {
            return 0.0;
        }

        double totalMaxSim = 0.0;
        List<List<Double>> docVectors = (List<List<Double>>) doc.get(docFieldName);

        if (docVectors == null || docVectors.isEmpty()) {
            return 0.0;
        }

        for (List<Double> q_vec : queryVectors) {
            if (q_vec == null || q_vec.isEmpty()) {
                continue;
            }

            double maxDocTokenSim = 0.0;

            for (List<Double> doc_token_vec : docVectors) {
                if (doc_token_vec == null || doc_token_vec.isEmpty()) {
                    continue;
                }

                double currentSim = dotProduct(q_vec, doc_token_vec);

                if (currentSim > maxDocTokenSim) {
                    maxDocTokenSim = currentSim;
                }
            }
            totalMaxSim += maxDocTokenSim;
        }
        return totalMaxSim;
    }

    private static double dotProduct(List<Double> vec1, List<Double> vec2) {
        if (vec1 == null || vec2 == null || vec1.size() != vec2.size()) {
            return 0.0;
        }
        double[] a = vec1.stream().mapToDouble(d -> d).toArray();
        double[] b = vec2.stream().mapToDouble(d -> d).toArray();
        double res = 0;
        int i = 0;
        for (; i < SPECIES.loopBound(a.length); i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            res += va.mul(vb).reduceLanes(VectorOperators.ADD);
        }
        for (; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }
}
