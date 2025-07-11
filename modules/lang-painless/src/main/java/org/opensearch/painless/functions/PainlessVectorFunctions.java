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

public final class PainlessVectorFunctions {

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

                double currentSim = 0.0;
                if (q_vec.size() == doc_token_vec.size()) {
                    for (int k = 0; k < q_vec.size(); k++) {
                        currentSim += q_vec.get(k) * doc_token_vec.get(k);
                    }
                } else {
                    // Handle dimension mismatch, perhaps log a warning or return a specific value
                    // For now, as per original script, if dimensions mismatch, currentSim remains 0.0
                    currentSim = 0.0;
                }

                if (currentSim > maxDocTokenSim) {
                    maxDocTokenSim = currentSim;
                }
            }
            totalMaxSim += maxDocTokenSim;
        }
        return totalMaxSim;
    }
}
