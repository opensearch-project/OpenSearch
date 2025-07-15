/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless.functions;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PainlessVectorFunctionsTests extends OpenSearchTestCase {

    public void testLateInteractionScore() {
        // Create query vectors
        List<List<Double>> queryVectors = new ArrayList<>();
        List<Double> qv1 = new ArrayList<>();
        qv1.add(0.1);
        qv1.add(0.2);
        qv1.add(0.3);
        qv1.add(0.4);
        queryVectors.add(qv1);

        List<Double> qv2 = new ArrayList<>();
        qv2.add(0.5);
        qv2.add(0.6);
        qv2.add(0.7);
        qv2.add(0.8);
        queryVectors.add(qv2);

        // Create document vectors
        List<List<Double>> docVectors = new ArrayList<>();
        List<Double> dv1 = new ArrayList<>();
        dv1.add(0.1);
        dv1.add(0.2);
        dv1.add(0.3);
        dv1.add(0.4);
        docVectors.add(dv1);

        List<Double> dv2 = new ArrayList<>();
        dv2.add(0.5);
        dv2.add(0.6);
        dv2.add(0.7);
        dv2.add(0.8);
        docVectors.add(dv2);

        // Create document source
        Map<String, Object> doc = new HashMap<>();
        doc.put("my_vector", docVectors);

        // Calculate expected result
        // Let's calculate all dot products:
        double qv1dv1 = 0.1 * 0.1 + 0.2 * 0.2 + 0.3 * 0.3 + 0.4 * 0.4; // = 0.3
        double qv1dv2 = 0.1 * 0.5 + 0.2 * 0.6 + 0.3 * 0.7 + 0.4 * 0.8; // = 0.7
        double qv2dv1 = 0.5 * 0.1 + 0.6 * 0.2 + 0.7 * 0.3 + 0.8 * 0.4; // = 0.7
        double qv2dv2 = 0.5 * 0.5 + 0.6 * 0.6 + 0.7 * 0.7 + 0.8 * 0.8; // = 1.74

        // For qv1, max similarity is with dv2: 0.7
        // For qv2, max similarity is with dv2: 1.74
        // Total: 0.7 + 1.74 = 2.44
        double expected = 2.44;

        // Calculate actual result
        double actual = PainlessVectorFunctions.lateInteractionScore(queryVectors, "my_vector", doc);

        // Assert
        assertEquals(expected, actual, 0.001);
    }

    public void testLateInteractionScoreWithEmptyVectors() {
        // Test with empty query vectors
        List<List<Double>> emptyQueryVectors = new ArrayList<>();
        Map<String, Object> doc = new HashMap<>();
        doc.put("my_vector", new ArrayList<List<Double>>());

        assertEquals(0.0, PainlessVectorFunctions.lateInteractionScore(emptyQueryVectors, "my_vector", doc), 0.0);

        // Test with null query vectors
        assertEquals(0.0, PainlessVectorFunctions.lateInteractionScore(null, "my_vector", doc), 0.0);

        // Test with missing field
        List<List<Double>> queryVectors = new ArrayList<>();
        List<Double> qv = new ArrayList<>();
        qv.add(0.1);
        queryVectors.add(qv);

        assertEquals(0.0, PainlessVectorFunctions.lateInteractionScore(queryVectors, "non_existent_field", doc), 0.0);
    }

    public void testLateInteractionScoreWithDimensionMismatch() {
        // Create query vectors
        List<List<Double>> queryVectors = new ArrayList<>();
        List<Double> qv = new ArrayList<>();
        qv.add(0.1);
        qv.add(0.2);
        queryVectors.add(qv);

        // Create document vectors with different dimensions
        List<List<Double>> docVectors = new ArrayList<>();
        List<Double> dv = new ArrayList<>();
        dv.add(0.1);
        dv.add(0.2);
        dv.add(0.3); // Extra dimension
        docVectors.add(dv);

        // Create document source
        Map<String, Object> doc = new HashMap<>();
        doc.put("my_vector", docVectors);

        // When dimensions mismatch, the similarity should be 0.0
        assertEquals(0.0, PainlessVectorFunctions.lateInteractionScore(queryVectors, "my_vector", doc), 0.0);
    }

    public void testLateInteractionScoreWithMultipleDocVectors() {
        // Create query vectors
        List<List<Double>> queryVectors = new ArrayList<>();
        List<Double> qv = new ArrayList<>();
        qv.add(0.1);
        qv.add(0.2);
        queryVectors.add(qv);

        // Create multiple document vectors
        List<List<Double>> docVectors = new ArrayList<>();

        List<Double> dv1 = new ArrayList<>();
        dv1.add(0.1);
        dv1.add(0.2);
        docVectors.add(dv1);

        List<Double> dv2 = new ArrayList<>();
        dv2.add(0.3);
        dv2.add(0.4);
        docVectors.add(dv2);

        List<Double> dv3 = new ArrayList<>();
        dv3.add(0.5);
        dv3.add(0.6);
        docVectors.add(dv3);

        // Create document source
        Map<String, Object> doc = new HashMap<>();
        doc.put("my_vector", docVectors);

        // Calculate expected result
        // For qv, max similarity with doc vectors is with dv3: 0.1*0.5 + 0.2*0.6 = 0.17
        double expected = 0.17;

        // Calculate actual result
        double actual = PainlessVectorFunctions.lateInteractionScore(queryVectors, "my_vector", doc);

        // Assert
        assertEquals(expected, actual, 0.001);
    }
}
