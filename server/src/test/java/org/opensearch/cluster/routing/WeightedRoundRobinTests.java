/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WeightedRoundRobinTests extends OpenSearchTestCase {

    public void testWeightedRoundRobinOrder() {
        // weights set as A:4, B:3, C:2
        List<WeightedRoundRobin.Entity<String>> entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(4, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(3, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(2, "C"));
        WeightedRoundRobin<String> weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        List<WeightedRoundRobin.Entity<String>> orderedEntities = weightedRoundRobin.orderEntities();
        List<String> expectedOrdering = Arrays.asList("A", "A", "B", "A", "B", "C", "A", "B", "C");
        List<String> actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set as A:1, B:1, C:0
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(1, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(1, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(0, "C"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList("A", "B");
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set as A:0, B:0, C:0
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(0, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(0, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(0, "C"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList();
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set as A:-1, B:0, C:1
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(-1, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(0, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(1, "C"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList("C");
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set as A:-1, B:3, C:0, D:10
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(-1, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(3, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(0, "C"));
        entity.add(new WeightedRoundRobin.Entity<>(10, "D"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList("B", "D", "B", "D", "B", "D", "D", "D", "D", "D", "D", "D", "D");
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set as A:-1, B:3, C:0, D:10000
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(-1, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(3, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(0, "C"));
        entity.add(new WeightedRoundRobin.Entity<>(10000, "D"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        assertEquals(10003, orderedEntities.size());
        // Count of D's
        int countD = 0;
        // Count of B's
        int countB = 0;
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            if (en.getTarget().equals("D")) {
                countD++;
            } else if (en.getTarget().equals("B")) {
                countB++;
            }
        }
        assertEquals(3, countB);
        assertEquals(10000, countD);

        // weights set C:0
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(0, "C"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList();
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set C:1
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(1, "C"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList("C");
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);

        // weights set C:2
        entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(2, "C"));
        weightedRoundRobin = new WeightedRoundRobin<String>(entity);
        orderedEntities = weightedRoundRobin.orderEntities();
        expectedOrdering = Arrays.asList("C", "C");
        actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }
        assertEquals(expectedOrdering, actualOrdering);
    }

}
