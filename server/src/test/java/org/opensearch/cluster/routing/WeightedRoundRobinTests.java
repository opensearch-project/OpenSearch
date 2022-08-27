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

    public void testWRROrder() {

        List<WeightedRoundRobin.Entity<String>> entity = new ArrayList<WeightedRoundRobin.Entity<String>>();
        entity.add(new WeightedRoundRobin.Entity<>(4, "A"));
        entity.add(new WeightedRoundRobin.Entity<>(3, "B"));
        entity.add(new WeightedRoundRobin.Entity<>(2, "C"));
        WeightedRoundRobin<String> wrr = new WeightedRoundRobin<String>(entity);
        List<WeightedRoundRobin.Entity<String>> orderedEntities = wrr.orderEntities();

        List<String> expectedOrdering = Arrays.asList("A", "A", "B", "A", "B", "C", "A", "B", "C");
        List<String> actualOrdering = new ArrayList<>();
        for (WeightedRoundRobin.Entity<String> en : orderedEntities) {
            actualOrdering.add(en.getTarget());
        }

        assertEquals(expectedOrdering, actualOrdering);
    }

}
