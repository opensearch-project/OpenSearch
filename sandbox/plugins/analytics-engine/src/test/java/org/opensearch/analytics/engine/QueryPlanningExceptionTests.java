/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Property-based tests for {@link QueryPlanningException}.
 *
 * <p>Uses OpenSearch's randomized testing utilities to simulate property-based testing
 * across many random inputs.
 */
public class QueryPlanningExceptionTests extends OpenSearchTestCase {

    /**
     * Property 13: QueryPlanningException message aggregation
     *
     * <p>For any list of N error message strings (N >= 1), a QueryPlanningException
     * constructed with that list SHALL:
     * <ul>
     *   <li>have {@code getErrors()} return an unmodifiable list of size N with the same messages</li>
     *   <li>have {@code getMessage()} return those messages joined by {@code "\n"}</li>
     * </ul>
     *
     * <p>Validates: Requirements 8.1, 8.2, 8.3
     *
     * // Feature: analytics-query-planner, Property 13: QueryPlanningException message aggregation
     */
    public void testMessageAggregation() {
        // Run 100 iterations to simulate property-based testing
        for (int iteration = 0; iteration < 100; iteration++) {
            // Generate a random list of 1–10 error messages
            int n = randomIntBetween(1, 10);
            List<String> messages = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                messages.add(randomAlphaOfLengthBetween(1, 50));
            }

            QueryPlanningException ex = new QueryPlanningException(messages);

            // 1. getErrors() returns a list of size N with the same messages in the same order
            List<String> errors = ex.getErrors();
            assertEquals("getErrors() size must equal input size (iteration " + iteration + ")", n, errors.size());
            for (int i = 0; i < n; i++) {
                assertEquals(
                    "getErrors() element " + i + " must match input (iteration " + iteration + ")",
                    messages.get(i),
                    errors.get(i)
                );
            }

            // 2. getMessage() returns messages joined by "\n"
            String expectedMessage = String.join("\n", messages);
            assertEquals(
                "getMessage() must be messages joined by newline (iteration " + iteration + ")",
                expectedMessage,
                ex.getMessage()
            );

            // 3. getErrors() list is unmodifiable
            assertThrows(
                "getErrors() must return an unmodifiable list (iteration " + iteration + ")",
                UnsupportedOperationException.class,
                () -> errors.add("extra")
            );
        }
    }
}
