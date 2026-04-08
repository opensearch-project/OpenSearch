/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import java.util.List;

/**
 * Thrown by the query planner when validation or backend resolution fails.
 *
 * <p>Carries all collected planning error messages so callers can distinguish
 * planning failures from execution failures and surface actionable messages to users.
 *
 * @opensearch.internal
 */
public final class QueryPlanningException extends RuntimeException {

    private final List<String> errors;

    /**
     * Constructs a new {@code QueryPlanningException} with one or more error messages.
     *
     * @param errors list of planning error messages; must not be null or empty
     */
    public QueryPlanningException(List<String> errors) {
        super(String.join("\n", errors));
        this.errors = List.copyOf(errors);
    }

    /**
     * Returns an unmodifiable list of all planning error messages.
     *
     * @return unmodifiable list of error messages
     */
    public List<String> getErrors() {
        return errors;
    }
}
