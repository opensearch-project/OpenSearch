/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

/**
 * Result of validating a query builder's request shape before conversion.
 *
 * <p>{@code reasonCode} is used by routing/grammar for stable observability, while
 * {@code message} is used when conversion needs to surface a human-readable error.
 */
public record ValidationResult(String reasonCode, String message) {

    private static final ValidationResult ACCEPTED = new ValidationResult(null, null);

    /** Returns the shared successful validation result. */
    public static ValidationResult accepted() {
        return ACCEPTED;
    }

    /** Returns a rejected validation result carrying both machine and human-readable forms. */
    public static ValidationResult rejected(String reasonCode, String message) {
        return new ValidationResult(reasonCode, message);
    }

    /** Returns {@code true} when validation passed. */
    public boolean isAccepted() {
        return reasonCode == null;
    }
}
