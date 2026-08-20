/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

/**
 * Partition resolver type for a concurrency limiter alias.
 * <ul>
 *   <li>{@link #NONE} — no resolver; all requests share the same pool.</li>
 *   <li>{@link #BY_HEADER} — routes by the {@code X-Request-Tier} header value.</li>
 *   <li>{@link #FIXED} — routes all requests to a single named partition.</li>
 *   <li>{@link #BY_SEARCH_TYPE} — routes by search shape (aggregation vs filter).</li>
 * </ul>
 */
public enum ConcurrencyLimitResolverType {

    /** No resolver; all requests share the same pool. */
    NONE(""),
    /** Routes by the {@code X-Request-Tier} header value. */
    BY_HEADER("byHeader"),
    /** Routes all requests to a single named partition. */
    FIXED("fixed"),
    /** Routes by search shape (aggregation vs filter). */
    BY_SEARCH_TYPE("bySearchType");

    private final String name;

    ConcurrencyLimitResolverType(String name) {
        this.name = name;
    }

    /**
     * Returns the setting-level string representation (e.g. {@code "byHeader"}).
     */
    public String getName() {
        return name;
    }

    /**
     * Parses a resolver type name (case-insensitive) into the corresponding enum constant.
     *
     * @param name the resolver type name to parse
     * @throws IllegalArgumentException if the name does not match any resolver type
     */
    public static ConcurrencyLimitResolverType fromName(String name) {
        for (ConcurrencyLimitResolverType type : values()) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException(
            "Unknown ConcurrencyLimitResolverType [" + name + "]. Must be one of: byHeader, fixed, bySearchType"
        );
    }
}
