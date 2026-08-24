/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

/**
 * Operating mode for a concurrency limiter alias.
 * <ul>
 *   <li>{@link #DISABLED} — limiter is inactive; no tracking, no rejection.</li>
 *   <li>{@link #MONITOR_ONLY} — tracks metrics and logs rejections but never returns HTTP 429.</li>
 *   <li>{@link #ENFORCED} — actively rejects requests when the adaptive limit is reached.</li>
 * </ul>
 */
public enum ConcurrencyLimitMode {

    /** Limiter is inactive; no tracking, no rejection. */
    DISABLED("disabled"),
    /** Tracks metrics and logs rejections but never returns HTTP 429. */
    MONITOR_ONLY("monitor_only"),
    /** Actively rejects requests when the adaptive limit is reached. */
    ENFORCED("enforced");

    private final String name;

    ConcurrencyLimitMode(String name) {
        this.name = name;
    }

    /**
     * Returns the setting-level string representation (e.g. {@code "monitor_only"}).
     */
    public String getName() {
        return name;
    }

    /**
     * Parses a mode name (case-insensitive) into the corresponding enum constant.
     *
     * @param name the mode name to parse
     * @throws IllegalArgumentException if the name does not match any mode
     */
    public static ConcurrencyLimitMode fromName(String name) {
        for (ConcurrencyLimitMode mode : values()) {
            if (mode.name.equalsIgnoreCase(name)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown ConcurrencyLimitMode [" + name + "]. Must be one of: disabled, monitor_only, enforced");
    }
}
