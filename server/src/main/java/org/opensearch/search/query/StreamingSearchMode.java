/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Streaming search mode selector.
 *
 * <p>Only {@link #NO_SCORING} is active in the current streaming implementation.
 * Other values are retained for request parsing/backward compatibility.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public enum StreamingSearchMode {

    /**
     * No scoring and no sorting.
     */
    NO_SCORING("no_scoring");

    private final String value;

    StreamingSearchMode(String value) {
        this.value = value;
    }

    /**
     * Parse mode from string representation.
     *
     * @param mode The string representation of the mode
     * @return The corresponding StreamingSearchMode
     * @throws IllegalArgumentException if mode is unknown
     */
    public static StreamingSearchMode fromString(String mode) {
        if (mode == null) {
            return NO_SCORING; // Default
        }

        for (StreamingSearchMode m : StreamingSearchMode.values()) {
            if (m.name().equalsIgnoreCase(mode) || m.value.equalsIgnoreCase(mode)) {
                return m;
            }
        }

        throw new IllegalArgumentException("Unknown StreamingSearchMode: " + mode);
    }

    @Override
    public String toString() {
        return value;
    }
}
