/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

/**
 * A class that encapsulates some stats about a field, including min/max etc.
 *
 * @opensearch.internal
 */
public class FieldStats {
    public static final FieldStats UNKNOWN = new FieldStats(null, false);

    private final MinAndMax<?> minAndMax;
    private final boolean allDocsNonMissing;

    public FieldStats(MinAndMax<?> minAndMax, boolean allDocsNonMissing) {
        this.minAndMax = minAndMax;
        this.allDocsNonMissing = allDocsNonMissing;
    }

    /**
     * Return the minimum and maximum value.
     */
    public MinAndMax<?> getMinAndMax() {
        return minAndMax;
    }

    /**
     * Indicates whether all docs have values for corresponding field
     */
    public boolean allDocsNonMissing() {
        return allDocsNonMissing;
    }
}
