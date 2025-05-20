/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import java.util.Objects;

/**
 * A class that encapsulates some stats for a given field.
 *
 * @param minAndMax the minimum and maximum value of the given field
 * @param allDocsHaveValue whether all docs have value for the given field
 * @opensearch.internal
 */
public record FieldStats(MinAndMax<?> minAndMax, boolean allDocsHaveValue) {
    public FieldStats {
        Objects.requireNonNull(minAndMax);
    }
}
