/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;
import java.util.Map;

/**
 * Represents a grouping contribution from a bucket aggregation.
 * Implementations provide field-based grouping (terms) or
 * expression-based grouping (histogram, range) without modifying this interface.
 */
public interface GroupingInfo {

    /** Returns the logical field names this grouping contributes. */
    List<String> getFieldNames();

    /**
     * Returns the null-substitution value per field (the {@code missing} request parameter).
     * Documents with a null value for a listed field join the substitute value's group; fields
     * absent from the map keep the default semantics — their null-valued documents are excluded
     * from grouping entirely, matching classic search. Empty when no field has a
     * {@code missing} value.
     */
    Map<String, Object> getMissingByField();
}
