/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import java.util.List;

/**
 * Base interface for bucket aggregation grouping strategies.
 * Provides field names used by the grouping for dependency tracking.
 */
public interface GroupingInfo {

    /**
     * Returns the field names referenced by this grouping.
     * Used for tracking dependencies and building child key filters.
     */
    List<String> getFieldNames();
}
