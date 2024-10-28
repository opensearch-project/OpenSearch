/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

/**
 * Interface for classes with a fieldName method
 *
 * @opensearch.internal
 */
public interface WithFieldName {
    /**
     * Get the field name for this query.
     */
    String fieldName();
}
