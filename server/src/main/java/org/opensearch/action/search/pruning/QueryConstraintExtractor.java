/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Set;

/**
 * Extracts mandatory query constraints that are safe to use for index pruning.
 */
public interface QueryConstraintExtractor {
    /**
     * Extracts constraints that every matching document must satisfy.
     *
     * @param source search source containing the query tree
     * @param fields configured pruning fields
     * @return mandatory constraints for configured fields
     */
    List<QueryConstraint> extractMandatoryConstraints(SearchSourceBuilder source, Set<String> fields);
}
