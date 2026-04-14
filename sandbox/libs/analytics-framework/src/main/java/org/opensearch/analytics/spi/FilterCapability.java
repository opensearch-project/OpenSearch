/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares a backend's ability to evaluate filter predicates, scoped to data formats.
 * Two variants for the two categories of filter operations.
 *
 * <p>TODO: add index-backed filter capability variants (ExactIndex, ApproximateIndex)
 * to distinguish backends that can use index structures (terms index, BKD tree, bloom filter)
 * for predicate evaluation. Exact vs approximate distinction matters for PlanForker when
 * pruning alternatives based on query accuracy requirements.
 *
 * @opensearch.internal
 */
public sealed interface FilterCapability {

    /** Standard comparison filter (EQUALS, GT, IN, LIKE, etc.) on a field type in given formats. */
    record Standard(FilterOperator operator, FieldType fieldType,
                    Set<String> formats) implements FilterCapability {
        public Standard {
            formats = Set.copyOf(formats);
        }
    }

    /** Full-text filter (MATCH, MATCH_PHRASE, FUZZY, etc.) with supported query parameters. */
    record FullText(FilterOperator operator, FieldType fieldType,
                    Set<String> formats, Set<String> supportedParams) implements FilterCapability {
        public FullText {
            formats = Set.copyOf(formats);
            supportedParams = Set.copyOf(supportedParams);
        }
    }
}
