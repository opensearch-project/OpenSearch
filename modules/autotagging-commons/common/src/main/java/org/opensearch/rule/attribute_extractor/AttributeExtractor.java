/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.attribute_extractor;

import org.opensearch.rule.autotagging.Attribute;

/**
 * This interface defines the contract for extracting the attributes for Rule based auto-tagging feature
 * @param <V>
 */
public interface AttributeExtractor<V> {

    /**
     * Defines the combination style used when a request contains multiple values
     * for an attribute.
     */
    enum LogicalOperator {
        /**
         * Logical AND
         */
        AND,
        /**
         * Logical OR
         */
        OR
    }

    /**
     * This method returns the Attribute which it is responsible for extracting
     * @return attribute
     */
    Attribute getAttribute();

    /**
     * This method returns the attribute values in context of the current request
     * @return attribute value
     */
    Iterable<V> extract();

    /**
     * Returns the logical operator used when a request contains multiple values
     * for an attribute.
     * For example, if the request targets both index A and B, then a rule must
     * have both index A and B as attributes, requiring an AND operator.
     * @return the logical operator (e.g., AND, OR)
     */
    LogicalOperator getLogicalOperator();
}
