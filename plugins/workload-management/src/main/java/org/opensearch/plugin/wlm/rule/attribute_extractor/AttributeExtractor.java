/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.attribute_extractor;

import org.opensearch.plugin.wlm.rule.Rule;


/**
 * This interface defines the contract for extracting the attributes for Rule based auto-tagging feature
 * @param <V>
 */
public interface AttributeExtractor<V> {
    /**
     * This method returns the Attribute which it is responsible for extracting
     * @return attribute
     */
    Rule.Attribute getAttribute();

    /**
     * This method returns the attribute values in context of the current request
     * @return attribute value
     */
    Iterable<V> extract();
}
