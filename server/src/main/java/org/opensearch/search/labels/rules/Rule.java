/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels.rules;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.Map;
import java.util.Set;

/**
 * An interface to define a labeling rule
 */
public abstract class Rule {
    private Set<String> allowed_user_labels;
    private Set<String> allowed_computed_labels;
    /**
     * Defines the rule to calculate labels from the context and request
     * This function should also check if the label is valid
     *
     * @return a Map of labels for POC
     */
    public Map<String, Object> evaluate(final ThreadContext threadContext, final SearchRequest searchRequest);

}
