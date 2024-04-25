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

/**
 * An interface to define a labeling rule
 */
public interface Rule {
    /**
     * Defines the rule to calculate labels from the context and request
     *
     * @return a Map of labels for POC
     */
    public Map<String, Object> evaluate(final ThreadContext threadContext, final SearchRequest searchRequest);

}
