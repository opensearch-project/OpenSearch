/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.labels.rules.Rule;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service to attach labels to a search request based on pre-defined rules
 * It evaluate all available rules and generate labels into the thread context.
 */
public class RequestLabelingService {
    /**
     * Field name for computed labels
     */
    public static final String COMPUTED_LABELS = "computed_labels";
    private final ThreadPool threadPool;
    private final List<Rule> rules;

    public RequestLabelingService(final ThreadPool threadPool, final List<Rule> rules) {
        this.threadPool = threadPool;
        this.rules = rules;
    }

    /**
     * Get all the existing rules
     *
     * @return list of existing rules
     */
    public List<Rule> getRules() {
        return rules;
    }

    /**
     * Add a labeling rule to the service
     *
     * @param rule {@link Rule}
     */
    public void addRule(final Rule rule) {
        this.rules.add(rule);
    }

    /**
     * Get the user provided tag from the X-Opaque-Id header
     *
     * @return user provided tag
     */
    public String getUserProvidedTag() {
        return threadPool.getThreadContext().getRequestHeadersOnly().getOrDefault(Task.X_OPAQUE_ID, null);
    }

    /**
     * Evaluate all labeling rules and store the computed rules into thread context
     *
     * @param searchRequest {@link SearchRequest}
     */
    public void applyAllRules(final SearchRequest searchRequest) {
        Map<String, Object> labels = rules.stream()
            .map(rule -> rule.evaluate(threadPool.getThreadContext(), searchRequest))
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> replacement));
        threadPool.getThreadContext().putTransient(COMPUTED_LABELS, labels);
    }
}
