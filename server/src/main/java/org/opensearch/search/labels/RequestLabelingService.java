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
import java.util.Locale;
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
    public static final String RULE_BASED_LABELS = "rule_based_labels";
    private final ThreadPool threadPool;
    private final List<Rule> rules;

    public RequestLabelingService(final ThreadPool threadPool, final List<Rule> rules) {
        this.threadPool = threadPool;
        this.rules = rules;
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
        String userProvidedTag = getUserProvidedTag(threadPool);
        if (labels.containsKey(Task.X_OPAQUE_ID) && userProvidedTag.equals(labels.get(Task.X_OPAQUE_ID))) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unexpected label %s found: %s", Task.X_OPAQUE_ID, userProvidedTag)
            );
        }
        threadPool.getThreadContext().putTransient(RULE_BASED_LABELS, labels);
    }

    /**
     * Get the user provided tag from the X-Opaque-Id header
     *
     * @return user provided tag
     */
    public static String getUserProvidedTag(ThreadPool threadPool) {
        return threadPool.getThreadContext().getRequestHeadersOnly().getOrDefault(Task.X_OPAQUE_ID, null);
    }

    public static Map<String, Object> getRuleBasedLabels(ThreadPool threadPool) {
        return threadPool.getThreadContext().getTransient(RequestLabelingService.RULE_BASED_LABELS);
    }
}
