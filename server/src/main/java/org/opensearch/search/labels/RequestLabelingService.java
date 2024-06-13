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

// Rule: allowed user provided labels, internally supported labels. -> do we allow conflicts?
// take json string (or one key-value pair for each label?), parse and check if user-provided labels are valid based on allowed labels from different rules, if valid, put into all_labels.
// evaluate all rules, put into all_labels thread context
// for user defined labels, do we want to keep in another thread context?

    // or add a label for each separate key?

    // each rule define their own "allowed labels", each rule should only use their own defined "allowed labels"
    // This service should be responsible to check conflicts.
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

    public void parseUserLabels() {
        // parse user provided labels into a map, also validate them based on allowed_user_labels from each rule

        threadPool.getThreadContext().putTransient(USER_PROVIDED_LABELS, userLabels);
    }

    /**
     * Evaluate all labeling rules and store the computed rules into thread context
     *
     * @param searchRequest {@link SearchRequest}
     */
    public void applyAllRules(final SearchRequest searchRequest) {
        Map<String, Object> computedLabels = rules.stream()
            .map(rule -> rule.evaluate(threadPool.getThreadContext(), searchRequest))
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> replacement));
//        String userProvidedTag = getUserProvidedTag(threadPool);

        threadPool.getThreadContext().putTransient(RULE_BASED_LABELS, computedLabels);
    }

    /**
     * Get the user provided tag from the X-Opaque-Id header
     *
     * @return user provided tag
     */
    public static String getUserProvidedTag(ThreadPool threadPool) {
        return threadPool.getThreadContext().getTransient(USER_PROVIDED_LABELS);
    }

    public static Map<String, Object> getRuleBasedLabels(ThreadPool threadPool) {
        return threadPool.getThreadContext().getTransient(RULE_BASED_LABELS);
    }
}
