/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.labels;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.search.labels.rules.DefaultUserInfoLabelingRule;
import org.opensearch.search.labels.rules.Rule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service to attach labels to a search request based on pre-defined rules
 *
 * In this POC, this service only handles search requests, but in theory it should be able to handle index as well.
 */
public class RuleBasedLabelingService {
    private final List<Rule> rules;

    public RuleBasedLabelingService(List<Rule> rules) {
        this.rules = rules;
        // default rules
        rules.add(new DefaultUserInfoLabelingRule());
    }

    public List<Rule> getRules() {
        return rules;
    }

    public void addRule(Rule rule) {
        this.rules.add(rule);
    }

    /**
     * Evaluate all rules and return labels
     */
    public void applyAllRules(final ThreadContext threadContext, final SearchRequest searchRequest) {
        Map<String, Object> labels = rules.stream()
            .map(rule -> rule.evaluate(threadContext, searchRequest))
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // Handling potential spoofing by checking if any conflicts exist between user-supplied labels and the computed labels
        for (String key : searchRequest.source().labels().keySet()) {
            if (labels.containsKey(key)) {
                throw new IllegalArgumentException("Unexpected label found: " + key);
            }
        }
        searchRequest.source().addLabels(labels);
    }
}
