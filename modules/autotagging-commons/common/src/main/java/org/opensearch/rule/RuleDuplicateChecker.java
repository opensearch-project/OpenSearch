/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.autotagging.Rule;

import java.util.Map;
import java.util.Optional;

/**
 * Interface to check for rule duplication.
 */
@ExperimentalApi
public interface RuleDuplicateChecker {
    /**
     * Checks if the given rule already exists in the provided rule map.
     * @param rule     the rule to check for duplication
     * @param ruleMap  a map of existing rules, keyed by rule ID
     */
    Optional<String> getDuplicateRuleId(Rule rule, Map<String, Rule> ruleMap);
}
