/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.DuplicateRuleChecker;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.Rule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This class is used to check rule duplication for indexed based rules.
 */
@ExperimentalApi
public class IndexBasedDuplicateRuleChecker implements DuplicateRuleChecker {

    /**
     * Default constructor
     */
    public IndexBasedDuplicateRuleChecker() {}

    /**
     * Checks if a duplicate rule exists based on the attribute map.
     * A rule is considered a duplicate when the attribute value already exists in the index, and the number of
     * attributes in the new rule is equal to the number of attributes in an existing rule.
     *
     * For example, if an existing rule has:
     *   attribute1 = ['a'] and attribute2 = ['c']
     * And we are creating a new rule with:
     *   attribute1 = ['a']
     * Then it's not a duplicate because the existing rule has attribute2 and is more granular
     *
     * @param rule The rule to be validated against ruleMap.
     * @param ruleMap This map entries are Rules that contain the attribute values from rule, meaning they
     *                have a partial or complete overlap with the new rule being created.
     */
    @Override
    public Optional<String> getDuplicateRuleId(Rule rule, Map<String, Rule> ruleMap) {
        Map<Attribute, Set<String>> attributeMapToValidate = rule.getAttributeMap();
        for (Map.Entry<String, Rule> entry : ruleMap.entrySet()) {
            String ruleId = entry.getKey();
            Rule currRule = entry.getValue();
            // Compare the size of the attribute maps to ensure we only check for duplicates with the same number of attributes.
            if (attributeMapToValidate.size() == currRule.getAttributeMap().size()) {
                return Optional.of(ruleId);
            }
        }
        return Optional.empty();
    }
}
