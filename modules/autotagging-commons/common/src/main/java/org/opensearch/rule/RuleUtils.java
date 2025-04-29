/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for operations related to {@link Rule} objects.
 * @opensearch.experimental
 */
@ExperimentalApi
public class RuleUtils {

    /**
     * constructor for RuleUtils
     */
    public RuleUtils() {}

    /**
     * Creates an updated {@link Rule} object by applying non-null fields from the given {@link UpdateRuleRequest}
     * to the original rule. Fields not provided in the request will retain their values from the original rule.
     * @param originalRule the original rule to update
     * @param request the request containing the new values for the rule
     * @param featureType the feature type to assign to the updated rule
     */
    public static Rule composeUpdatedRule(Rule originalRule, UpdateRuleRequest request, FeatureType featureType) {
        String requestDescription = request.getDescription();
        Map<Attribute, Set<String>> requestMap = request.getAttributeMap();
        String requestLabel = request.getFeatureValue();
        return new Rule(
            requestDescription == null ? originalRule.getDescription() : requestDescription,
            requestMap == null || requestMap.isEmpty() ? originalRule.getAttributeMap() : requestMap,
            featureType,
            requestLabel == null ? originalRule.getFeatureValue() : requestLabel,
            Instant.now().toString()
        );
    }
}
