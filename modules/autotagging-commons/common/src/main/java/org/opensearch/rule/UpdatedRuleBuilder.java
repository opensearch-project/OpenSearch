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
public class UpdatedRuleBuilder {

    private final Rule originalRule;
    private final UpdateRuleRequest request;

    /**
     * constructor for UpdatedRuleBuilder
     * @param originalRule - the existing rule to update
     * @param updateRuleRequest - the update request containing updating details
     */
    public UpdatedRuleBuilder(Rule originalRule, UpdateRuleRequest updateRuleRequest) {
        this.originalRule = originalRule;
        this.request = updateRuleRequest;
    }

    /**
     * Creates an updated {@link Rule} object by applying non-null fields from the given {@link UpdateRuleRequest}
     * to the original rule. Fields not provided in the request will retain their values from the original rule.
     */
    public Rule build() {
        String requestDescription = request.getDescription();
        Map<Attribute, Set<String>> requestMap = request.getAttributeMap();
        String requestLabel = request.getFeatureValue();
        return new Rule(
            requestDescription == null ? originalRule.getDescription() : requestDescription,
            requestMap == null || requestMap.isEmpty() ? originalRule.getAttributeMap() : requestMap,
            originalRule.getFeatureType(),
            requestLabel == null ? originalRule.getFeatureValue() : requestLabel,
            Instant.now().toString()
        );
    }
}
