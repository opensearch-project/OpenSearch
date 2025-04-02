/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.common.collect.Tuple;
import org.opensearch.search.SearchHit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.autotagging.Rule.parseRule;

/**
 * Interface defining the contract for processing and parsing rule-related logic.
 * @opensearch.experimental
 */
public interface RuleProcessingService {
    /**
     * logger for RuleProcessingService
     */
    Logger logger = LogManager.getLogger(RuleProcessingService.class);

    /**
     * function to retrieve the feature type corresponding to the rule
     */
    FeatureType retrieveFeatureTypeInstance();

    /**
     * Process searchResponse from index and send a GetRuleResponse
     * @param id - Rule id fetched
     * @param searchResponse - Response received from index
     */
    default Tuple<Map<String, Rule>, String> parseGetRuleResponse(String id, SearchResponse searchResponse) {
        List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
        if (id != null && hits.isEmpty()) {
            logger.error("Rule with ID " + id + " not found.");
            return new Tuple<>(new HashMap<>(), null);
        }
        Map<String, Rule> ruleMap = hits.stream()
            .collect(
                Collectors.toMap(
                    SearchHit::getId,
                    hit -> parseRule(hit.getId(), hit.getSourceAsString(), retrieveFeatureTypeInstance()).getValue()
                )
            );
        String lastValidRuleId = hits.isEmpty() ? null : hits.get(hits.size() - 1).getId();
        return new Tuple<>(ruleMap, lastValidRuleId);
    }
}
