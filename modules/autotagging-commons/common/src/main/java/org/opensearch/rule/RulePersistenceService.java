/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface for a service that handles rule persistence CRUD operations.
 * @opensearch.experimental
 */
public interface RulePersistenceService {

    /**
     * Create rules based on the provided request.
     * @param request The request containing the details for creating the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener);

    /**
     * Get rules based on the provided request.
     * @param request The request containing the details for retrieving the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void getRule(GetRuleRequest request, ActionListener<GetRuleResponse> listener);

    /**
     * Gets every rule of a feature type, following {@link GetRuleResponse#getSearchAfter()} through every page of
     * {@link #getRule}. The listener receives the rules only once every page is read, so callers never see a partial
     * set. If any page fails, or paging stops making progress (a page with a cursor adds no new rules, or the cursor
     * does not advance), the listener receives a failure instead. As with {@link ActionListener#wrap}, an exception
     * thrown by the listener's own {@code onResponse} is also passed to its {@code onFailure}.
     * @param featureType The feature type whose rules to get.
     * @param listener The listener that receives all rules, or the failure.
     */
    default void getAllRules(FeatureType featureType, ActionListener<List<Rule>> listener) {
        getAllRules(featureType, null, new LinkedHashMap<>(), listener);
    }

    private void getAllRules(
        FeatureType featureType,
        String searchAfter,
        Map<String, Rule> collected,
        ActionListener<List<Rule>> listener
    ) {
        getRule(new GetRuleRequest(null, Collections.emptyMap(), searchAfter, featureType), ActionListener.wrap(response -> {
            boolean addedRules = false;
            for (Rule rule : response.getRules()) {
                addedRules |= collected.putIfAbsent(rule.getId(), rule) == null;
            }
            final String nextSearchAfter = response.getSearchAfter();
            if (nextSearchAfter == null) {
                listener.onResponse(new ArrayList<>(collected.values()));
                return;
            }
            if (addedRules == false || nextSearchAfter.equals(searchAfter)) {
                // Returning what was read so far would look like the unread rules had been deleted.
                listener.onFailure(
                    new IllegalStateException(
                        "Paging through rules of feature type ["
                            + featureType.getName()
                            + "] stopped making progress at search_after ["
                            + nextSearchAfter
                            + "] after "
                            + collected.size()
                            + " rules"
                    )
                );
                return;
            }
            getAllRules(featureType, nextSearchAfter, collected, listener);
        }, listener::onFailure));
    }

    /**
     * Delete a rule based on the provided request.
     * @param request The request containing the ID of the rule to delete.
     * @param listener The listener that will handle the response or failure.
     */
    void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Update rule based on the provided request.
     * @param request The request containing the details for updating the rule.
     * @param listener The listener that will handle the response or failure.
     */
    void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener);
}
