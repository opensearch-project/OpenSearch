/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.action.ActionType;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Rest action to get a Rule
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestGetRuleAction extends BaseRestHandler {
    private final String name;
    private final List<Route> routes;
    private final FeatureType featureType;
    private final ActionType<GetRuleResponse> getRuleAction;
    /**
     * field name used for pagination
     */
    public static final String SEARCH_AFTER_STRING = "search_after";

    /**
     * constructor for RestGetRuleAction
     * @param name - RestGetRuleAction name
     * @param routes the list of REST routes this action handles
     * @param featureType the feature type associated with the rule
     * @param getRuleAction the action to execute for getting a rule
     */
    public RestGetRuleAction(String name, List<Route> routes, FeatureType featureType, ActionType<GetRuleResponse> getRuleAction) {
        this.name = name;
        this.routes = routes;
        this.featureType = featureType;
        this.getRuleAction = getRuleAction;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Route> routes() {
        return routes;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Map<Attribute, Set<String>> attributeFilters = new HashMap<>();
        for (String attributeName : request.params().keySet()) {
            if (attributeName.equals(_ID_STRING) || attributeName.equals(SEARCH_AFTER_STRING)) {
                continue;
            }
            Attribute attribute = featureType.getAttributeFromName(attributeName);
            if (attribute == null) {
                throw new IllegalArgumentException(attributeName + " is not a valid attribute under feature type " + featureType.getName());
            }
            attributeFilters.put(attribute, parseAttributeValues(request.param(attributeName), attributeName));
        }
        final GetRuleRequest getRuleRequest = new GetRuleRequest(
            request.param(_ID_STRING),
            attributeFilters,
            request.param(SEARCH_AFTER_STRING),
            featureType
        );
        return channel -> client.execute(getRuleAction, getRuleRequest, getRuleResponse(channel));
    }

    /**
     * Parses a comma-separated string of attribute values and validates each value.
     * @param attributeValues - the comma-separated string representing attributeValues
     * @param attributeName - attribute name
     */
    private HashSet<String> parseAttributeValues(String attributeValues, String attributeName) {
        String[] valuesArray = attributeValues.split(",");
        int maxNumberOfValuesPerAttribute = featureType.getMaxNumberOfValuesPerAttribute();
        if (valuesArray.length > maxNumberOfValuesPerAttribute) {
            throw new IllegalArgumentException(
                "The attribute value length for " + attributeName + " exceeds the maximum allowed of " + maxNumberOfValuesPerAttribute
            );
        }
        for (String value : valuesArray) {
            if (value == null || value.trim().isEmpty() || value.length() > featureType.getMaxCharLengthPerAttributeValue()) {
                throw new IllegalArgumentException(
                    "Invalid attribute value for: "
                        + attributeName
                        + " : String cannot be empty or over "
                        + featureType.getMaxCharLengthPerAttributeValue()
                        + " characters."
                );
            }
        }
        return new HashSet<>(Arrays.asList(valuesArray));
    }

    private RestResponseListener<GetRuleResponse> getRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
