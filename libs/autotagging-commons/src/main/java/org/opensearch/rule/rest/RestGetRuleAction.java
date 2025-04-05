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
public abstract class RestGetRuleAction extends BaseRestHandler {
    /**
     * field name used for pagination
     */
    public static final String SEARCH_AFTER_STRING = "search_after";

    /**
     * Constructor for RestGetRuleAction
     */
    public RestGetRuleAction() {}

    @Override
    public abstract String getName();

    @Override
    public abstract List<Route> routes();

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Map<Attribute, Set<String>> attributeFilters = new HashMap<>();
        for (String attributeName : request.params().keySet()) {
            if (attributeName.equals(_ID_STRING) || attributeName.equals(SEARCH_AFTER_STRING)) {
                continue;
            }
            attributeFilters.put(getAttributeFromName(attributeName), parseAttributeValues(request.param(attributeName), attributeName));
        }
        final GetRuleRequest getRuleRequest = buildGetRuleRequest(
            request.param(_ID_STRING),
            attributeFilters,
            request.param(SEARCH_AFTER_STRING)
        );
        return channel -> client.execute(retrieveGetRuleActionInstance(), getRuleRequest, getRuleResponse(channel));
    }

    /**
     * Parses a comma-separated string of attribute values and validates each value.
     * @param attributeValues - the comma-separated string representing attributeValues
     * @param attributeName - attribute name
     */
    public HashSet<String> parseAttributeValues(String attributeValues, String attributeName) {
        String[] valuesArray = attributeValues.split(",");
        int maxNumberOfValuesPerAttribute = retrieveFeatureTypeInstance().getMaxNumberOfValuesPerAttribute();
        if (valuesArray.length > maxNumberOfValuesPerAttribute) {
            throw new IllegalArgumentException("The attribute value length for " + attributeName + " exceeds the maximum allowed of " + maxNumberOfValuesPerAttribute);
        }
        for (String value : valuesArray) {
            if (value == null || value.trim().isEmpty() || value.length() > retrieveFeatureTypeInstance().getMaxCharLengthPerAttributeValue()) {
                throw new IllegalArgumentException("Invalid attribute value for: " + attributeName + " : String cannot be empty or over ");
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

    /**
     * Abstract method for subclasses to retrieve the Attribute corresponding
     * to the attribute name.
     * @param name - The name of the attribute to retrieve.
     */
    public abstract Attribute getAttributeFromName(String name);

    /**
     * Abstract method for subclasses to provide specific ActionType Instance
     */
    protected abstract <T extends ActionType<GetRuleResponse>> T retrieveGetRuleActionInstance();

    /**
     * Abstract method for subclasses to provide specific FeatureType Instance
     */
    protected abstract FeatureType retrieveFeatureTypeInstance();

    /**
     * Abstract method for subclasses to construct a {@link GetRuleRequest}. This method allows subclasses
     * to define their own request-building logic depending on their specific needs.
     *
     * @param id - The ID of the rule to retrieve.
     * @param attributeFilters - A map of {@link Attribute} keys to sets of string values for filtering.
     * @param searchAfter - The pagination value to fetch the next set of results.
     */
    protected abstract GetRuleRequest buildGetRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter);
}
