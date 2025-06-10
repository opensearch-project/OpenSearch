/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.autotagging.RuleValidator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A request for get Rule
 * Example Request:
 * The endpoint "localhost:9200/_wlm/rule" is specific to the Workload Management feature to manage rules
 * curl -X GET "localhost:9200/_wlm/rule" - get all rules
 * curl -X GET "localhost:9200/_wlm/rule/{_id}" - get single rule by id
 * curl -X GET "localhost:9200/_wlm/rule?index_pattern=a,b" - get all rules containing attribute index_pattern as a or b
 * @opensearch.experimental
 */
@ExperimentalApi
public class GetRuleRequest extends ActionRequest {
    private final String id;
    private final Map<Attribute, Set<String>> attributeFilters;
    private final String searchAfter;
    private final FeatureType featureType;

    /**
     * Constructor for GetRuleRequest
     * @param id - Rule id to get
     * @param attributeFilters - Rules will be filtered based on the attribute-value mappings.
     * @param searchAfter - The sort value used for pagination.
     * @param featureType - The feature type related to rule.
     */
    public GetRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter, FeatureType featureType) {
        this.id = id;
        this.attributeFilters = attributeFilters;
        this.searchAfter = searchAfter;
        this.featureType = featureType;
    }

    /**
     * Constructs a GetRuleRequest from a StreamInput for deserialization.
     * @param in - The {@link StreamInput} instance to read from.
     */
    public GetRuleRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        featureType = FeatureType.from(in);
        attributeFilters = in.readMap(i -> Attribute.from(i, featureType), i -> new HashSet<>(i.readStringList()));
        searchAfter = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (RuleValidator.isEmpty(id)) {
            throw new IllegalArgumentException(Rule._ID_STRING + " cannot be empty.");
        }
        if (RuleValidator.isEmpty(searchAfter)) {
            throw new IllegalArgumentException("search_after cannot be empty.");
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        featureType.writeTo(out);
        out.writeMap(attributeFilters, (output, attribute) -> attribute.writeTo(output), StreamOutput::writeStringCollection);
        out.writeOptionalString(searchAfter);
    }

    /**
     * id getter
     */
    public String getId() {
        return id;
    }

    /**
     * attributeFilters getter
     */
    public Map<Attribute, Set<String>> getAttributeFilters() {
        return attributeFilters;
    }

    /**
     * searchAfter getter
     */
    public String getSearchAfter() {
        return searchAfter;
    }

    /**
     * FeatureType getter
     * @return
     */
    public FeatureType getFeatureType() {
        return featureType;
    }
}
