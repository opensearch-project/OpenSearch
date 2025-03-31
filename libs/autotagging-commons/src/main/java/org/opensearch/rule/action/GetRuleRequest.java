/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

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
public abstract class GetRuleRequest extends ActionRequest {
    private final String id;
    private final Map<Attribute, Set<String>> attributeFilters;
    private final String searchAfter;

    /**
     * Constructor for GetRuleRequest
     * @param id - Rule id to get
     * @param attributeFilters - Rules will be filtered based on the attribute-value mappings.
     * @param searchAfter - The sort value used for pagination.
     */
    public GetRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter) {
        this.id = id;
        this.attributeFilters = attributeFilters;
        this.searchAfter = searchAfter;
    }

    /**
     * Constructs a GetRuleRequest from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public GetRuleRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        attributeFilters = in.readMap(i -> Attribute.from(i, retrieveFeatureTypeInstance()), i -> new HashSet<>(i.readStringList()));
        searchAfter = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeMap(attributeFilters, (output, attribute) -> attribute.writeTo(output), StreamOutput::writeStringCollection);
        out.writeOptionalString(searchAfter);
    }

    /**
     * Abstract method for subclasses to provide specific FeatureType Instance
     */
    protected abstract FeatureType retrieveFeatureTypeInstance();

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
}
