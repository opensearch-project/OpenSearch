/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.autotagging.Attribute;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A request for get Rule
 * Example Request:
 * curl -X GET "localhost:9200/_wlm/rule" - get all rules
 * curl -X GET "localhost:9200/_wlm/rule/{_id}" - get single rule by id
 * curl -X GET "localhost:9200/_wlm/rule?index_pattern=a,b" - get all rules containing index_pattern as a or b
 * @opensearch.experimental
 */
public class GetRuleRequest extends ActionRequest {
    private final String _id;
    private final Map<Attribute, Set<String>> attributeFilters;
    private final String searchAfter;

    /**
     * Constructor for GetRuleRequest
     * @param _id - Rule _id that we want to get
     * @param attributeFilters - Attributes that we want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     */
    public GetRuleRequest(String _id, Map<Attribute, Set<String>> attributeFilters, String searchAfter) {
        this._id = _id;
        this.attributeFilters = attributeFilters;
        this.searchAfter = searchAfter;
    }

    /**
     * Constructor for GetRuleRequest
     * @param in - A {@link StreamInput} object
     */
    public GetRuleRequest(StreamInput in) throws IOException {
        super(in);
        _id = in.readOptionalString();
        attributeFilters = in.readMap(Attribute::from, i -> new HashSet<>(i.readStringList()));
        searchAfter = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(_id);
        out.writeMap(attributeFilters, (output, attribute) -> attribute.writeTo(output), StreamOutput::writeStringCollection);
        out.writeOptionalString(searchAfter);
    }

    /**
     * _id getter
     */
    public String get_id() {
        return _id;
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
