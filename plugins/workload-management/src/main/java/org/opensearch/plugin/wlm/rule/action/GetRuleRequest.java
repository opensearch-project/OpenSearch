/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.wlm.Rule.RuleAttribute;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A request for get Rule
 * @opensearch.experimental
 */
public class GetRuleRequest extends ClusterManagerNodeRequest<GetRuleRequest> {
    private final String _id;
    private final Map<RuleAttribute, Set<String>> attributeFilters;

    /**
     * Constructor for GetRuleRequest
     * @param _id - Rule _id that we want to get
     * @param attributeFilters - Attributes that we want to filter on
     */
    public GetRuleRequest(String _id, Map<RuleAttribute, Set<String>> attributeFilters) {
        this._id = _id;
        this.attributeFilters = attributeFilters;
    }

    /**
     * Constructor for GetRuleRequest
     * @param in - A {@link StreamInput} object
     */
    public GetRuleRequest(StreamInput in) throws IOException {
        super(in);
        _id = in.readOptionalString();
        attributeFilters = in.readMap((i) -> RuleAttribute.fromName(i.readString()), i -> new HashSet<>(i.readStringList()));
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(_id);
        out.writeMap(attributeFilters, RuleAttribute::writeTo, StreamOutput::writeStringCollection);
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
    public Map<RuleAttribute, Set<String>> getAttributeFilters() {
        return attributeFilters;
    }
}
