/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.rule.action.GetRuleRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A request to get workload management Rules in workload management
 * @opensearch.experimental
 */
public class GetWlmRuleRequest extends GetRuleRequest {
    /**
     * Constructor for GetWlmRuleRequest
     * @param id - Rule id to get
     * @param attributeFilters - A map containing the attributes to filter on
     * @param searchAfter - A string used for pagination
     * @param featureType - The featureType instance for wlm
     */
    public GetWlmRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter, FeatureType featureType) {
        super(id, attributeFilters, searchAfter, featureType);
    }

    /**
     * Constructs a new GetWlmRuleRequest instance from StreamInput
     * @param in The {@link StreamInput} from which to read the request data.
     */
    public GetWlmRuleRequest(StreamInput in) throws IOException {
        super(in);
    }
}
