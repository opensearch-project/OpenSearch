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
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.rule.action.GetRuleRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A request to get workload management Rules in workload management
 * Example Request:
 * curl -X GET "localhost:9200/_wlm/rule" - get all rules
 * curl -X GET "localhost:9200/_wlm/rule/{_id}" - get single rule by id
 * curl -X GET "localhost:9200/_wlm/rule?index_pattern=a,b" - get all rules containing attribute index_pattern as a or b
 * @opensearch.experimental
 */
public class GetWlmRuleRequest extends GetRuleRequest {
    /**
     * Constructor for GetWlmRuleRequest
     * @param id - Rule id to get
     * @param attributeFilters - A map containing the attributes to filter on
     * @param searchAfter - A string used for pagination
     */
    public GetWlmRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter) {
        super(id, attributeFilters, searchAfter);
    }

    /**
     * Constructs a new GetWlmRuleRequest instance from StreamInput
     * @param in The {@link StreamInput} from which to read the request data.
     */
    public GetWlmRuleRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected FeatureType retrieveFeatureTypeInstance() {
        return QueryGroupFeatureType.INSTANCE;
    }
}
