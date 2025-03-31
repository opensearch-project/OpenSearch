/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.GetWlmRuleResponse;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.service.RulePersistenceService;
import org.opensearch.transport.client.Client;

import java.util.Map;

/**
 * This class encapsulates the logic to manage the lifecycle of workload management rules at index level
 * @opensearch.experimental
 */
@SuppressWarnings("unchecked")
public class WlmRulePersistenceService extends RulePersistenceService {
    /**
     * Constructor for WlmRulePersistenceService
     * @param clusterService {@link ClusterService} - The cluster service to be used by RulePersistenceService
     * @param client         {@link Settings} - The client to be used by RulePersistenceService
     */
    @Inject
    public WlmRulePersistenceService(ClusterService clusterService, Client client) {
        super(clusterService, client);
    }

    @Override
    protected FeatureType retrieveFeatureTypeInstance() {
        return QueryGroupFeatureType.INSTANCE;
    }

    @Override
    protected <T extends GetRuleResponse> T buildGetRuleResponse(Map<String, Rule> ruleMap, String nextSearchAfter, RestStatus restStatus) {
        return (T) new GetWlmRuleResponse(ruleMap, nextSearchAfter, restStatus);
    }
}
