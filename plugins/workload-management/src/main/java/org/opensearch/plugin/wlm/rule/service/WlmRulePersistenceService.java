/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.service;

import org.opensearch.autotagging.FeatureType;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.rule.service.RulePersistenceService;
import org.opensearch.transport.client.Client;

/**
 * This class encapsulates the logic to manage the lifecycle of workload management rules at index level
 * @opensearch.experimental
 */
@SuppressWarnings("unchecked")
public class WlmRulePersistenceService implements RulePersistenceService {
    public static final String RULES_INDEX = ".rules";
    private final ClusterService clusterService;
    private final Client client;

    @Override
    public String getIndexName() {
        return RULES_INDEX;
    }

    /**
     * Constructor for WlmRulePersistenceService
     * @param clusterService {@link ClusterService} - The cluster service to be used by RulePersistenceService
     * @param client         {@link Settings} - The client to be used by RulePersistenceService
     */
    @Inject
    public WlmRulePersistenceService(ClusterService clusterService, Client client) {
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public ClusterService getClusterService() {
        return clusterService;
    }

    @Override
    public FeatureType retrieveFeatureTypeInstance() {
        return QueryGroupFeatureType.INSTANCE;
    }
}
