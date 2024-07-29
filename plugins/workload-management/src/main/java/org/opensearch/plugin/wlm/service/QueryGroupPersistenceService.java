/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.action.GetQueryGroupResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class defines the functions for QueryGroup persistence
 */
public class QueryGroupPersistenceService {
    private static final String SOURCE = "query-group-persistence-service";
    private static final Logger logger = LogManager.getLogger(QueryGroupPersistenceService.class);
    /**
     *  max QueryGroup count setting name
     */
    public static final String QUERY_GROUP_COUNT_SETTING_NAME = "node.query_group.max_count";
    /**
     * default max queryGroup count on any node at any given point in time
     */
    private static final int DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE = 100;
    /**
     * min queryGroup count on any node at any given point in time
     */
    private static final int MIN_QUERY_GROUP_COUNT_VALUE = 1;
    /**
     *  max QueryGroup count setting
     */
    public static final Setting<Integer> MAX_QUERY_GROUP_COUNT = Setting.intSetting(
        QUERY_GROUP_COUNT_SETTING_NAME,
        DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE,
        0,
        QueryGroupPersistenceService::validateMaxQueryGroupCount,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private final ClusterService clusterService;
    private volatile int maxQueryGroupCount;

    /**
     * Constructor for QueryGroupPersistenceService
     *
     * @param clusterService {@link ClusterService} - The cluster service to be used by QueryGroupPersistenceService
     * @param settings {@link Settings} - The settings to be used by QueryGroupPersistenceService
     * @param clusterSettings {@link ClusterSettings} - The cluster settings to be used by QueryGroupPersistenceService
     */
    @Inject
    public QueryGroupPersistenceService(
        final ClusterService clusterService,
        final Settings settings,
        final ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        setMaxQueryGroupCount(MAX_QUERY_GROUP_COUNT.get(settings));
        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxQueryGroupCount);
    }

    /**
     * Set maxQueryGroupCount to be newMaxQueryGroupCount
     * @param newMaxQueryGroupCount - the max number of QueryGroup allowed
     */
    public void setMaxQueryGroupCount(int newMaxQueryGroupCount) {
        validateMaxQueryGroupCount(newMaxQueryGroupCount);
        this.maxQueryGroupCount = newMaxQueryGroupCount;
    }

    /**
     * Validator for maxQueryGroupCount
     * @param maxQueryGroupCount - the maxQueryGroupCount number to be verified
     */
    private static void validateMaxQueryGroupCount(int maxQueryGroupCount) {
        if (maxQueryGroupCount > DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE || maxQueryGroupCount < MIN_QUERY_GROUP_COUNT_VALUE) {
            throw new IllegalArgumentException(QUERY_GROUP_COUNT_SETTING_NAME + " should be in range [1-100].");
        }
    }

    /**
     * Get the QueryGroup with the specified name and generate response
     * @param name - the QueryGroup name we are getting
     * @param listener - ActionListener for GetQueryGroupResponse
     */
    public void getFromClusterStateMetadata(String name, ActionListener<GetQueryGroupResponse> listener) {
        List<QueryGroup> resultGroups = getQueryGroupsFromClusterState(name, clusterService.state());
        if (resultGroups.isEmpty() && name != null && !name.isEmpty()) {
            logger.warn("No QueryGroup exists with the provided name: {}", name);
            Exception e = new IllegalArgumentException("No QueryGroup exists with the provided name: " + name);
            listener.onFailure(e);
            return;
        }
        listener.onResponse(new GetQueryGroupResponse(resultGroups, RestStatus.OK));
    }

    /**
     * Get the QueryGroups with the specified name from cluster state
     * @param name - the QueryGroup name we are getting
     * @param currentState - current cluster state
     */
    List<QueryGroup> getQueryGroupsFromClusterState(String name, ClusterState currentState) {
        Map<String, QueryGroup> currentGroups = currentState.getMetadata().queryGroups();
        if (name == null || name.isEmpty()) {
            return new ArrayList<>(currentGroups.values());
        }
        List<QueryGroup> resultGroups = new ArrayList<>();
        currentGroups.values().stream().filter(group -> group.getName().equals(name)).findFirst().ifPresent(resultGroups::add);
        return resultGroups;
    }

    /**
     * maxQueryGroupCount getter
     */
    public int getMaxQueryGroupCount() {
        return maxQueryGroupCount;
    }
}
