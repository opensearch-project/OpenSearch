/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler.ThrottlingKey;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

import static org.opensearch.search.query_group.QueryGroupServiceSettings.MAX_QUERY_GROUP_COUNT;

/**
 * This class defines the functions for QueryGroup persistence
 */
public class QueryGroupPersistenceService implements Persistable<QueryGroup> {
    private static final Logger logger = LogManager.getLogger(QueryGroupPersistenceService.class);
    private final ClusterService clusterService;
    private static final String SOURCE = "query-group-persistence-service";
    private static final String CREATE_QUERY_GROUP_THROTTLING_KEY = "create-query-group";
    private static final String UPDATE_QUERY_GROUP_THROTTLING_KEY = "update-query-group";
    private static final String DELETE_QUERY_GROUP_THROTTLING_KEY = "delete-query-group";
    private final AtomicInteger inflightCreateQueryGroupRequestCount;
    private final Map<String, DoubleAdder> inflightResourceLimitValues;
    private volatile int maxQueryGroupCount;
    final ThrottlingKey createQueryGroupThrottlingKey;
    final ThrottlingKey updateQueryGroupThrottlingKey;
    final ThrottlingKey deleteQueryGroupThrottlingKey;

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
        this.createQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(CREATE_QUERY_GROUP_THROTTLING_KEY, true);
        this.deleteQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(DELETE_QUERY_GROUP_THROTTLING_KEY, true);
        this.updateQueryGroupThrottlingKey = clusterService.registerClusterManagerTask(UPDATE_QUERY_GROUP_THROTTLING_KEY, true);
        maxQueryGroupCount = MAX_QUERY_GROUP_COUNT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxQueryGroupCount);
        inflightCreateQueryGroupRequestCount = new AtomicInteger();
        inflightResourceLimitValues = new HashMap<>();
    }

    /**
     * Set maxQueryGroupCount to be newMaxQueryGroupCount
     * @param newMaxQueryGroupCount - the max number of QueryGroup allowed
     */
    public void setMaxQueryGroupCount(int newMaxQueryGroupCount) {
        if (newMaxQueryGroupCount < 0) {
            throw new IllegalArgumentException("node.query_group.max_count can't be negative");
        }
        this.maxQueryGroupCount = newMaxQueryGroupCount;
    }

    @Override
    public void delete(String name, ActionListener<DeleteQueryGroupResponse> listener) {
        deleteInClusterStateMetadata(name, listener);
    }

    /**
     * Modify cluster state to delete the QueryGroup
     * @param name - the name for QueryGroup to be deleted
     */
    void deleteInClusterStateMetadata(String name, ActionListener<DeleteQueryGroupResponse> listener) {
        clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return deleteQueryGroupInClusterState(name, currentState);
            }

            @Override
            public ThrottlingKey getClusterManagerThrottlingKey() {
                return deleteQueryGroupThrottlingKey;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to delete QueryGroup due to error: {}, for source: {}", e.getMessage(), source);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                final Map<String, QueryGroup> oldGroupsMap = oldState.metadata().queryGroups();
                final Map<String, QueryGroup> newGroupsMap = newState.metadata().queryGroups();
                List<QueryGroup> deletedGroups = new ArrayList<>();
                for (String groupId : oldGroupsMap.keySet()) {
                    if (!newGroupsMap.containsKey(groupId)) {
                        deletedGroups.add(oldGroupsMap.get(groupId));
                    }
                }
                DeleteQueryGroupResponse response = new DeleteQueryGroupResponse(deletedGroups, RestStatus.OK);
                listener.onResponse(response);
            }
        });
    }

    /**
     * Modify cluster state to delete the QueryGroup
     * @param name - the name for QueryGroup to be deleted
     * @param currentClusterState - current cluster state
     */
    ClusterState deleteQueryGroupInClusterState(final String name, final ClusterState currentClusterState) {
        final Metadata metadata = currentClusterState.metadata();
        final Map<String, QueryGroup> previousGroups = metadata.queryGroups();

        if (name == null || name.isEmpty()) { // delete all
            return ClusterState.builder(currentClusterState)
                .metadata(Metadata.builder(metadata).queryGroups(new HashMap<>()).build())
                .build();
        }
        for (QueryGroup queryGroup : previousGroups.values()) {
            if (queryGroup.getName().equals(name)) {
                return ClusterState.builder(currentClusterState).metadata(Metadata.builder(metadata).remove(queryGroup).build()).build();
            }
        }
        logger.error("The QueryGroup with provided name {} doesn't exist", name);
        throw new RuntimeException("No QueryGroup exists with the provided name: " + name);
    }

    /**
     * inflightCreateQueryGroupRequestCount getter
     */
    public AtomicInteger getInflightCreateQueryGroupRequestCount() {
        return inflightCreateQueryGroupRequestCount;
    }

    /**
     * inflightResourceLimitValues getter
     */
    public Map<String, DoubleAdder> getInflightResourceLimitValues() {
        return inflightResourceLimitValues;
    }
}
