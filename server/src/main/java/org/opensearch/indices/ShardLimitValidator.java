/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.Optional;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

/**
 * This class contains the logic used to check the cluster-wide shard limit before shards are created and ensuring that the limit is
 * updated correctly on setting updates, etc.
 *
 * NOTE: This is the limit applied at *shard creation time*. If you are looking for the limit applied at *allocation* time, which is
 * controlled by a different setting,
 * see {@link org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider}.
 *
 * @opensearch.internal
 */
public class ShardLimitValidator {

    public static final String SETTING_MAX_SHARDS_PER_CLUSTER_KEY = "cluster.routing.allocation.total_shards_limit";
    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_NODE = Setting.intSetting(
        "cluster.max_shards_per_node",
        1000,
        1,
        new MaxShardPerNodeLimitValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER = Setting.intSetting(
        SETTING_MAX_SHARDS_PER_CLUSTER_KEY,
        -1,
        -1,
        new MaxShardPerClusterLimitValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> SETTING_CLUSTER_IGNORE_DOT_INDEXES = Setting.boolSetting(
        "cluster.ignore_dot_indexes",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    protected final AtomicInteger shardLimitPerNode = new AtomicInteger();
    protected final AtomicInteger shardLimitPerCluster = new AtomicInteger();
    private final SystemIndices systemIndices;
    private volatile boolean ignoreDotIndexes;

    public ShardLimitValidator(final Settings settings, ClusterService clusterService, SystemIndices systemIndices) {
        this.shardLimitPerNode.set(SETTING_CLUSTER_MAX_SHARDS_PER_NODE.get(settings));
        this.shardLimitPerCluster.set(SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER.get(settings));
        this.ignoreDotIndexes = SETTING_CLUSTER_IGNORE_DOT_INDEXES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_NODE, this::setShardLimitPerNode);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER, this::setShardLimitPerCluster);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(SETTING_CLUSTER_IGNORE_DOT_INDEXES, this::setIgnoreDotIndexes);
        this.systemIndices = systemIndices;
    }

    private void setShardLimitPerNode(int newValue) {
        this.shardLimitPerNode.set(newValue);
    }

    private void setShardLimitPerCluster(int newValue) {
        this.shardLimitPerCluster.set(newValue);
    }

    /**
     * Gets the currently configured value of the {@link ShardLimitValidator#SETTING_CLUSTER_MAX_SHARDS_PER_NODE} setting.
     * @return the current value of the setting
     */
    public int getShardLimitPerNode() {
        return shardLimitPerNode.get();
    }

    /**
     * Gets the currently configured value of the {@link ShardLimitValidator#SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER} setting.
     * @return the current value of the setting.
     */
    public int getShardLimitPerCluster() {
        return shardLimitPerCluster.get();
    }

    private void setIgnoreDotIndexes(boolean newValue) {
        this.ignoreDotIndexes = newValue;
    }

    /**
     * Checks whether an index can be created without going over the cluster shard limit.
     * Validate shard limit only for non system indices as it is not hard limit anyways.
     * Further also validates if the cluster.ignore_dot_indexes is set to true.
     * If so then it does not validate any index which starts with '.' except data-stream index.
     *
     * @param indexName      the name of the index being created
     * @param settings       the settings of the index to be created
     * @param state          the current cluster state
     * @throws ValidationException if creating this index would put the cluster over the cluster shard limit
     */
    public void validateShardLimit(final String indexName, final Settings settings, final ClusterState state) {
        /*
        Validate shard limit only for non system indices as it is not hard limit anyways.
        Further also validates if the cluster.ignore_dot_indexes is set to true.
        If so then it does not validate any index which starts with '.'.
        */
        if (shouldIndexBeIgnored(indexName)) {
            return;
        }

        final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        final int numberOfReplicas = IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);
        final int shardsToCreate = numberOfShards * (1 + numberOfReplicas);

        final Optional<String> shardLimit = checkShardLimit(shardsToCreate, state);
        if (shardLimit.isPresent()) {
            final ValidationException e = new ValidationException();
            e.addValidationError(shardLimit.get());
            throw e;
        }
    }

    /**
     * Validates whether a list of indices can be opened without going over the cluster shard limit.  Only counts indices which are
     * currently closed and will be opened, ignores indices which are already open. Adding to this it validates the
     * shard limit only for non system indices and if the cluster.ignore_dot_indexes property is set to true
     * then the indexes starting with '.' are ignored except the data-stream indexes.
     *
     * @param currentState The current cluster state.
     * @param indicesToOpen The indices which are to be opened.
     * @throws ValidationException If this operation would take the cluster over the limit and enforcement is enabled.
     */
    public void validateShardLimit(ClusterState currentState, Index[] indicesToOpen) {
        int shardsToOpen = Arrays.stream(indicesToOpen)
            /*
            Validate shard limit only for non system indices as it is not hard limit anyways.
            Further also validates if the cluster.ignore_dot_indexes is set to true.
            If so then it does not validate any index which starts with '.'
            however data-stream indexes are still validated.
            */
            .filter(index -> !shouldIndexBeIgnored(index.getName()))
            .filter(index -> currentState.metadata().index(index).getState().equals(IndexMetadata.State.CLOSE))
            .mapToInt(index -> getTotalShardCount(currentState, index))
            .sum();

        Optional<String> error = checkShardLimit(shardsToOpen, currentState);
        if (error.isPresent()) {
            ValidationException ex = new ValidationException();
            ex.addValidationError(error.get());
            throw ex;
        }
    }

    private static int getTotalShardCount(ClusterState state, Index index) {
        IndexMetadata indexMetadata = state.metadata().index(index);
        return indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas());
    }

    /**
     * Returns true if the index should be ignored during validation.
     * Index is ignored if it is a system index or if cluster.ignore_dot_indexes is set to true
     * then indexes which are starting with dot and are not data stream index are ignored.
     *
     * @param indexName  The index which needs to be validated.
     */
    private boolean shouldIndexBeIgnored(String indexName) {
        if (this.ignoreDotIndexes) {
            return validateDotIndex(indexName) && !isDataStreamIndex(indexName);
        } else return systemIndices.validateSystemIndex(indexName);
    }

    /**
     * Returns true if the index name starts with '.' else false.
     *
     * @param indexName  The index which needs to be validated.
     */
    private boolean validateDotIndex(String indexName) {
        return indexName.charAt(0) == '.';
    }

    /**
     * Returns true if the index is dataStreamIndex false otherwise.
     *
     * @param indexName  The index which needs to be validated.
     */
    private boolean isDataStreamIndex(String indexName) {
        return indexName.startsWith(DataStream.BACKING_INDEX_PREFIX);
    }

    /**
     * Checks to see if an operation can be performed without taking the cluster over the cluster-wide shard limit.
     * Returns an error message if appropriate, or an empty {@link Optional} otherwise.
     *
     * @param newShards         The number of shards to be added by this operation
     * @param state             The current cluster state
     * @return If present, an error message to be given as the reason for failing
     * an operation. If empty, a sign that the operation is valid.
     */
    public Optional<String> checkShardLimit(int newShards, ClusterState state) {
        return checkShardLimit(newShards, state, getShardLimitPerNode(), getShardLimitPerCluster());
    }

    // package-private for testing
    static Optional<String> checkShardLimit(
        int newShards,
        ClusterState state,
        int maxShardsPerNodeSetting,
        int maxShardsPerClusterSetting
    ) {
        int nodeCount = state.getNodes().getDataNodes().size();

        // Only enforce the shard limit if we have at least one data node, so that we don't block
        // index creation during cluster setup
        if (nodeCount == 0 || newShards < 0) {
            return Optional.empty();
        }

        int maxShardsInCluster = maxShardsPerClusterSetting;
        if (maxShardsInCluster == -1) {
            maxShardsInCluster = maxShardsPerNodeSetting * nodeCount;
        } else {
            maxShardsInCluster = Math.min(maxShardsInCluster, maxShardsPerNodeSetting * nodeCount);
        }

        int currentOpenShards = state.getMetadata().getTotalOpenIndexShards();
        if ((currentOpenShards + newShards) > maxShardsInCluster) {
            String errorMessage = "this action would add ["
                + newShards
                + "] total shards, but this cluster currently has ["
                + currentOpenShards
                + "]/["
                + maxShardsInCluster
                + "] maximum shards open";
            return Optional.of(errorMessage);
        }
        return Optional.empty();
    }

    /**
     * Validates the MaxShadPerCluster threshold.
     */
    static final class MaxShardPerClusterLimitValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(Integer value) {}

        @Override
        public void validate(Integer maxShardPerCluster, Map<Setting<?>, Object> settings) {
            final int maxShardPerNode = (int) settings.get(SETTING_CLUSTER_MAX_SHARDS_PER_NODE);
            doValidate(maxShardPerCluster, maxShardPerNode);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Collections.singletonList(SETTING_CLUSTER_MAX_SHARDS_PER_NODE);
            return settings.iterator();
        }
    }

    /**
     * Validates the MaxShadPerNode threshold.
     */
    static final class MaxShardPerNodeLimitValidator implements Setting.Validator<Integer> {

        @Override
        public void validate(Integer value) {}

        @Override
        public void validate(Integer maxShardPerNode, Map<Setting<?>, Object> settings) {
            final int maxShardPerCluster = (int) settings.get(SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER);
            doValidate(maxShardPerCluster, maxShardPerNode);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = Collections.singletonList(SETTING_CLUSTER_MAX_SHARDS_PER_CLUSTER);
            return settings.iterator();
        }
    }

    private static void doValidate(final int maxShardPerCluster, final int maxShardPerNode) {
        if (maxShardPerCluster != -1 && maxShardPerCluster < maxShardPerNode) {
            throw new IllegalArgumentException(
                "MaxShardPerCluster " + maxShardPerCluster + " should be greater than or equal to MaxShardPerNode " + maxShardPerNode
            );
        }
    }
}
